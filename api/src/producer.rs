use crate::common::{Config, Result};
use crate::producer::batch_publisher::BatchPublisher;
use crate::producer::partition_selector::PartitionSelector;
use broker::model::TopicPartition;
use linked_hash_map::LinkedHashMap;
use protocol::record::{Record, RecordBatch};
use std::ops::Sub;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Instant, SystemTime};
use crate::common::broker_resolver::BrokerResolver;

mod batch_publisher;
mod partition_selector;

#[cfg(test)]
mod producer_test;

#[cfg(test)]
mod mock_utils;

/// This client handles the batching of individual records. It selects the partition to send the record to based on the record's key, if present, or in a
/// round-robin fashion otherwise. Batches can be flushed by two separate mechanisms: when the max size is reached, immediately after the last record was
/// added, or when the duration elapsed between the first insertion in the batch to the current time exceeds the configured linger duration. The expiry
/// mechanism works on a separate thread, while the batching process happens on a single thread (the thread the [Client::send_record] method is called from).
/// Regardless of why a batch is flushed out, it is submitted asynchronously on another thread (a single one), in order to keep the main producer thread
/// quick and have low contention on the mutex. The client is expected to be used from a single thread. Note that to get more throughput on the same producer
/// host, it is possible to have several clients running in different threads, or otherwise make small changes to this struct to make it fully thread-safe
/// (having several clients would reduce the batching potential, so it's not very desirable).
///
/// As explained in [BatchPublisher], we have made significant simplifications to the problem by not handling some failure scenarios. Please read this doc
/// for more information. We could definitely handle those, it's just more work and this is a personal project for fun and learning, so I have to put some
/// limits to keep things manageable.
pub struct Client {
    config: Config,
    batches: Arc<Mutex<LinkedHashMap<TopicPartition, InflightBatch>>>,
    batch_publisher: Arc<BatchPublisher>,
    partition_selector: PartitionSelector,
    resume_reaping_notifier: Arc<Condvar>,
    batch_reaper_handle: JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
}

struct InflightBatch {
    start: Instant,
    records: Vec<Record>,
}

impl Client {
    pub fn new(config: Config, coordinator: Arc<dyn coordinator::Client>) -> Result<Self> {
        let broker_resolver = BrokerResolver::new(Arc::clone(&coordinator))?;
        Self::new_for_testing(config, coordinator, broker_resolver)
    }

    fn new_for_testing(config: Config, coordinator: Arc<dyn coordinator::Client>, broker_resolver: BrokerResolver) -> Result<Self> {
        let batch_publisher = Arc::new(BatchPublisher::new(Arc::clone(&coordinator), broker_resolver));

        let partition_selector = PartitionSelector::new(Arc::clone(&coordinator));
        let resume_reaping_notifier = Arc::new(Condvar::new());
        let batches = Arc::new(Mutex::new(LinkedHashMap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let batch_reaper_task = BatchReaperTask {
            config,
            resume_reaping_notifier: Arc::clone(&resume_reaping_notifier),
            batches: Arc::clone(&batches),
            batch_publisher: Arc::clone(&batch_publisher),
            shutdown: Arc::clone(&shutdown),
        };
        let batch_reaper_handle = thread::spawn(move || { batch_reaper_task.start() });

        Ok(Self { config, batches, batch_publisher, partition_selector, resume_reaping_notifier, batch_reaper_handle, shutdown })
    }
    
    pub fn send_record(&mut self, topic: String, record: Record) -> Result<()> {
        let mut batches= self.batches.lock().unwrap();
        let was_empty = batches.is_empty();

        let partition = self.partition_selector.select_partition(&topic, &record)?;
        let topic_partition = TopicPartition { topic: topic.to_owned(), partition };

        let records = &mut batches.entry(topic_partition)
            .or_insert_with(|| InflightBatch { start: Instant::now(), records: Vec::new() })
            .records;

        records.push(record);

        if records.len() >= self.config.max_batch_size {
            let topic_partition = TopicPartition { topic: topic.to_owned(), partition };
            let inflight_batch = batches.remove(&topic_partition).unwrap();
            publish_batch_async(self.config.protocol_version, topic_partition, inflight_batch.records, &self.batch_publisher)
        } else if was_empty {
            // we inserted an entry in an empty map, so we wake up the reaper task to start tracking its expiry
            self.resume_reaping_notifier.notify_one();
        }

        Ok(())
    }

    /// Terminates the batch reaper thread and consumes ownership to discard this instance.
    pub fn shutdown(self) -> thread::Result<()> {
        self.shutdown.store(true, atomic::Ordering::Relaxed);
        // Resume the reaper thread in case it was in indefinite wait mode (does it when it found the map empty). The shutdown flag set above will tell it
        // to break the loop.
        self.resume_reaping_notifier.notify_one();
        self.batch_reaper_handle.join()?;

        // the reaper is now shut down, we should be the only ones still holding a reference to the batches
        match Arc::into_inner(self.batches) {
            None => panic!("Tried unwrapping the shared batch mutex reference while multiple references to it were still alive"),
            Some(batches) => {
                batches.into_inner().unwrap().into_iter().for_each(|(topic_partition, batch)| {
                    publish_batch_async(self.config.protocol_version, topic_partition, batch.records, &self.batch_publisher);
                });
            }
        }

        // now that we have submitted all remaining batches to the batch publisher, we can notify it to complete its work and shutdown
        match Arc::into_inner(self.batch_publisher) {
            None => panic!("Tried unwrapping the shared batch publisher reference while multiple references to it were still alive"),
            Some(batch_publisher) => batch_publisher.shutdown()
        }
    }
}

/// This struct handles the flushing of batches based on the configured [`common::Config::linger_duration`]. It does so in a separate thread,
/// deciding how long to sleep before the next time a batch will expire, and sending the actual flush task to [BatchPublisher] when an expired
/// batch is encountered.
struct BatchReaperTask {
    config: Config,
    resume_reaping_notifier: Arc<Condvar>,
    batches: Arc<Mutex<LinkedHashMap<TopicPartition, InflightBatch>>>,
    batch_publisher: Arc<BatchPublisher>,
    shutdown: Arc<AtomicBool>,
}

impl BatchReaperTask {
    fn start(&self) {
        loop {
            let mut batches = self.batches.lock().unwrap();
            batches = self.resume_reaping_notifier.wait_while(batches, |b|
                b.is_empty() && !self.shutdown.load(atomic::Ordering::Relaxed)).unwrap();

            if self.shutdown.load(atomic::Ordering::Relaxed) { break }

            // Implementation note: we essentially rely on the iteration order of the linked list within the hash map to know which ones
            // were inserted first. That's why it's important to remove the entry once we flush the batch out, so that next time a record
            // falls in the same partition, the new batch gets inserted at the end.
            let now = Instant::now();
            let expired_keys = batches.iter()
                .take_while(|(_, batch)| now.duration_since(batch.start).ge(&self.config.linger_duration))
                .map(|(topic_partition, _) | topic_partition.clone())
                .collect::<Vec<_>>();

            for expired_key in expired_keys {
                let records = batches.remove(&expired_key).unwrap().records;
                publish_batch_async(self.config.protocol_version, expired_key, records, &self.batch_publisher);
            }

            match batches.iter().next() {
                Some((_, inflight_batch)) => {
                    let time_to_expiry =  self.config.linger_duration.sub(now.duration_since(inflight_batch.start));
                    drop(batches); //  release the lock before sleeping
                    thread::sleep(time_to_expiry);
                }
                None => continue,
            }
        }
    }
}

fn publish_batch_async(protocol_version: u8, topic_partition: TopicPartition, records: Vec<Record>, batch_publisher: &BatchPublisher) {
    assert!(records.len() > 0); // only called internally so it seems fair to panic, it cannot be a user error

    let base_timestamp = records.iter().min_by_key(|record| record.timestamp).unwrap().timestamp;
    let batch = RecordBatch {
        protocol_version,
        base_timestamp,
        records,
    };

    batch_publisher.publish_async(topic_partition, batch);
}