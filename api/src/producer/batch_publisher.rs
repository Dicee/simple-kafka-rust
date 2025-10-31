use crate::common::{self, map_broker_error, broker_resolver::BrokerResolver};
use broker::model::TopicPartition;
use protocol::record::{serialize_batch, RecordBatch};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;

#[cfg(test)]
#[path = "./batch_publisher_test.rs"]
mod batch_publisher_test;

/// The [BatchPublisher] takes care of receiving [PublishRequest] instances on an MPSC blocking channel and process them sequentially on a single thread.
/// This design ensures that ordering is respected: if a batch is sent before another one, we know we will publish it before. Of course, this is also
/// potentially a bottleneck, however there would generally be multiple [producer::Client] instances running on different threads to parallelize publishing.
/// Additionally, we could also spawn asynchronous tasks (one per topic/partition, which shouldn't be too expensive are tasks are made to be inexpensive
/// practical equivalents of a thread). The advantage of this design is mainly that we do the publishing on a separate thread from the batching, which runs
/// in a tight loop and needs to acquire a lock to keep the internal state safe due to concurrent accesses by the flusher thread which enforces the linger
/// duration. Using [BatchPublisher], we can release the lock as soon as possible and perform network calls on a separate thread.
///
/// One important point we will not handle here is robustness, as it adds significant complexity. Indeed, publishing asynchronously means that the client
/// won't easily know if what they have sent through [producer::Client] has indeed been published. The process could also crash before everything has
/// successfully been processed. In a more robust, production-ready design, we'd need to include retries and potentially temporary persistence of the records
/// to recover in case of a process crash. Because the project is already pretty ambitious and challenging for a personal project, I'll simplify here and
/// assume that life is good and brokers and the network are infinitely reliable.
pub struct BatchPublisher {
    join_handle: JoinHandle<()>,
    tx: Sender<PublishRequest>,
}

struct PublishRequest {
    pub topic_partition: TopicPartition,
    pub batch: RecordBatch,
}

impl BatchPublisher {
    pub fn new(coordinator: Arc<dyn coordinator::Client>, broker_resolver: BrokerResolver) -> Self {
        let (tx, rx) = mpsc::channel();

        let publisher_task = BatchPublisherTask {
            coordinator: Arc::clone(&coordinator),
            broker_resolver,
            rx,
        };

        let join_handle = thread::spawn(move || publisher_task.start_publish_loop());
        Self { tx, join_handle }
    }

    /// Submits a publish task on the queue and returns immediately
    /// # Panicking
    /// This method will panic if the publish task has closed the channel on the receiving end, which should not be possible.
    pub fn publish_async(&self, topic_partition: TopicPartition, batch: RecordBatch) {
        self.tx.send(PublishRequest { topic_partition, batch }).expect("Receiver unexpectedly dropped");
    }

    /// Terminates the publish thread and consumes ownership to discard this instance.
    pub fn shutdown(self) -> thread::Result<()> {
        drop(self.tx); // prompt the publish loop to stop once all messages have been consumed
        self.join_handle.join()
    }
}

struct BatchPublisherTask {
    coordinator: Arc<dyn coordinator::Client>,
    broker_resolver: BrokerResolver,
    rx: Receiver<PublishRequest>,
}

impl BatchPublisherTask {
    fn start_publish_loop(&self) {
        loop {
            match self.rx.recv() {
                Ok(request) => {
                    let TopicPartition { topic, partition } = request.topic_partition;
                    if let Err(e) = self.do_publish(&topic, partition, request.batch) {
                        // in real-life we'd have more error-handling such as retries, perhaps dedicated retry topics etc, but as explained in
                        // the documentation of BatchPublisher, all of this comes with a lot of complexity and for now I need to simplify some parts
                        // to be able to get something working end-to-end
                        eprintln!("Failed to publish batch for topic {} and partition {} due to {:?}", topic, partition, e);
                    }
                },
                Err(_) => {
                    println!("Publish request sender disconnected, interrupting publish loop.");
                    break;
                }
            }
        }
    }

    fn do_publish(&self, topic: &str, partition: u32, batch: RecordBatch) -> common::Result<()> {
        let record_count = batch.records.len() as u32;
        let bytes = serialize_batch(batch);

        let broker = self.broker_resolver.client_for(partition);
        broker.publish_raw(&topic, partition, bytes, record_count).map_err(map_broker_error)?;

        Ok(())
    }
}