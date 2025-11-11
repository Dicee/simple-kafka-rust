use crate::common::broker_resolver::BrokerResolver;
use crate::common::{map_broker_error, map_coordinator_error};
use broker::model::PollConfig;
use protocol::record::{read_next_batch, Record};
use std::io::{Cursor, Read};
use std::sync::Arc;

#[cfg(test)]
#[path = "consumer_test.rs"]
mod consumer_test;

/// Basic client to consume a collection of topics from a single consumer (for now). The records are served as an iterator of batches, in round-robin fashion
/// for fairness. Of course, if one of the topics has significantly more partitions than the other, its records will be dominant in the stream. All the partitions
/// for a given topic will be processed sequentially (one polling operation for each) before moving on to the next topic, in an infinite cycle. For efficiency
/// reasons, the broker is called with long polling, and each request fetches multiple batches. Those batches will show up consecutively in the iterator. The
/// client is single-threaded, so obviously not scalable tht the moment. Eventually, we'll implement consumer groups so that multiple clients can cooperate
/// on the same set of topics. The client can subscribe only once in its lifetime, and then live through the stream itself.
///
/// Note that because of the round-robin behaviour, it is not recommended to consume multiple topics within the same iterator if they have very different
/// message rates, as the slow topic(s) will cause long polling to wait more often than not and delay faster topics.
pub struct Client {
    coordinator: Arc<dyn coordinator::Client>,
    broker_resolver: Arc<BrokerResolver>,
    poll_config: PollConfig,
}

pub struct ConsumerRecordIter {
    consumer_group: String,
    poll_config: PollConfig,
    consumer_states: Vec<TopicConsumerState>,
    state_index: usize,
    buffer: Box<dyn Iterator<Item = ConsumerRecord>>,
    broker_resolver: Arc<BrokerResolver>,
}

#[derive(Debug)]
struct TopicConsumerState {
    topic: String,
    partition_count: u32,
    current_partition: u32,
}

#[derive(Eq, PartialEq, Debug)]
pub struct ConsumerRecord {
    pub offset: u64,
    pub topic: String,
    pub partition: u32,
    pub record: Record,
}

impl Client {
    pub fn new(poll_config: PollConfig, coordinator: Arc<dyn coordinator::Client>) -> crate::common::Result<Self> {
        let broker_resolver = Arc::new(BrokerResolver::new(Arc::clone(&coordinator))?);
        Ok(Self::new_for_testing(coordinator, poll_config, broker_resolver))
    }

    fn new_for_testing(coordinator: Arc<dyn coordinator::Client>, poll_config: PollConfig, broker_resolver: Arc<BrokerResolver>) -> Self {
        Self { coordinator, broker_resolver, poll_config }
    }

    /// Creates an iterator subscribed to a collection of topics. It is up to the consumer to ensure they're all unique. I would have preferred passing a
    /// LinkedHashSet but I didn't like that there wasn't a neat way to instantiate them with elements.
    pub fn subscribe(&self, consumer_group: String, topics: Vec<String>) -> crate::common::Result<ConsumerRecordIter> {
        let mut consumer_states = Vec::new();

        for topic in topics {
            let response = self.coordinator.get_topic(&topic).map_err(map_coordinator_error)?;
            consumer_states.push(TopicConsumerState {
                topic: response.name,
                partition_count: response.partition_count,
                current_partition: 0,
            });
        }
        Ok(ConsumerRecordIter {
            consumer_group,
            poll_config: self.poll_config,
            consumer_states,
            state_index: 0,
            buffer: Box::new(std::iter::empty()),
            broker_resolver: Arc::clone(&self.broker_resolver),
        })
    }
}

impl Iterator for ConsumerRecordIter {
    type Item = crate::common::Result<ConsumerRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(record) = self.buffer.next() { return Some(Ok(record)); }

        let state = self.consumer_states.get_mut(self.state_index).unwrap();
        let broker = self.broker_resolver.client_for(state.current_partition);

        // TODO: read the correct offset
        match broker.poll_batches_raw(state.topic.clone(), state.current_partition, self.consumer_group.clone(), 0, self.poll_config) {
            Err(e) => return Some(Err(map_broker_error(e))),
            Ok(response) => self.buffer = Box::new(Self::decode_records(&state.topic, state.current_partition, response.bytes).into_iter())
        }

        state.current_partition = (state.current_partition + 1) % state.partition_count;

        // once we processed all the partitions of a given topic, we move to the next topic
        if state.current_partition == 0 {
            self.state_index = (self.state_index + 1) % self.consumer_states.len();
        }

        // we just loaded the buffer, so we call next again (without having consumed an element) to serve the next elements from the buffer,
        // without code duplication
        self.next()
    }
}

impl ConsumerRecordIter {
    fn decode_records(topic: &str, partition: u32, bytes: Vec<u8>) -> Vec<ConsumerRecord> {
        if bytes.is_empty() { return Vec::new() }

        let byte_count = bytes.len() as u64;
        let mut batches = Vec::new();
        let mut cursor = Cursor::new(bytes);

        while cursor.position() < byte_count - 1 {
            // there shouldn't be IO errors since it's in memory. If there are errors it must be that we reached EOF too early, which is
            // either data corruption, or a programming error, both of which are on-recoverable, so panicking is fair
            let mut buf = [0u8 ; 8];
            cursor.read_exact(&mut buf).unwrap();
            let base_offset = u64::from_le_bytes(buf);

            for (i, record) in read_next_batch(&mut cursor).unwrap().records.into_iter().enumerate() {
                batches.push(ConsumerRecord {
                    topic: topic.to_owned(),
                    record,
                    partition,
                    offset: base_offset + i as u64
                });
            }
        }

        batches
    }
}

