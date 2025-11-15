use crate::common::broker_resolver::BrokerResolver;
use broker::model::{PollConfig, TopicPartition};
use protocol::record::{read_next_batch, Record};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{Cursor, Read};
use std::rc::Rc;
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
    auto_commit: bool,
}

/// Infinite stream of records for a fixed set of topics and partitions. Of course, there might be a finite number of records to read, but this iterator
/// will never stop looking for new ones. [ConsumerRecordStream::next] polls from the broker, so expect to have latency from this method when the underlying
/// topics do not receive many records. This struct also manages the state of offsets, tracking both the latest acknowledged read offset and the last
/// unacknowledged read offset. This is useful during consumer group rebalancing, to attempt to complete all the pending acknowledgements before calculating
/// the new topic/partition assignments.
pub struct ConsumerRecordStream {
    consumer_group: String,
    poll_config: PollConfig,
    offset_state_manager: Rc<OffsetStateManager>,
    consumer_states: Vec<TopicConsumerState>,
    state_index: usize,
    buffer: Box<dyn Iterator<Item = ConsumerRecord>>,
    coordinator: Arc<dyn coordinator::Client>,
    broker_resolver: Arc<BrokerResolver>,
    auto_commit: bool,
}

struct OffsetStateManager {
    offset_states: HashMap<TopicPartition, Rc<RefCell<OffsetState>>>,
    consumer_group: String,
    auto_commit: bool,
    coordinator: Arc<dyn coordinator::Client>,
}

#[derive(Debug)]
struct TopicConsumerState {
    topic: String,
    partition_count: u32,
    current_partition: u32,
}

pub struct ConsumerRecord {
    pub offset: u64,
    pub topic: String,
    pub partition: u32,
    pub record: Record,
    offset_state_manager: Rc<OffsetStateManager>,
}

#[derive(Eq, PartialEq, Debug)]
struct OffsetState {
    ack_read_offset: Option<u64>,
    unack_read_offset: Option<u64>,
}

impl Client {
    /// Creates a new consumer client. If `auto_commit` is true, all `ConsumerRecord` instances that will be acquired from this client will automatically
    /// acknowledge the corresponding offset after being dropped. As a result, it follows that the records should be consumed sequentially to prevent
    /// out-of-order acknowledgements, which would lead to error responses from the coordinator.
    pub fn new(poll_config: PollConfig, coordinator: Arc<dyn coordinator::Client>, auto_commit: bool) -> crate::common::Result<Self> {
        let broker_resolver = Arc::new(BrokerResolver::new(Arc::clone(&coordinator))?);
        Ok(Self::new_for_testing(coordinator, poll_config, broker_resolver, auto_commit))
    }

    fn new_for_testing(coordinator: Arc<dyn coordinator::Client>, poll_config: PollConfig, broker_resolver: Arc<BrokerResolver>, auto_commit: bool) -> Self {
        Self { coordinator, broker_resolver, poll_config, auto_commit }
    }

    /// Creates an iterator subscribed to a collection of topics. It is up to the consumer to ensure they're all unique. I would have preferred passing a
    /// LinkedHashSet but I didn't like that there wasn't a neat way to instantiate them with elements.
    pub fn subscribe(&self, consumer_group: String, topics: Vec<String>) -> crate::common::Result<ConsumerRecordStream> {
        let mut consumer_states = Vec::new();
        let mut offset_states = HashMap::new();

        for topic in topics {
            let response = self.coordinator.get_topic(&topic)?;
            consumer_states.push(TopicConsumerState {
                topic: response.name.into(),
                partition_count: response.partition_count,
                current_partition: 0,
            });

            for partition in 0..response.partition_count {
                let ack_read_offset = self.coordinator.get_read_offset(&topic, partition, &consumer_group)?.offset;
                let topic_partition = TopicPartition::new(topic.clone(), partition);
                offset_states.insert(topic_partition, Rc::new(RefCell::new(OffsetState { ack_read_offset, unack_read_offset: None })));
            }
        }
        Ok(ConsumerRecordStream {
            offset_state_manager: Rc::new(OffsetStateManager {
                offset_states,
                consumer_group: consumer_group.clone(),
                auto_commit: self.auto_commit,
                coordinator: Arc::clone(&self.coordinator),
            }),
            state_index: 0,
            consumer_states,
            consumer_group,
            buffer: Box::new(std::iter::empty()),
            coordinator: Arc::clone(&self.coordinator),
            broker_resolver: Arc::clone(&self.broker_resolver),
            poll_config: self.poll_config,
            auto_commit: self.auto_commit,
        })
    }
}

impl Iterator for ConsumerRecordStream {
    type Item = crate::common::Result<ConsumerRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(record) = self.buffer.next() { return Some(Ok(record)); }

        let consumer_state = self.consumer_states.get_mut(self.state_index).unwrap();
        let topic = &consumer_state.topic;

        let current_partition = consumer_state.current_partition;
        let offset_state = self.offset_state_manager.get_state_for(&TopicPartition::new(topic.clone(), current_partition));
        let mut offset_state = offset_state.borrow_mut();

        let broker = self.broker_resolver.client_for(current_partition);

        match broker.poll_batches_raw(&topic, current_partition, &self.consumer_group, offset_state.next_offset(), self.poll_config) {
            Err(e) => return Some(Err(e.into())),
            Ok(response) => {
                let records = Self::decode_records(&topic, current_partition, response.bytes, &self.offset_state_manager);
                // batches normally can't be empty, but no harm handling this case safely here.
                offset_state.update_unack_offset(records.last().map(|record| record.offset));
                self.buffer = Box::new(records.into_iter())
            },
        }

        consumer_state.current_partition = (current_partition + 1) % consumer_state.partition_count;

        // once we processed all the partitions of a given topic, we move to the next topic
        if consumer_state.current_partition == 0 {
            self.state_index = (self.state_index + 1) % self.consumer_states.len();
        }

        // we just loaded the buffer, so we call next again (without having consumed an element) to serve the next elements from the buffer,
        // without code duplication
        self.next()
    }
}

impl ConsumerRecordStream {
    fn decode_records(topic: &str, partition: u32, bytes: Vec<u8>, offset_state_manager: &Rc<OffsetStateManager>) -> Vec<ConsumerRecord> {
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
                    offset: base_offset + i as u64,
                    offset_state_manager: Rc::clone(offset_state_manager),
                });
            }
        }

        batches
    }
}

impl ConsumerRecord {
    /// Acknowledges the provided read offset for this record. Once the offset is committed, it won't be possible to read data before this offset within
    /// the same consumer group. **Warning**: to call this method, the stream the record is from MUST NOT have been dropped already, as this method needs
    /// to amend some state in the original stream. The method exists so that the user cannot make any mistake such as committing an offset for a
    /// topic/partition that is not managed by the stream. This is better than runtime validation, since it prevents this type of mistakes altogether.
    pub fn commit(&self) -> crate::common::Result<()> {
        self.offset_state_manager.commit(&self.topic, self.partition, self.offset)
    }
}

impl Drop for ConsumerRecord {
    fn drop(&mut self) {
        if let Err(e) = self.offset_state_manager.on_record_dropped(&self.topic, self.partition, self.offset) {
            eprintln!("Failed auto-committing offset for topic {} and partition {} due to {e:?}", self.topic, self.partition);
        }
    }
}

impl Debug for ConsumerRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConsumerRecord { ")?;
        f.write_fmt(format_args!("topic: {}, partition: {}, offset: {}, record: {:?}", self.topic, self.partition, self.offset, self.record))?;
        f.write_str(" }")
    }
}

impl OffsetStateManager {
    fn get_state_for(&self, topic_partition: &TopicPartition) -> Rc<RefCell<OffsetState>> {
        Rc::clone(&self.offset_states.get(topic_partition).unwrap())
    }

    fn on_record_dropped(&self, topic: &str, partition: u32, offset: u64) -> crate::common::Result<()> {
        if self.auto_commit { self.commit(topic, partition, offset) } else { Ok(()) }
    }

    fn commit(&self, topic: &str, partition: u32, offset: u64) -> crate::common::Result<()> {
        // The client must commit for a topic/partition managed by this ConsumerRecordStream, otherwise it's a programming error and panicking seems fair.
        // Additionally, note that we're checking this **before** calling the coordinator client because we shouldn't modify the authoritative state if
        // the user had no business updating this offset. I'll keep this security even though this method is internal and should never be called incorrectly.
        let topic_partition = TopicPartition::new(topic.into(), partition);
        let mut offset_state = self.offset_states.get(&topic_partition)
            .unwrap_or_else(|| panic!("Unknown topic/partition: {}/{}", topic, partition))
            .borrow_mut();

        // ignore commit calls if the offset already committed is more than the offset we're trying to commit now
        let should_commit = match offset_state.ack_read_offset {
            None => true,
            Some(read_offset) if read_offset < offset => true,
            _ => false,
        };
        if should_commit {
            self.coordinator.ack_read_offset(topic, partition, &self.consumer_group, offset)?;
            // only updated **after** the call to the coordinator was successful, to keep things consistent
            offset_state.update_ack_offset(offset);
        }

        Ok(())
    }
}

impl OffsetState {
    fn next_offset(&self) -> u64 {
        match (self.ack_read_offset, self.unack_read_offset) {
            (None, None) => 0,
            (_, Some(unack_offset)) => unack_offset + 1,
            (Some(ack_offset), None) => ack_offset + 1,
        }
    }

    fn update_unack_offset(&mut self, new_value: Option<u64>) {
        if new_value.is_some() { self.unack_read_offset = new_value; }
    }

    fn update_ack_offset(&mut self, ack_read_offset: u64) {
        self.ack_read_offset = Some(ack_read_offset);

        // Clear the unacknowledged offset if it was less than the offset we just acknowledged. In all other cases, the unacknowledged offset should
        // retain its current value.
        if let Some(unack_offset) = self.unack_read_offset && unack_offset <= ack_read_offset {
            self.unack_read_offset = None;
        }
    }
}
