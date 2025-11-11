//! This module vends some common mock objects that will be useful to have in several crates to mock the behaviour of the coordinator. Unfortunately,
//! Rust's mocking capabilities aren't great, which is why I had to add this extra code just for testing.

use std::collections::HashMap;
use std::sync::Mutex;
use crate::model::*;
use crate::client::Result;

pub struct DummyClient {
    write_offsets: Mutex<HashMap<TopicPartition, u64>>,
    read_offsets: Mutex<HashMap<TopicPartitionConsumer, u64>>,
}

impl DummyClient {
    pub fn new() -> Self { Self { write_offsets: Mutex::new(HashMap::new()) ,read_offsets: Mutex::new(HashMap::new()) } }
}

impl crate::client::Client for DummyClient {
    fn increment_write_offset(&self, topic: &str, partition: u32, inc: u32) -> Result<()> {
        let mut write_offsets = self.write_offsets.lock().unwrap();
        write_offsets
            .entry(TopicPartition::new(topic.to_owned(), partition))
            .and_modify(|offset| *offset += inc as u64)
            .or_insert(inc as u64 - 1); // since it's 0-indexed, the first time we have to increment by request.inc - 1

        Ok(())
    }

    fn get_write_offset(&self, topic: &str, partition: u32) -> Result<GetWriteOffsetResponse> {
        let write_offsets = self.write_offsets.lock().unwrap();
        Ok(GetWriteOffsetResponse { offset: write_offsets.get(&TopicPartition::new(topic.into(), partition)).copied() })
    }

    fn ack_read_offset(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64) -> Result<()> {
        let mut read_offsets = self.read_offsets.lock().unwrap();
        let read_offset = read_offsets
            .entry(TopicPartitionConsumer::new(topic.into(), partition, consumer_group.into()))
            .or_insert(0);

        *read_offset = offset;
        Ok(())
    }

    fn get_read_offset(&self, topic: &str, partition: u32, consumer_group: &str) -> Result<GetReadOffsetResponse> {
        let read_offsets = self.read_offsets.lock().unwrap();
        let key = TopicPartitionConsumer::new(topic.into(), partition, consumer_group.into());
        Ok(GetReadOffsetResponse { offset: read_offsets.get(&key).copied() })
    }

    fn create_topic(&self, _: &str, _: u32) -> Result<()> { unimplemented!() }
    fn get_topic(&self, _: &str) -> Result<GetTopicResponse> { unimplemented!() }
    fn register_broker(&self, _: &str, _: u16) -> Result<()> { unimplemented!() }
    fn list_brokers(&self) -> Result<ListBrokersResponse> { unimplemented!() }
}