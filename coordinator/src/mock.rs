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
    fn create_topic(&self, _: CreateTopicRequest) -> Result<()> { unimplemented!() }
    fn get_topic(&self, _: GetTopicRequest) -> Result<GetTopicResponse> { unimplemented!() }
    fn list_brokers(&self) -> Result<ListBrokersResponse> { unimplemented!() }
    fn register_broker(&self, _: RegisterBrokerRequest) -> Result<()> { unimplemented!() }

    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> Result<()> {
        let mut write_offsets = self.write_offsets.lock().unwrap();
        write_offsets
            .entry(TopicPartition::new(request.topic, request.partition))
            .and_modify(|offset| *offset += request.inc as u64)
            .or_insert(request.inc as u64 - 1); // since it's 0-indexed, the first time we have to increment by request.inc - 1

        Ok(())
    }

    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> Result<GetWriteOffsetResponse> {
        let write_offsets = self.write_offsets.lock().unwrap();
        Ok(GetWriteOffsetResponse { offset: write_offsets.get(&TopicPartition::new(request.topic, request.partition)).copied() })
    }

    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> Result<()> {
        let mut read_offsets = self.read_offsets.lock().unwrap();
        let read_offset = read_offsets
            .entry(TopicPartitionConsumer::new(request.topic, request.partition, request.consumer_group))
            .or_insert(0);

        *read_offset = request.offset;
        Ok(())
    }

    fn get_read_offset(&self, request: GetReadOffsetRequest) -> Result<GetReadOffsetResponse> {
        let read_offsets = self.read_offsets.lock().unwrap();
        let key = TopicPartitionConsumer::new(request.topic, request.partition, request.consumer_group);
        Ok(GetReadOffsetResponse { offset: read_offsets.get(&key).copied() })
    }
}