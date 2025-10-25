//! This module vends some common mock objects that will be useful to have in several crates to mock the behaviour of the coordinator. Unfortunately,
//! Rust's mocking capabilities aren't great, which is why I had to add this extra code just for testing.

use std::collections::HashMap;
use std::sync::Mutex;
use crate::model::*;

pub struct DummyCoordinatorClient {
    pub write_offsets: Mutex<HashMap<TopicPartition, u64>>,
    pub read_offsets: Mutex<HashMap<TopicPartitionConsumer, u64>>,
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct TopicPartition(String, u32);

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct TopicPartitionConsumer(String, u32, String);

impl DummyCoordinatorClient {
    pub fn new() -> Self { Self { write_offsets: Mutex::new(HashMap::new()) ,read_offsets: Mutex::new(HashMap::new()) } }
}

impl crate::client::Client for DummyCoordinatorClient {
    fn create_topic(&self, request: CreateTopicRequest) -> crate::client::Result<()> { unimplemented!() }
    fn get_topic(&self, request: GetTopicRequest) -> crate::client::Result<GetTopicResponse> { unimplemented!() }

    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> crate::client::Result<()> {
        let mut write_offsets = self.write_offsets.lock().unwrap();
        write_offsets
            .entry(TopicPartition(request.topic, request.partition))
            .and_modify(|offset| *offset += request.inc as u64)
            .or_insert(request.inc as u64 - 1); // since it's 0-indexed, the first time we have to increment by request.inc - 1

        Ok(())
    }

    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> crate::client::Result<GetWriteOffsetResponse> {
        let write_offsets = self.write_offsets.lock().unwrap();
        Ok(GetWriteOffsetResponse { offset: write_offsets.get(&TopicPartition(request.topic, request.partition)).copied() })
    }

    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> crate::client::Result<()> {
        let mut read_offsets = self.read_offsets.lock().unwrap();
        let read_offset = read_offsets
            .entry(TopicPartitionConsumer(request.topic, request.partition, request.consumer_group))
            .or_insert(0);

        *read_offset = request.offset;
        Ok(())
    }

    fn get_read_offset(&self, request: GetReadOffsetRequest) -> crate::client::Result<GetReadOffsetResponse> {
        let read_offsets = self.read_offsets.lock().unwrap();
        let key = TopicPartitionConsumer(request.topic, request.partition, request.consumer_group);
        Ok(GetReadOffsetResponse { offset: read_offsets.get(&key).copied() })
    }
}