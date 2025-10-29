use serde::{Deserialize, Serialize};
use protocol::record::RecordBatch;

// POST operations
pub const PUBLISH: &str = "/publish";
pub const READ_NEXT_BATCH: &str = "/read-next-batch";

// GET resources
pub const TOPICS: &str = "/topics";
pub const PARTITIONS: &str = "partitions";
pub const CONSUMER_GROUPS: &str = "consumer-groups";
pub const NEXT_BATCH: &str = "next-batch";

// query parameters
pub const TOPIC: &str = "topic";
pub const PARTITION: &str = "partition";

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Debug)]
pub struct PublishResponse {
    pub base_offset: u64,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Debug)]
pub struct ReadNextBatchRequest {
    pub topic: String,
    pub partition: u32,
    pub consumer_group: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RecordBatchWithOffset {
    pub base_offset: u64,
    pub batch: RecordBatch,
}