use std::time::Duration;
use serde::{Deserialize, Serialize};
use protocol::record::RecordBatch;

// POST operations
pub const PUBLISH: &str = "/publish";
pub const PUBLISH_RAW: &str = "/publish-raw";
pub const POLL_BATCHES_RAW: &str = "/poll-batches-raw";

// GET resources
pub const TOPICS: &str = "/topics";
pub const PARTITIONS: &str = "partitions";
pub const CONSUMER_GROUPS: &str = "consumer-groups";
pub const NEXT_BATCH: &str = "next-batch";

// query parameters
pub const TOPIC: &str = "topic";
pub const PARTITION: &str = "partition";
pub const RECORD_COUNT: &str = "record_count";

// headers
pub const READ_OFFSET_HEADER: &str = "ack-read-offset";

/// Public-facing errors for the broker service
pub type BrokerApiError = client_utils::Error<BrokerApiErrorKind>;
#[derive(Serialize, Deserialize, Debug)]
pub enum BrokerApiErrorKind {
    BadRequest,
    CoordinatorFailure,
    Internal,
}

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct PublishRawRequest {
    pub topic: String,
    pub partition: u32,
    pub record_count: u32,
}

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
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
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RecordBatchWithOffset {
    pub base_offset: u64,
    pub batch: RecordBatch,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Debug)]
pub struct PollBatchesRequest {
    pub topic: String,
    pub partition: u32,
    pub consumer_group: String,
    pub poll_config: PollConfig,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct PollBatchesRawResponse {
    pub ack_read_offset: Option<u64>,
    pub bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub struct PollConfig {
    pub max_wait: Duration,
    pub max_batches: usize,
}