use serde::{Deserialize, Serialize};

// POST operations
pub const CREATE_TOPIC: &str = "/create-topic";
pub const INCREMENT_WRITE_OFFSET: &str = "/increment-write-offset";
pub const ACK_READ_OFFSET: &str = "/ack-read-offset";
pub const REGISTER_BROKER: &str = "/register-broker";

// GET resources
pub const TOPICS: &str = "/topics";
pub const PARTITIONS: &str = "partitions";
pub const CONSUMER_GROUPS: &str = "consumer-groups";
pub const WRITE_OFFSET: &str = "write-offset";
pub const READ_OFFSET: &str = "read-offset";
pub const BROKERS: &str = "/brokers";

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partition_count: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct GetTopicRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GetTopicResponse {
    pub name: String,
    pub partition_count: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct IncrementWriteOffsetRequest {
    pub topic: String,
    pub partition: u32,
    pub inc: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct GetWriteOffsetRequest {
    pub topic: String,
    pub partition: u32,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct GetWriteOffsetResponse {
    pub offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct AckReadOffsetRequest {
    pub topic: String,
    pub partition: u32,
    pub consumer_group: String,
    pub offset: u64,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct GetReadOffsetRequest {
    pub topic: String,
    pub partition: u32,
    pub consumer_group: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct GetReadOffsetResponse {
    pub offset: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    pub status_code: u16,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct ListBrokersRequest {}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct ListBrokersResponse {
    pub brokers: Vec<HostAndPort>,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq)]
pub struct RegisterBrokerRequest {
    pub host: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct HostAndPort {
    pub host: String,
    pub port: u16,
}

impl HostAndPort {
    pub fn new(host: String, port: u16) -> Self { Self { host, port } }
    
    pub fn full_domain(&self) -> String { format!("{}:{}", self.host, self.port) }
}

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Hash, Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: u32,
}

impl TopicPartition {
    pub fn new(topic: String, partition: u32) -> Self { Self { topic, partition } }
}

#[derive(Deserialize)]
#[derive(Eq, PartialEq, Hash, Debug)]
pub struct TopicPartitionConsumer {
    pub topic: String,
    pub partition: u32,
    pub consumer_group: String,
}

impl TopicPartitionConsumer {
    pub fn new(topic: String, partition: u32, consumer_group: String) -> Self { Self { topic, partition, consumer_group } }
}