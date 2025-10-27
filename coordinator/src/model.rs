use serde::{Deserialize, Serialize};

pub const CREATE_TOPIC: &str = "/create-topic";
pub const GET_TOPIC: &str = "/get-topic";
pub const INCREMENT_WRITE_OFFSET: &str = "/increment-write-offset";
pub const GET_WRITE_OFFSET: &str = "/get-write-offset";
pub const ACK_READ_OFFSET: &str = "/ack-read-offset";
pub const GET_READ_OFFSET: &str = "/get-read-offset";
pub const LIST_BROKERS: &str = "/list-brokers";
pub const REGISTER_BROKER: &str = "/register-broker";

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
#[derive(Debug, PartialEq, Eq)]
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
}