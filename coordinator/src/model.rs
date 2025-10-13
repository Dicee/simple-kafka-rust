use serde::{Deserialize, Serialize};

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