use serde::Serialize;
use protocol::record::RecordBatch;

#[derive(Serialize)]
#[derive(Eq, PartialEq)]
pub struct PublishResponse {
    pub base_offset: u64,
}

#[derive(Serialize)]
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RecordBatchWithOffset {
    pub base_offset: u64,
    pub batch: RecordBatch,
}