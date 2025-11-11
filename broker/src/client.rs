use std::borrow::Cow;
use crate::model::*;
use client_utils::ApiClient;
use mockall::automock;

use protocol::record::RecordBatch;

pub type Error = client_utils::Error<BrokerApiErrorKind>;
pub type Result<T> = std::result::Result<T, Error>;

#[automock]
pub trait Client : Send + Sync {
    fn publish(&self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<PublishResponse>;
    fn publish_raw(&self, topic: &str, partition: u32, bytes: Vec<u8>, record_count: u32) -> Result<PublishResponse>;
    fn poll_batches_raw(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64, poll_config: PollConfig) -> Result<PollBatchesRawResponse>;
}

pub struct ClientImpl {
    api_client: ApiClient<BrokerApiErrorKind>,
}

impl ClientImpl {
    pub fn new(domain: String) -> Self {
        // for our local testing, we'll always use unsecure HTTP
        Self { api_client: ApiClient::new(domain, false, false) }
    }
}

impl Client for ClientImpl {
    fn publish(&self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<PublishResponse> {
        self.api_client.post_and_parse(&format!("{PUBLISH}?{TOPIC}={topic}&{PARTITION}={partition}"), record_batch)
    }

    fn publish_raw(&self, topic: &str, partition: u32, bytes: Vec<u8>, record_count: u32) -> Result<PublishResponse> {
        self.api_client.post_raw_and_parse(&format!(
            "{PUBLISH_RAW}?\
            {TOPIC}={topic}&\
            {PARTITION}={partition}&\
            {RECORD_COUNT}={record_count}"
        ), bytes)
    }

    fn poll_batches_raw(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64, poll_config: PollConfig) -> Result<PollBatchesRawResponse> {
        let request = PollBatchesRequest { topic: Cow::Borrowed(topic), partition, consumer_group: Cow::Borrowed(consumer_group), offset, poll_config };
        let response = self.api_client.post(POLL_BATCHES_RAW, request)?;
        let ack_read_offset = match ApiClient::get_optional_header(READ_OFFSET_HEADER, &response)? {
            None => None,
            Some(header) => Some(header.parse().map_err(|e| Error::InvalidResponse(format!("Failed to parse {READ_OFFSET_HEADER} to u64 due to {e:?}")))?)
        };

        Ok(PollBatchesRawResponse {
            ack_read_offset,
            bytes: response.into_body().read_to_vec()?,
        })
    }
 }
