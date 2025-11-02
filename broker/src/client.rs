use actix_web::http::header;
use crate::model::*;
use client_utils::ApiClient;
use mockall::automock;

// re-exporting gives a nicer feeling of homogeneity to the users, and also gives us the freedom to change the definition of the result type transparently
pub use client_utils::Result as Result;
pub use client_utils::Error as Error;
use protocol::record::RecordBatch;

#[automock]
pub trait Client : Send + Sync {
    fn publish(&self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<PublishResponse>;
    fn publish_raw(&self, topic: &str, partition: u32, bytes: Vec<u8>, record_count: u32) -> Result<PublishResponse>;
    fn read_next_batch(&self, topic: String, partition: u32, consumer_group: String) -> Result<RecordBatchWithOffset>;
    fn read_next_batch_raw(&self, topic: String, partition: u32, consumer_group: String) -> Result<RawRecordBatchWithOffset>;
}

pub struct ClientImpl {
    api_client: ApiClient,
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

    fn read_next_batch(&self, topic: String, partition: u32, consumer_group: String) -> Result<RecordBatchWithOffset> {
        self.api_client.post_and_parse(&format!("{READ_NEXT_BATCH}"), ReadNextBatchRequest { topic, partition, consumer_group })
    }

    fn read_next_batch_raw(&self, topic: String, partition: u32, consumer_group: String) -> Result<RawRecordBatchWithOffset> {
        let response = self.api_client.post(&format!("{READ_NEXT_BATCH_RAW}"), ReadNextBatchRequest { topic, partition, consumer_group })?;
        let base_offset = ApiClient::get_required_header(BASE_OFFSET_HEADER, &response)?
            .parse().map_err(|e| Error::Api(format!("Failed to parse {BASE_OFFSET_HEADER} to u64 due to {e:?}")))?;

        Ok(RawRecordBatchWithOffset {
            base_offset,
            bytes: response.into_body().read_to_vec()?
        })
    }
 }
