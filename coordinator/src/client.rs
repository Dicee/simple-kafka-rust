use crate::model::*;
use client_utils::ApiClient;
use mockall::automock;
use std::collections::HashMap;
use std::sync::Mutex;

// re-exporting gives a nicer feeling of homogeneity to the users, and also gives us the freedom to change the definition of the result type transparently
pub use client_utils::Result as Result;
pub use client_utils::Error as Error;

#[cfg(test)]
#[path = "./client_test.rs"]
mod client_test;

#[automock]
pub trait Client : Send + Sync {
    fn create_topic(&self, request: CreateTopicRequest) -> Result<()>;
    fn get_topic(&self, request: GetTopicRequest) -> Result<GetTopicResponse>;
    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> Result<()>;
    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> Result<GetWriteOffsetResponse>;
    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> Result<()>;
    fn get_read_offset(&self, request: GetReadOffsetRequest) -> Result<GetReadOffsetResponse>;
    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<()>;
    fn list_brokers(&self) -> Result<ListBrokersResponse>;
}

pub struct ClientImpl {
    api_client: ApiClient,
    topics: Mutex<HashMap<String, GetTopicResponse>>,
}

impl ClientImpl {
    pub fn new(domain: String) -> Self {
        Self {
            // for our local testing, we'll always use unsecure HTTP
            api_client: ApiClient::new(domain, false, false),
            topics: Mutex::new(HashMap::new()),
        }
    }
}

impl Client for ClientImpl {
    fn create_topic(&self, request: CreateTopicRequest) -> Result<()> {
        self.api_client.post(CREATE_TOPIC, request)?;
        Ok(())
    }

    fn get_topic(&self, request: GetTopicRequest) -> Result<GetTopicResponse> {
        // this information is static in all our simulations, so highly cachable
        let mut topics = self.topics.lock().unwrap();
        if let Some(response)  = topics.get(&request.name) {
            return Ok(response.clone())
        }

        let topic_name = request.name;
        let response: GetTopicResponse = self.api_client.get(&format!("{TOPICS}/{}", topic_name.clone()))?;
        topics.insert(topic_name, response.clone());

        Ok(response)
    }

    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> Result<()> {
        self.api_client.post(INCREMENT_WRITE_OFFSET, request)?;
        Ok(())
    }

    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> Result<GetWriteOffsetResponse> {
        self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{WRITE_OFFSET}", request.topic, request.partition))
    }

    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> Result<()> {
        self.api_client.post(ACK_READ_OFFSET, request)?;
        Ok(())
    }

    fn get_read_offset(&self, request: GetReadOffsetRequest) -> Result<GetReadOffsetResponse> {
        self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{CONSUMER_GROUPS}/{}/{READ_OFFSET}",
            request.topic, request.partition, request.consumer_group))
    }

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<()> {
        self.api_client.post(REGISTER_BROKER, request)?;
        Ok(())
    }

    fn list_brokers(&self) -> Result<ListBrokersResponse> { self.api_client.get(BROKERS) }
}