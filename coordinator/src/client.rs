use std::borrow::Cow;
use crate::model::*;
use client_utils::ApiClient;
use mockall::automock;
use std::collections::HashMap;
use std::sync::Mutex;

pub type Error = client_utils::Error<CoordinatorApiErrorKind>;
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[path = "./client_test.rs"]
mod client_test;

#[automock]
pub trait Client : Send + Sync {
    fn create_topic(&self, name: &str, partition_count: u32) -> Result<()>;
    fn get_topic(&self, name: &str) -> Result<GetTopicResponse>;
    fn increment_write_offset(&self, topic: &str, partition: u32, inc: u32) -> Result<()>;
    fn get_write_offset(&self, topic: &str, partition: u32) -> Result<GetWriteOffsetResponse>;
    fn ack_read_offset(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64) -> Result<()>;
    fn get_read_offset(&self, topic: &str, partition: u32, consumer_group: &str) -> Result<GetReadOffsetResponse>;
    fn register_broker(&self, host: &str, port: u16) -> Result<()>;
    fn list_brokers(&self) -> Result<ListBrokersResponse>;
}

pub struct ClientImpl {
    api_client: ApiClient<CoordinatorApiErrorKind>,
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
    fn create_topic(&self, name: &str, partition_count: u32) -> Result<()> {
        self.api_client.post(CREATE_TOPIC, CreateTopicRequest { name: Cow::Borrowed(name), partition_count })?;
        Ok(())
    }

    fn get_topic(&self, name: &str) -> Result<GetTopicResponse> {
        // this information is static in all our simulations, so highly cachable
        let mut topics = self.topics.lock().unwrap();
        if let Some(response)  = topics.get(name) {
            return Ok(response.clone())
        }

        let response: GetTopicResponse = self.api_client.get(&format!("{TOPICS}/{}", name))?;
        topics.insert(name.to_owned(), response.clone());

        Ok(response)
    }

    fn increment_write_offset(&self, topic: &str, partition: u32, inc: u32) -> Result<()> {
        self.api_client.post(INCREMENT_WRITE_OFFSET, IncrementWriteOffsetRequest { topic: Cow::Borrowed(topic), partition, inc })?;
        Ok(())
    }

    fn get_write_offset(&self, topic: &str, partition: u32) -> Result<GetWriteOffsetResponse> {
        self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{WRITE_OFFSET}", topic, partition))
    }

    fn ack_read_offset(&self, topic: &str, partition: u32, consumer_group: &str, offset: u64) -> Result<()> {
        self.api_client.post(ACK_READ_OFFSET, AckReadOffsetRequest { 
            topic: Cow::Borrowed(topic), 
            partition,
            consumer_group: Cow::Borrowed(consumer_group),
            offset,
        })?;
        Ok(())
    }

    fn get_read_offset(&self, topic: &str, partition: u32, consumer_group: &str) -> Result<GetReadOffsetResponse> {
        self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{CONSUMER_GROUPS}/{}/{READ_OFFSET}", topic, partition, consumer_group))
    }

    fn register_broker(&self, host: &str, port: u16) -> Result<()> {
        self.api_client.post(REGISTER_BROKER, RegisterBrokerRequest { host: Cow::Borrowed(host), port })?;
        Ok(())
    }

    fn list_brokers(&self) -> Result<ListBrokersResponse> { self.api_client.get(BROKERS) }
}