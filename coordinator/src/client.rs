use crate::model::*;
use client_utils::ApiClient;
use mockall::automock;

// re-exporting gives a nicer feeling of homogeneity to the users, and also gives us the freedom to change the definition of the result type transparently
pub use client_utils::Result as Result;
pub use client_utils::Error as Error;

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
}

impl ClientImpl {
    pub fn new(domain: String) -> Self {
        // for our local testing, we'll always use unsecure HTTP
        Self { api_client: ApiClient::new(domain, false, false) }
    }
}

impl Client for ClientImpl {
    fn create_topic(&self, request: CreateTopicRequest) -> Result<()> {
        self.api_client.post(CREATE_TOPIC, request)?;
        Ok(())
    }

    fn get_topic(&self, request: GetTopicRequest) -> Result<GetTopicResponse> {
        Ok(self.api_client.get(&format!("{TOPICS}/{}", request.name))?)
    }

    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> Result<()> {
        self.api_client.post(INCREMENT_WRITE_OFFSET, request)?;
        Ok(())
    }

    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> Result<GetWriteOffsetResponse> {
        Ok(self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{WRITE_OFFSET}", request.topic, request.partition))?)
    }

    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> Result<()> {
        self.api_client.post(ACK_READ_OFFSET, request)?;
        Ok(())
    }

    fn get_read_offset(&self, request: GetReadOffsetRequest) -> Result<GetReadOffsetResponse> {
        Ok(self.api_client.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{CONSUMER_GROUPS}/{}/{READ_OFFSET}",
            request.topic, request.partition, request.consumer_group))?)
    }

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<()> {
        self.api_client.post(REGISTER_BROKER, request)?;
        Ok(())
    }

    fn list_brokers(&self) -> Result<ListBrokersResponse> {
        Ok(self.api_client.get(BROKERS)?)
    }
}