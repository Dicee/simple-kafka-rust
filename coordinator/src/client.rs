use crate::client::Error::*;
use crate::model::*;
use mockall::automock;
use serde::{Deserialize, Serialize};
use serde_json;
use ureq;
use ureq::config::Config;
use ureq::http::Response;
use ureq::{Agent, Body, SendBody};

#[cfg(test)]
#[path = "./client_test.rs"]
mod client_test;

#[derive(Debug)]
pub enum Error {
    Ureq(ureq::Error),
    Serde(serde_json::Error),
    Api(String),
}

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
    url_base: String,
    http_client: Box<dyn HttpClient>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl ClientImpl {
    pub fn new(domain: String, use_tls: bool) -> Self {
        let config = Agent::config_builder()
            // I don't like the default handling because it doesn't return the body, which contains error messages
            .http_status_as_error(false)
            .build();

        let http_client = Box::new(HttpClientImpl { config });
        Self::new_with_http_client(domain, use_tls, http_client)        
    }
    
    // for testing
    fn new_with_http_client(domain: String, use_tls: bool, http_client: Box<dyn HttpClient>) -> Self {
        let protocol = if use_tls { "https" } else { "http" };
        Self {
            url_base: format!("{protocol}://{domain}"),
            http_client,
        }
    }

    fn get<Res>(&self, api: &str) -> Result<Res> where for<'de> Res: Deserialize<'de> {
        let uri = format!("{}{api}", self.url_base);
        let mut response = self.http_client.get(&uri)?;

        if !response.status().is_success() {
            Err(Api(response.body_mut().read_to_string()?))
        } else {
            let body = response.body_mut().read_to_string()?;
            Ok(serde_json::from_str(&body)?)
        }
    }

    fn post<Req: Serialize>(&self, api: &str, request: Req) -> Result<Response<Body>> {
        let uri = format!("{}{api}", self.url_base);
        let mut response = self.http_client.post(&uri, SendBody::from_json(&request)?)?;

        if !response.status().is_success() {
            Err(Api(response.body_mut().read_to_string()?))
        } else {
            Ok(response)
        }
    }
}

impl Client for ClientImpl {
    fn create_topic(&self, request: CreateTopicRequest) -> Result<()> {
        self.post(CREATE_TOPIC, request)?;
        Ok(())
    }

    fn get_topic(&self, request: GetTopicRequest) -> Result<GetTopicResponse> {
        Ok(self.get(&format!("{TOPICS}/{}", request.name))?)
    }

    fn increment_write_offset(&self, request: IncrementWriteOffsetRequest) -> Result<()> {
        self.post(INCREMENT_WRITE_OFFSET, request)?;
        Ok(())
    }

    fn get_write_offset(&self, request: GetWriteOffsetRequest) -> Result<GetWriteOffsetResponse> {
        Ok(self.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{WRITE_OFFSET}", request.topic, request.partition))?)
    }

    fn ack_read_offset(&self, request: AckReadOffsetRequest) -> Result<()> {
        self.post(ACK_READ_OFFSET, request)?;
        Ok(())
    }

    fn get_read_offset(&self, request: GetReadOffsetRequest) -> Result<GetReadOffsetResponse> {
        Ok(self.get(&format!("{TOPICS}/{}/{PARTITIONS}/{}/{CONSUMER_GROUPS}/{}/{READ_OFFSET}",
            request.topic, request.partition, request.consumer_group))?)
    }

    fn register_broker(&self, request: RegisterBrokerRequest) -> Result<()> {
        self.post(REGISTER_BROKER, request)?;
        Ok(())
    }

    fn list_brokers(&self) -> Result<ListBrokersResponse> {
        Ok(self.get(BROKERS)?)
    }
}

#[automock]
trait HttpClient : Send + Sync {
    fn get(&self, uri: &str) -> std::result::Result<Response<Body>, ureq::Error>;
    fn post<'a>(&self, uri: &str, body: SendBody<'a>) -> std::result::Result<Response<Body>, ureq::Error>;
}

// Created for testing, as I didn't find a good way to do it in Rust. It's a very thin layer on top of ureq, but much easier to mock.
struct HttpClientImpl {
    config: Config,
}

impl HttpClient for HttpClientImpl {
    fn get(&self, uri: &str) -> std::result::Result<Response<Body>, ureq::Error> {
        self.config.new_agent().get(uri).call()
    }

    fn post<'a>(&self, uri: &str, body: SendBody<'a>) -> std::result::Result<Response<Body>, ureq::Error> {
        self.config.new_agent()
            .post(uri)
            .header("content-type", "application/json")
            // not using send_json because it complicates my HttpClient trait a lot due to having to pass a generic type, and then the compiler
            // whines about it not being dyn compatible + mockall doesn't work either
            .send(body)
    }
}

impl From<ureq::Error> for Error {
    fn from(value: ureq::Error) -> Self { Ureq(value) }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self { Serde(value) }
}