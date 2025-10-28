use mockall::automock;
use serde::{Deserialize, Serialize};
use ureq;
use ureq::config::Config;
use ureq::http::Response;
use ureq::{Agent, Body, SendBody};
use crate::Error::{Api, Ureq};

#[cfg(test)]
#[path = "api_client_test.rs"]
mod api_client_test;

#[derive(Debug)]
pub enum Error {
    Ureq(ureq::Error),
    Api(String),
}

pub struct ApiClient {
    url_base: String,
    http_client: Box<dyn HttpClient>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl ApiClient {
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

    pub fn get<Res>(&self, api: &str) -> Result<Res> where for<'de> Res: Deserialize<'de> {
        let uri = format!("{}{api}", self.url_base);
        let mut response = self.http_client.get(&uri)?;

        if !response.status().is_success() {
            Err(Api(response.body_mut().read_to_string()?))
        } else {
            Ok(response.body_mut().read_json()?)
        }
    }

    pub fn post<Req: Serialize>(&self, api: &str, request: Req) -> Result<()> {
        let uri = format!("{}{api}", self.url_base);
        let mut response = self.http_client.post(&uri, SendBody::from_json(&request)?)?;

        if !response.status().is_success() {
            Err(Api(response.body_mut().read_to_string()?))
        } else {
            Ok(())
        }
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