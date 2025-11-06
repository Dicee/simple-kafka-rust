use crate::Error::{Api, Ureq};
use mockall::automock;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::time::Instant;
use ureq::http::Response;
use ureq::{self, Agent, Body, config::Config, SendBody};

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
    debug: bool,
}

pub type Result<T> = std::result::Result<T, Error>;

impl ApiClient {
    pub fn new(domain: String, debug: bool, use_tls: bool) -> Self {
        let config = Agent::config_builder()
            // I don't like the default handling because it doesn't return the body, which contains error messages
            .http_status_as_error(false)
            .build();

        let http_client = Box::new(HttpClientImpl { config });
        Self::new_with_http_client(domain, debug, use_tls, http_client)
    }

    // purely for testing, exposed for users to be able to mock network calls as well
    pub fn new_with_http_client(domain: String, debug: bool, use_tls: bool, http_client: Box<dyn HttpClient>) -> Self {
        let protocol = if use_tls { "https" } else { "http" };
        Self {
            url_base: format!("{protocol}://{domain}"),
            http_client,
            debug,
        }
    }

    pub fn get<Res>(&self, api: &str) -> Result<Res> where for<'de> Res: Deserialize<'de> {
        let uri = format!("{}{api}", self.url_base);
        self.with_latency_logging(&format!("GET {uri}"), || Self::parse_response(self.http_client.get(&uri)?))
    }

    pub fn post<Req: Serialize>(&self, api: &str, request: Req) -> Result<Response<Body>> {
        let uri = format!("{}{api}", self.url_base);
        self.with_latency_logging(&format!("POST {uri}"), || {
            let mut response = self.http_client.post(&uri, SendBody::from_json(&request)?)?;
            if !response.status().is_success() {
                Err(Api(response.body_mut().read_to_string()?))
            } else {
                Ok(response)
            }
        })
    }

    pub fn post_raw_and_parse<Res>(&self, api: &str, bytes: Vec<u8>) -> Result<Res>
    where for<'de> Res: Deserialize<'de> {
        let uri = format!("{}{api}", self.url_base);
        self.with_latency_logging(&format!("POST {uri}"), || {
            let mut cursor = Cursor::new(&bytes);
            let body = SendBody::from_reader(&mut cursor);
            Self::parse_response(self.http_client.post(&uri, body)?)
        })
    }

    pub fn post_and_parse<Req: Serialize, Res>(&self, api: &str, request: Req) -> Result<Res>
    where for<'de> Res: Deserialize<'de> {
        let uri = format!("{}{api}", self.url_base);
        self.with_latency_logging(&format!("POST {uri}"), || {
            Self::parse_response(self.http_client.post(&uri, SendBody::from_json(&request)?)?)
        })
    }

    fn parse_response<Res>(mut response: Response<Body>) -> Result<Res> where for<'de> Res: Deserialize<'de> {
        if !response.status().is_success() {
            Err(Api(response.body_mut().read_to_string()?))
        } else {
            Ok(response.body_mut().read_json()?)
        }
    }

    fn with_latency_logging<T>(&self, description: &str, f: impl Fn() -> T) -> T {
        let start = Instant::now();
        let result = f();
        if self.debug { println!("{} completed in {:?}", description, start.elapsed()); }
        result
    }

    pub fn get_required_header<'a>(header_key: &str, response: &'a Response<Body>) -> Result<&'a str> {
        match Self::get_optional_header(header_key, response)? {
            None => Err(Api(format!("Missing {header_key} header"))),
            Some(header) => Ok(header)
        }
    }

    pub fn get_optional_header<'a>(header_key: &str, response: &'a Response<Body>) -> Result<Option<&'a str>> {
        Ok(match response.headers().get(header_key) {
            None => None,
            Some(header) => Some(header.to_str()
                .map_err(|e| Api(format!("Failed to parse {header_key} to string due to {e:?}")))?)
        })
    }
}

// purely for testing, exposed for users to be able to mock network calls as well
#[automock]
pub trait HttpClient : Send + Sync {
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