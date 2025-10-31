use std::time::Duration;

pub mod broker_resolver;

#[derive(Copy, Clone, Debug)] // we'll just let it be copied rather than using Rc. It's simpler and we won't instantiate millions of these so it's fine.
pub struct Config {
    pub protocol_version: u8,
    pub max_batch_size: usize, // corresponds to Kafka's batch.size
    pub linger_duration: Duration, // corresponds to Kafka's linger.ms
}

#[derive(Debug)]
pub enum Error {
    CoordinatorApi(String),
    BrokerApi(String),
    Ureq(ureq::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn map_coordinator_error(e: coordinator::Error) -> Error {
    match e {
        coordinator::Error::Ureq(e) => Error::Ureq(e),
        coordinator::Error::Api(e) => Error::CoordinatorApi(e),
    }
}

pub fn map_broker_error(e: broker::Error) -> Error {
    match e {
        broker::Error::Ureq(e) => Error::Ureq(e),
        broker::Error::Api(e) => Error::BrokerApi(e),
    }
}