use crate::common::broker_resolver::{BrokerClientFactory, BrokerResolver};
use assertor::{assert_that, EqualityAssertion, IteratorAssertion};
use broker::model::{PollBatchesRawResponse, PollConfig, PublishResponse};
use coordinator::model::{HostAndPort, ListBrokersResponse};
use protocol::record::RecordBatch;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
#[should_panic(expected = "No brokers are registered to the coordinator")]
fn test_new_no_broker() {
    let mut coordinator = coordinator::MockClient::new();
    coordinator.expect_list_brokers()
        .times(1)
        .returning(move || Ok(ListBrokersResponse { brokers: vec![] }));

    let factory = DummyBrokerClientFactory::new();
    BrokerResolver::new_with_factory(Arc::new(coordinator), &factory).unwrap();
}

#[test]
fn test_new_coordinator_failure() {
    let mut coordinator = coordinator::MockClient::new();
    coordinator.expect_list_brokers()
        .times(1)
        .returning(move || Err(coordinator::Error::Api("Oh no!".to_owned())));

    let factory = DummyBrokerClientFactory::new();
    match BrokerResolver::new_with_factory(Arc::new(coordinator), &factory) {
        Err(crate::common::Error::CoordinatorApi(msg)) => assert_that!(msg).is_equal_to("Oh no!".to_owned()),
        _ => unreachable!(),
    }
}

#[test]
fn test_client_for() {
    let host0: HostAndPort = new_host("localhost", 8080);
    let host1: HostAndPort = new_host("localhost", 8081);
    let host2: HostAndPort = new_host("localhost", 8082);
    let brokers = vec![host0.clone(), host1.clone(), host2.clone()];

    let mut coordinator = coordinator::MockClient::new();
    coordinator.expect_list_brokers()
        .times(1)
        .returning(move || Ok(ListBrokersResponse { brokers: brokers.clone() }));

    let factory = DummyBrokerClientFactory::new();
    let broker_resolver = BrokerResolver::new_with_factory(Arc::new(coordinator), &factory).unwrap();

    select_client_for(&broker_resolver, 10);
    select_client_for(&broker_resolver, 9);
    select_client_for(&broker_resolver, 0);
    select_client_for(&broker_resolver, 187);
    select_client_for(&broker_resolver, 56);
    select_client_for(&broker_resolver, 187);

    // I couldn't find a nicer way to get a matching type between the iterator acquired through the locked map and an iterator
    // created from owned data, all the while keeping the borrow checker happy
    let calls = factory.calls.lock().unwrap();
    let key1 = (host1.clone(), 10);
    let key2 = (host0.clone(), 9);
    let key3 = (host0.clone(), 0);
    let key4 = (host1.clone(), 187);
    let key5 = (host2.clone(), 56);

    assert_that!(calls.iter()).contains_exactly(vec![
        (&key1, &1),
        (&key2, &1),
        (&key3, &1),
        (&key4, &2),
        (&key5, &1),
    ].into_iter());
}

fn select_client_for(broker_resolver: &BrokerResolver, partition: u32) {
    let topic = String::from("topic");
    let consumer_group = String::from("consumer");
    let poll_config = PollConfig { max_batches: 1, max_wait: Duration::from_secs(1) };

    broker_resolver.client_for(partition)
        .poll_batches_raw(topic, partition, consumer_group, poll_config)
        .unwrap();
}

struct DummyBrokerClientFactory {
    calls: Arc<Mutex<HashMap<(HostAndPort, u32), u32>>>,
}

impl DummyBrokerClientFactory {
    fn new() -> Self { Self { calls: Arc::new(Mutex::new(HashMap::new())) } }
}

impl BrokerClientFactory for DummyBrokerClientFactory {
    fn create(&self, host: &HostAndPort) -> Arc<dyn broker::Client> {
        Arc::new(DummyBrokerClient {
            host: host.clone(),
            calls: Arc::clone(&self.calls),
        })
    }
}

struct DummyBrokerClient {
    host: HostAndPort,
    calls: Arc<Mutex<HashMap<(HostAndPort, u32), u32>>>,
}

impl broker::Client for DummyBrokerClient {
    fn poll_batches_raw(&self, _: String, partition: u32, _: String, _: PollConfig) -> broker::Result<PollBatchesRawResponse> {
        let mut calls = self.calls.lock().unwrap();
        calls.entry((self.host.clone(), partition)).and_modify(|c| *c += 1).or_insert_with(|| 1);
        Ok(PollBatchesRawResponse { ack_read_offset: Some(17), bytes: vec![]  })
    }

    fn publish(&self, _: &str, _: u32, _: RecordBatch) -> broker::Result<PublishResponse> { unimplemented!() }
    fn publish_raw(&self, _: &str, _: u32, _: Vec<u8>, _: u32) -> broker::Result<PublishResponse> { unimplemented!(); }
}

fn new_host(host: &str, port: u16) -> HostAndPort {
    HostAndPort::new(String::from(host), port)
}