use crate::common::broker_resolver::{BrokerResolver, MockBrokerClientFactory};
use crate::producer::batch_publisher::BatchPublisher;
use broker::model::{PublishResponse, TopicPartition};
use coordinator::model::{HostAndPort, ListBrokersResponse};
use mockall::predicate;
use predicate::eq;
use protocol::record::{read_next_batch, Record, RecordBatch};
use std::io::Cursor;
use std::sync::Arc;
use std::vec;

const TOPIC1: &'static str = "topic1";
const TOPIC2: &'static str = "topic2";

const P1: u32 = 89;
const P2: u32 = 77;

#[test]
fn test_publish_async() {
    let mut coordinator = coordinator::MockClient::new();
    let mut broker_mock = broker::MockClient::new();

    let batch1 = new_single_record_batch("value1");
    let batch2 = new_single_record_batch("value2");

    expect_publish_raw(&mut broker_mock, TOPIC1, P1, batch1.clone());
    expect_publish_raw(&mut broker_mock, TOPIC2, P2, batch2.clone());

    mock_list_brokers(&mut coordinator);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker_mock);

    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), Arc::clone(&broker));
    let publisher = BatchPublisher::new(Arc::clone(&coordinator), broker_resolver);

    publisher.publish_async(TopicPartition { topic: TOPIC1.to_owned(), partition: P1 }, batch1);
    publisher.publish_async(TopicPartition { topic: TOPIC2.to_owned(), partition: P2 }, batch2);
    publisher.shutdown().unwrap();
}

#[test]
fn test_publish_async_publish_loop_survives_single_broker_failure() {
    let mut coordinator = coordinator::MockClient::new();
    let mut broker_mock = broker::MockClient::new();

    let batch1 = new_single_record_batch("value1");
    let batch2 = new_single_record_batch("value2");

    broker_mock
        .expect_publish_raw()
        .with(eq(TOPIC1.to_owned()), eq(P1), eq_serialized_batch(batch1.clone()), eq(1))
        .times(1)
        .returning(|_, _, _, _| Err(broker::Error::Api(String::from("Ain't my fault"))));
    expect_publish_raw(&mut broker_mock, TOPIC2, P2, batch2.clone());

    mock_list_brokers(&mut coordinator);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker_mock);

    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), Arc::clone(&broker));
    let publisher = BatchPublisher::new(Arc::clone(&coordinator), broker_resolver);

    publisher.publish_async(TopicPartition { topic: TOPIC1.to_owned(), partition: P1 }, batch1);
    publisher.publish_async(TopicPartition { topic: TOPIC2.to_owned(), partition: P2 }, batch2);
    publisher.shutdown().unwrap();
}

// in Java I would mock the resolver entirely, but in Rust mocking is really not that convenient, so since it's a relatively simple struct I'll only
// partially mock it
fn set_up_broker_resolver(coordinator: Arc<dyn coordinator::Client>, broker: Arc<dyn broker::Client>) -> BrokerResolver {
    let mut broker_client_factory = MockBrokerClientFactory::new();
    broker_client_factory
        .expect_create()
        .times(1)
        .return_const(Arc::clone(&broker));

    BrokerResolver::new_with_factory(Arc::clone(&coordinator), &broker_client_factory).unwrap()
}

fn mock_list_brokers(coordinator: &mut coordinator::MockClient) {
    coordinator
        .expect_list_brokers()
        .returning(|| Ok(ListBrokersResponse { brokers: vec![HostAndPort::new(String::from("localhost"), 8000)] }));
}

fn expect_publish_raw(broker_mock: &mut broker::MockClient, topic: &str, partition: u32, batch: RecordBatch) {
    broker_mock
        .expect_publish_raw()
        .with(eq(topic.to_owned()), eq(partition), eq_serialized_batch(batch), eq(1))
        .times(1)
        .returning(|_, _, _, _| Ok(PublishResponse { base_offset: 1 }));
}

fn eq_serialized_batch(batch: RecordBatch) -> impl predicates::Predicate<Vec<u8>> {
    predicate::function(move |bytes| read_next_batch(&mut Cursor::new(bytes)).unwrap() == batch)
}

fn new_single_record_batch(value: &str) -> RecordBatch {
    RecordBatch {
        base_timestamp: 0,
        protocol_version: 0,
        records: vec![Record {
            timestamp: 2,
            key: None,
            value: String::from(value),
            headers: vec![],
        }],
    }
}
