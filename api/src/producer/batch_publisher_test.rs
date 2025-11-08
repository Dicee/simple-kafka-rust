use crate::mock_utils::{eq_serialized_batch, expect_list_brokers, expect_publish_raw, set_up_broker_resolver};
use crate::producer::batch_publisher::BatchPublisher;
use broker::model::{BrokerApiErrorKind, TopicPartition};
use mockall::predicate;
use predicate::eq;
use protocol::record::{Record, RecordBatch};
use std::sync::Arc;
use std::vec;
use client_utils::ApiError;

const TOPIC1: &'static str = "topic1";
const TOPIC2: &'static str = "topic2";

const P1: u32 = 89;
const P2: u32 = 77;

#[test]
fn test_publish_async() {
    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    
    let batch1 = new_single_record_batch("value1");
    let batch2 = new_single_record_batch("value2");

    expect_publish_raw(&mut broker, TOPIC1, P1, batch1.clone());
    expect_publish_raw(&mut broker, TOPIC2, P2, batch2.clone());

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);

    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), Arc::clone(&broker));
    let publisher = BatchPublisher::new(broker_resolver);

    publisher.publish_async(TopicPartition { topic: TOPIC1.to_owned(), partition: P1 }, batch1);
    publisher.publish_async(TopicPartition { topic: TOPIC2.to_owned(), partition: P2 }, batch2);
    publisher.shutdown().unwrap();
}

#[test]
fn test_publish_async_publish_loop_survives_single_broker_failure() {
    let mut coordinator = coordinator::MockClient::new();
    let mut broker_mock = broker::MockClient::new();
    
    expect_list_brokers(&mut coordinator);

    let batch1 = new_single_record_batch("value1");
    let batch2 = new_single_record_batch("value2");
    
    broker_mock
        .expect_publish_raw()
        .with(eq(TOPIC1.to_owned()), eq(P1), eq_serialized_batch(batch1.clone()), eq(1))
        .times(1)
        .returning(|_, _, _, _| Err(broker::Error::Api(ApiError { kind: BrokerApiErrorKind::Internal, message: String::from("Ain't my fault") })));
    
    expect_publish_raw(&mut broker_mock, TOPIC2, P2, batch2.clone());
    
    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker_mock);

    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), Arc::clone(&broker));
    let publisher = BatchPublisher::new(broker_resolver);

    publisher.publish_async(TopicPartition { topic: TOPIC1.to_owned(), partition: P1 }, batch1);
    publisher.publish_async(TopicPartition { topic: TOPIC2.to_owned(), partition: P2 }, batch2);
    publisher.shutdown().unwrap();
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
