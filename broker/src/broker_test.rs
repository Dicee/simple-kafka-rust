use crate::broker::Error::Coordinator;
use crate::broker::{Broker, RecordBatchWithOffset};
use crate::persistence::LogManager;
use assertor::{assert_that, EqualityAssertion, OptionAssertion, ResultAssertion};
use coordinator::client::{Client as CoordinatorClient, Client, MockClient as MockCoordinatorClient};
use coordinator::mock::DummyCoordinatorClient;
use coordinator::model::*;
use file_test_utils::TempTestDir;
use protocol::record::{serialize_batch_into, Header, Record, RecordBatch};
use std::io::ErrorKind;
use std::sync::Arc;

const TOPIC: &str = "chess-news";
const PARTITION: u32 = 116;
const CONSUMER_GROUP: &str = "danya-fans";

#[test]
fn test_serialization_round_trip_from_first_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp_1 = 0;
    let base_timestamp_2 = 17;

    let batch_1 = new_record_batch(base_timestamp_1, vec![
        new_record(base_timestamp_1, 1),
        new_record(base_timestamp_1, 2),
    ]);
    let batch_2 = new_record_batch(base_timestamp_2, vec![new_record(base_timestamp_2, 1)]);

    assert_write_offset_is(coordinator_client.as_ref(), None);
    broker.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();
    assert_write_offset_is(coordinator_client.as_ref(), Some(1));

    broker.publish(TOPIC, PARTITION, batch_2.clone()).unwrap();
    assert_write_offset_is(coordinator_client.as_ref(), Some(2));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()).unwrap())
        .has_value(RecordBatchWithOffset { base_offset: 0, batch: batch_1 });

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()).unwrap())
        .has_value(RecordBatchWithOffset { base_offset: 2, batch: batch_2 });

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()).unwrap()).is_none();
}

#[test]
fn test_publish_write_offset_not_committed_if_failure_coordinator_failure() {
    let temp_dir = TempTestDir::create();

    let mut coordinator_client = MockCoordinatorClient::new();
    coordinator_client.expect_get_write_offset()
        .returning(|_| Err(coordinator::client::Error::Api(String::from("Oopsy"))));

    coordinator_client.expect_increment_write_offset().never();

    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(coordinator_client);

    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp = 15;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);
    match broker.publish(TOPIC, PARTITION, batch) {
        Err(Coordinator(msg)) => assert_that!(msg).is_equal_to(String::from("Api(\"Oopsy\")")),
        _ => unreachable!(),
    }
}

#[test]
fn test_read_next_batch_initialize_from_exact_base_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp_1 = 0;
    let base_timestamp_2 = 17;

    let batch_1 = new_record_batch(base_timestamp_1, vec![
        new_record(base_timestamp_1, 1),
        new_record(base_timestamp_1, 2),
        new_record(base_timestamp_1, 3),

    ]);
    let batch_2 = new_record_batch(base_timestamp_2, vec![
        new_record(base_timestamp_2, 1),
        new_record(base_timestamp_2, 2),
    ]);
    broker.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch_2.clone()).unwrap();

    ack_read_offset(coordinator_client, 2);

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 3, batch: batch_2 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

#[test]
fn test_read_next_batch_initialize_with_offset_within_a_batch() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp_1 = 0;
    let base_timestamp_2 = 17;

    let batch_1 = new_record_batch(base_timestamp_1, vec![
        new_record(base_timestamp_1, 1),
        new_record(base_timestamp_1, 2),

    ]);
    let batch_2 = new_record_batch(base_timestamp_2, vec![
        new_record(base_timestamp_2, 1),
        new_record(base_timestamp_2, 2),
        new_record(base_timestamp_2, 3),
    ]);
    broker.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch_2.clone()).unwrap();

    ack_read_offset(coordinator_client, 2);

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch_2 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

#[test]
fn test_read_next_batch_initialize_with_offset_at_the_end_of_a_batch() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp_1 = 0;
    let base_timestamp_2 = 17;
    let base_timestamp_3 = 51;

    let batch_1 = new_record_batch(base_timestamp_1, vec![new_record(base_timestamp_1, 1)]);
    let batch_2 = new_record_batch(base_timestamp_2, vec![
        new_record(base_timestamp_2, 1),
        new_record(base_timestamp_2, 2),
        new_record(base_timestamp_2, 3),
    ]);
    let batch_3 = new_record_batch(base_timestamp_2, vec![
        new_record(base_timestamp_3, 1),
        new_record(base_timestamp_3, 2),
    ]);

    broker.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch_2.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch_3.clone()).unwrap();

    ack_read_offset(coordinator_client, 2);

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 1, batch: batch_2 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 4, batch: batch_3 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

#[test]
fn test_read_next_batch_initialize_before_any_data_is_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);

    let base_timestamp = 14;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);
    broker.publish(TOPIC, PARTITION, batch.clone()).unwrap();

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

#[test]
fn test_read_next_batch_initialize_while_first_batch_is_being_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp = 14;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);

    let mut bytes = Vec::new();
    bytes.extend(0u64.to_le_bytes());
    serialize_batch_into(batch.clone(), &mut bytes);

    // simulate an unclean write, with only part of the data flushed to disk
    let mut log_manager = LogManager::new(temp_dir.path_as_str().to_owned());
    log_manager.write_and_commit(TOPIC, PARTITION, 0, &bytes[0..13]).unwrap();

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);

    log_manager.write_and_commit(TOPIC, PARTITION, 0, &bytes[13..]).unwrap();
    coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        inc: 1,
    }).unwrap();

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

#[test]
fn test_read_next_batch_while_a_batch_is_being_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn CoordinatorClient> = Arc::new(DummyCoordinatorClient::new());
    let mut broker = Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    };

    let base_timestamp_1 = 14;
    let base_timestamp_2 = 45;

    let batch_1 = new_record_batch(base_timestamp_1, vec![
        new_record(base_timestamp_1, 1),
        new_record(base_timestamp_1, 2),
    ]);
    let batch_2 = new_record_batch(base_timestamp_2, vec![
        new_record(base_timestamp_2, 1),
        new_record(base_timestamp_2, 2),
    ]);

    broker.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();

    let mut bytes = Vec::new();
    bytes.extend(2u64.to_le_bytes());
    serialize_batch_into(batch_2.clone(), &mut bytes);

    // simulate an unclean write, with only part of the data flushed to disk
    let mut log_manager = LogManager::new(temp_dir.path_as_str().to_owned());
    log_manager.write_and_commit(TOPIC, PARTITION, 0, &bytes[0..13]).unwrap();

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch: batch_1 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);

    log_manager.write_and_commit(TOPIC, PARTITION, 0, &bytes[13..]).unwrap();
    coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        inc: 1,
    }).unwrap();

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch_2 }));

    assert_that!(broker.read_next_batch(TOPIC, PARTITION, CONSUMER_GROUP.to_owned()))
        .has_ok(None);
}

fn ack_read_offset(coordinator_client: Arc<dyn Client>, offset: u64) {
    coordinator_client.ack_read_offset(AckReadOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        consumer_group: CONSUMER_GROUP.to_owned(),
        offset,
    }).unwrap();
}

fn assert_write_offset_is(dummy_coordinator: &dyn Client, offset: Option<u64>) {
    assert_that!(dummy_coordinator.get_write_offset(GetWriteOffsetRequest { topic: TOPIC.to_owned(), partition: PARTITION }))
        .has_ok(GetWriteOffsetResponse { offset });
}

fn new_record_batch(base_timestamp: u64, records: Vec<Record>) -> RecordBatch {
    RecordBatch { protocol_version: 14, base_timestamp, records }
}

fn new_record(base_timestamp: u64, index: u64) -> Record {
    Record {
        key: None,
        value: format!("record-{index}"),
        headers: vec![Header {
            key: format!("header-{index}"),
            value: format!("value-{index}"),
        }],
        timestamp: base_timestamp + index,
    }
}