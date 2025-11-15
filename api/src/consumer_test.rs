use crate::consumer::{ConsumerRecord, ConsumerRecordStream};
use crate::mock_utils::{expect_get_topic, expect_list_brokers, set_up_broker_resolver};
use crate::{common, consumer};
use assertor::{assert_that, EqualityAssertion, OptionAssertion};
use broker::model::{BrokerApiErrorKind, PollBatchesRawResponse, PollConfig};
use client_utils::ApiError;
use coordinator::model::GetReadOffsetResponse;
use coordinator::MockClient;
use predicates::ord::eq;
use protocol::record::{serialize_batch_into, Record, RecordBatch};
use std::borrow::ToOwned;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CONSUMER_GROUP: &str = "consumer-group";
const TOPIC1: &str = "topic1";
const TOPIC2: &str = "topic2";

static POLL_CONFIG: PollConfig = PollConfig { max_batches: 2, max_wait: Duration::from_secs(2) };

#[test]
fn test_subscribe_single_topic_single_partition_single_batch_several_records() {
    let partition = 0;

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    set_up_coordinator(&mut coordinator);
    expect_get_read_offset(&mut coordinator, TOPIC1, partition, None);
    expect_poll_batches_raw(&mut broker, TOPIC1, partition, 0, PollBatchesRawResponse {
        ack_read_offset: None,
        bytes: to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])]),
    });

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
}

#[test]
fn test_subscribe_single_topic_single_partition_several_batches() {
    let partition = 0;
    let mut coordinator = coordinator::MockClient::new();

    set_up_coordinator(&mut coordinator);
    expect_get_read_offset(&mut coordinator, TOPIC1, partition, None);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC1, partition, 0, PollBatchesRawResponse {
        ack_read_offset: None,
        bytes: to_bytes(0, vec![
            new_batch(vec![new_record("hello"), new_record("world")]),
            new_batch(vec![new_record("what's up?")]),
        ]),
    });

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
    assert_that_next_is(&mut iter, TOPIC1, partition, 2, "what's up?");
}

#[test]
fn test_commit_manual() {
    let mut coordinator = coordinator::MockClient::new();
    set_up_coordinator(&mut coordinator);

    let (initial_offset_0, initial_offset_1) = (5, 9);
    expect_get_read_offset(&mut coordinator, TOPIC2, 0, Some(initial_offset_0));
    expect_get_read_offset(&mut coordinator, TOPIC2, 1, Some(initial_offset_1));

    expect_ack_read_offset(&mut coordinator, TOPIC2, 0, CONSUMER_GROUP, initial_offset_0 + 2);
    expect_ack_read_offset(&mut coordinator, TOPIC2, 0, CONSUMER_GROUP, initial_offset_0 + 4);
    expect_ack_read_offset(&mut coordinator, TOPIC2, 1, CONSUMER_GROUP, initial_offset_1 + 1);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, initial_offset_0 + 1,
        new_poll_response(to_bytes(initial_offset_0 + 1, vec![
            new_batch(vec![new_record("hello"), new_record("world")]),
            new_batch(vec![new_record("what's up?")]),
        ]))
    );

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, initial_offset_1 + 1,
        new_poll_response(to_bytes(initial_offset_1 + 1, vec![new_batch(vec![new_record("I'm good")])])));

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, initial_offset_0 + 4,
        new_poll_response(to_bytes(initial_offset_0 + 4, vec![new_batch(vec![new_record("and you?")])])));

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC2.to_owned()]).unwrap();

    iter.next();
    let record = iter.next().unwrap().unwrap();
    assert_record_equal_to(&record, TOPIC2, 0, initial_offset_0 + 2, "world");
    record.commit().unwrap();

    iter.next();

    let record = iter.next().unwrap().unwrap();
    assert_record_equal_to(&record, TOPIC2, 1, initial_offset_1 + 1, "I'm good");
    record.commit().unwrap();

    let record = iter.next().unwrap().unwrap();
    assert_record_equal_to(&record, TOPIC2, 0, initial_offset_0 + 4, "and you?");
    record.commit().unwrap();
}

#[test]
fn test_commit_auto_commit() {
    let auto_commit = true;

    let mut coordinator = coordinator::MockClient::new();
    set_up_coordinator(&mut coordinator);

    expect_get_read_offset(&mut coordinator, TOPIC2, 0, None);
    expect_get_read_offset(&mut coordinator, TOPIC2, 1, None);

    expect_ack_read_offset(&mut coordinator, TOPIC2, 0, CONSUMER_GROUP, 0);
    expect_ack_read_offset(&mut coordinator, TOPIC2, 0, CONSUMER_GROUP, 1);
    expect_ack_read_offset(&mut coordinator, TOPIC2, 1, CONSUMER_GROUP, 0);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, 0,
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])])));

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, 0,
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("I'm good")])])));

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), auto_commit);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC2.to_owned()]).unwrap();

    assert_that_next_is(&mut iter, TOPIC2, 0, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC2, 0, 1, "world");
    assert_that_next_is(&mut iter, TOPIC2, 1, 0, "I'm good");
}

#[test]
fn test_commit_second_commit_is_noop() {
    let mut coordinator = coordinator::MockClient::new();
    set_up_coordinator(&mut coordinator);

    expect_get_read_offset(&mut coordinator, TOPIC1, 0, None);
    expect_ack_read_offset(&mut coordinator, TOPIC1, 0, CONSUMER_GROUP, 0); // asserts it's only called once

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC1, 0, 0,
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello")])])));

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();

    let record = iter.next().unwrap().unwrap();
    record.commit().unwrap();
    record.commit().unwrap();
}

#[test]
fn test_subscribe_single_topic_several_partitions_several_batches_different_initial_ack_offsets() {
    let mut coordinator = coordinator::MockClient::new();
    set_up_coordinator(&mut coordinator);

    let (initial_offset_0, initial_offset_1) = (5, 9);
    expect_get_read_offset(&mut coordinator, TOPIC2, 0, Some(initial_offset_0));
    expect_get_read_offset(&mut coordinator, TOPIC2, 1, Some(initial_offset_1));

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, initial_offset_0 + 1,
        new_poll_response(to_bytes(initial_offset_0 + 1, vec![
            new_batch(vec![new_record("hello"), new_record("world")]),
            new_batch(vec![new_record("what's up?")]),
        ]))
    );

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, initial_offset_1 + 1,
        new_poll_response(to_bytes(initial_offset_1 + 1, vec![new_batch(vec![new_record("I'm good")])])));

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, initial_offset_0 + 4,
        new_poll_response(to_bytes(initial_offset_0 + 4, vec![new_batch(vec![new_record("and you?")])])));

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC2.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC2, 0, initial_offset_0 + 1, "hello");
    assert_that_next_is(&mut iter, TOPIC2, 0, initial_offset_0 + 2, "world");
    assert_that_next_is(&mut iter, TOPIC2, 0, initial_offset_0 + 3, "what's up?");
    assert_that_next_is(&mut iter, TOPIC2, 1, initial_offset_1 + 1, "I'm good");
    assert_that_next_is(&mut iter, TOPIC2, 0, initial_offset_0 + 4, "and you?");
}

#[test]
fn test_subscribe_single_topic_broker_fails_then_succeeds() {
    let partition = 0;
    let mut coordinator = coordinator::MockClient::new();
    expect_get_read_offset(&mut coordinator, TOPIC1, partition, None);
    set_up_coordinator(&mut coordinator);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    let error_msg = "Failure";
    let call_index = Rc::new(AtomicU32::new(0));

    broker.expect_poll_batches_raw()
        .with(eq(TOPIC1.to_owned()), eq(0), eq(CONSUMER_GROUP.to_owned()), eq(0), eq(POLL_CONFIG))
        .returning_st(move |_, _, _, _, _| match call_index.fetch_add(1, Ordering::Relaxed) {
            0 => Err(broker::Error::Api(ApiError { kind: BrokerApiErrorKind::Internal, message: error_msg.to_owned() })),
            _ => Ok(new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])]))),
        });

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    match iter.next() {
        Some(Err(common::Error::BrokerApi(ApiError {
            kind: BrokerApiErrorKind::Internal,
            message
        }))) => assert_that!(message).is_equal_to(error_msg.to_owned()),
        _ => unreachable!(),
    };
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
}

#[test]
fn test_subscribe_multiple_topics_one_with_no_batch_at_first() {
    let mut coordinator = coordinator::MockClient::new();
    expect_get_read_offset(&mut coordinator, TOPIC1, 0, None);
    expect_get_read_offset(&mut coordinator, TOPIC2, 0, None);
    expect_get_read_offset(&mut coordinator, TOPIC2, 1, None);
    set_up_coordinator(&mut coordinator);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw_multiple(&mut broker, TOPIC1, 0, 0, vec![
        new_empty_poll_response(0),
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("and you?")])])),
    ]);

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, 0,
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])])));

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, 2,
        new_poll_response(to_bytes(2, vec![new_batch(vec![new_record("Well, thanks for asking")])])));

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, 0,
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("I'm good")])])));

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver), false);

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned(), TOPIC2.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC2, 0, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC2, 0, 1, "world");
    assert_that_next_is(&mut iter, TOPIC2, 1, 0, "I'm good");
    assert_that_next_is(&mut iter, TOPIC1, 0, 0, "and you?");
    assert_that_next_is(&mut iter, TOPIC2, 0, 2, "Well, thanks for asking");
}

fn set_up_coordinator(coordinator: &mut coordinator::MockClient) {
    expect_get_topic(coordinator, TOPIC1, 1);
    expect_get_topic(coordinator, TOPIC2, 2);
    expect_list_brokers(coordinator);
}

fn expect_poll_batches_raw_multiple(broker: &mut broker::MockClient, topic: &str, partition: u32, offset: u64, mut expected: Vec<PollBatchesRawResponse>) {
    broker.expect_poll_batches_raw()
        .with(eq(topic.to_owned()), eq(partition), eq(CONSUMER_GROUP.to_owned()), eq(offset), eq(POLL_CONFIG))
        .returning(move |_, _, _, _, _| Ok(expected.remove(0)));
}

fn expect_poll_batches_raw(broker: &mut broker::MockClient, topic: &str, partition: u32, offset: u64, expected: PollBatchesRawResponse) {
    broker.expect_poll_batches_raw()
        .with(eq(topic.to_owned()), eq(partition), eq(CONSUMER_GROUP.to_owned()), eq(offset), eq(POLL_CONFIG))
        .return_once(move |_, _, _, _, _| Ok(expected));
}

fn expect_get_read_offset(coordinator: &mut coordinator::MockClient, topic: &str, partition: u32, offset: Option<u64>) {
    coordinator.expect_get_read_offset()
        .with(eq(topic.to_owned()), eq(partition), eq(CONSUMER_GROUP.to_owned()))
        .returning(move |_, _, _| Ok(GetReadOffsetResponse { offset }));
}

fn expect_ack_read_offset(coordinator: &mut MockClient, topic: &'static str, partition: u32, consumer_group: &'static str, offset: u64) {
    coordinator.expect_ack_read_offset()
        .times(1)
        .with(eq(topic), eq(partition), eq(consumer_group), eq(offset))
        .return_once(|_, _, _, _| Ok(()));
}

fn assert_that_next_is(iter: &mut ConsumerRecordStream, topic: &str, partition: u32, offset: u64, value: &str) {
    let actual = iter.next().unwrap().unwrap();
    // Can't use struct equality because of the OffsetStateManager inside the records. It's annoying, but the safety brought by this design (the user
    // cannot make mistakes while calling the commit method) is worth this slight pain.
    assert_record_equal_to(&actual, topic, partition, offset, value);
}

fn assert_record_equal_to(actual: &ConsumerRecord, topic: &str, partition: u32, offset: u64, value: &str) {
    assert_that!(actual.topic).is_equal_to(topic.to_owned());
    assert_that!(actual.partition).is_equal_to(partition);
    assert_that!(actual.offset).is_equal_to(offset);
    assert_that!(actual.record).is_equal_to(new_record(value));
}

fn new_empty_poll_response(offset: u64) -> PollBatchesRawResponse {
    new_poll_response(to_bytes(offset, vec![new_batch(vec![])]))
}

fn new_poll_response(bytes: Vec<u8>) -> PollBatchesRawResponse {
    PollBatchesRawResponse { ack_read_offset: None, bytes }
}

fn new_batch(records: Vec<Record>) -> RecordBatch {
    RecordBatch { protocol_version: 1, base_timestamp: 123, records }
}

fn new_record(value: &str) -> Record {
    Record { timestamp: 1234, key: None, value: value.to_owned(), headers: vec![] }
}

fn to_bytes(offset: u64, batches: Vec<RecordBatch>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut current_offset = offset;

    for batch in batches {
        bytes.extend(current_offset.to_le_bytes());
        current_offset += batch.records.len() as u64;

        serialize_batch_into(batch, &mut bytes);
    }
    bytes
}
