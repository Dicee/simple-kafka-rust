use crate::consumer::{ConsumerRecord, ConsumerRecordIter};
use crate::mock_utils::{expect_list_brokers, set_up_broker_resolver};
use crate::{common, consumer};
use assertor::{assert_that, EqualityAssertion, OptionAssertion};
use broker::model::{PollBatchesRawResponse, PollConfig};
use coordinator::model::{GetTopicRequest, GetTopicResponse};
use predicates::ord::eq;
use protocol::record::{serialize_batch_into, Record, RecordBatch};
use std::borrow::ToOwned;
use std::cell::RefCell;
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
    let coordinator = set_up_coordinator();
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC1, partition, vec![PollBatchesRawResponse {
        ack_read_offset: None,
        bytes: to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])]),
    }]);

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver));

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
}

#[test]
fn test_subscribe_single_topic_single_partition_several_batches() {
    let partition = 0;
    let coordinator = set_up_coordinator();
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC1, partition, vec![PollBatchesRawResponse {
        ack_read_offset: None,
        bytes: to_bytes(0, vec![
            new_batch(vec![new_record("hello"), new_record("world")]),
            new_batch(vec![new_record("what's up?")]),
        ]),
    }]);

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver));

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
    assert_that_next_is(&mut iter, TOPIC1, partition, 2, "what's up?");
}

#[test]
fn test_subscribe_single_topic_several_partitions_several_batches() {
    let coordinator = set_up_coordinator();
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, vec![
        new_poll_response(to_bytes(0, vec![
            new_batch(vec![new_record("hello"), new_record("world")]),
            new_batch(vec![new_record("what's up?")]),
        ])),
        new_poll_response(to_bytes(4, vec![
            new_batch(vec![new_record("and you?")]),
        ])),
    ]);

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, vec![new_poll_response(to_bytes(3, vec![
        new_batch(vec![new_record("I'm good")]),
    ]))]);

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver));

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC2.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC2, 0, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC2, 0, 1, "world");
    assert_that_next_is(&mut iter, TOPIC2, 0, 2, "what's up?");
    assert_that_next_is(&mut iter, TOPIC2, 1, 3, "I'm good");
    assert_that_next_is(&mut iter, TOPIC2, 0, 4, "and you?");
}

#[test]
fn test_subscribe_single_topic_broker_fails_then_succeeds() {
    let partition = 0;
    let coordinator = set_up_coordinator();
    let mut broker = broker::MockClient::new();

    let error_msg = "Failure";
    let call_index = Rc::new(AtomicU32::new(0));

    broker.expect_poll_batches_raw()
        .with(eq(TOPIC1.to_owned()), eq(0), eq(CONSUMER_GROUP.to_owned()), eq(POLL_CONFIG))
        .returning_st(move |_, _, _, _| match call_index.fetch_add(1, Ordering::Relaxed) {
            0 => Err(broker::Error::Api(error_msg.to_owned())),
            _ => Ok(new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])]))),
        });

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver));

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned()]).unwrap();
    match iter.next() {
        Some(Err(common::Error::BrokerApi(msg))) => assert_that!(msg).is_equal_to(error_msg.to_owned()),
        _ => unreachable!(),
    };
    assert_that_next_is(&mut iter, TOPIC1, partition, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC1, partition, 1, "world");
}

#[test]
fn test_subscribe_multiple_topics_one_with_no_batch_at_first() {
    let coordinator = set_up_coordinator();
    let mut broker = broker::MockClient::new();

    expect_poll_batches_raw(&mut broker, TOPIC1, 0, vec![
        new_empty_poll_response(0),
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("and you?")])])),
    ]);

    expect_poll_batches_raw(&mut broker, TOPIC2, 0, vec![
        new_poll_response(to_bytes(0, vec![new_batch(vec![new_record("hello"), new_record("world")])])),
        new_poll_response(to_bytes(2, vec![new_batch(vec![new_record("Well, thanks for asking")])])),
    ]);

    expect_poll_batches_raw(&mut broker, TOPIC2, 1, vec![
        new_poll_response(to_bytes(2, vec![new_batch(vec![new_record("I'm good")])])),
    ]);

    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let consumer = consumer::Client::new_for_testing(coordinator, POLL_CONFIG, Arc::new(broker_resolver));

    let mut iter = consumer.subscribe(CONSUMER_GROUP.to_owned(), vec![TOPIC1.to_owned(), TOPIC2.to_owned()]).unwrap();
    assert_that_next_is(&mut iter, TOPIC2, 0, 0, "hello");
    assert_that_next_is(&mut iter, TOPIC2, 0, 1, "world");
    assert_that_next_is(&mut iter, TOPIC2, 1, 2, "I'm good");
    assert_that_next_is(&mut iter, TOPIC1, 0, 0, "and you?");
    assert_that_next_is(&mut iter, TOPIC2, 0, 2, "Well, thanks for asking");
}

fn set_up_coordinator() -> Arc<dyn coordinator::Client> {
    let mut coordinator = coordinator::MockClient::new();

    coordinator.expect_get_topic()
        .with(eq(GetTopicRequest { name: TOPIC1.to_owned() }))
        .returning(|_| Ok(GetTopicResponse { name: TOPIC1.to_owned(), partition_count: 1 }));

    coordinator.expect_get_topic()
        .with(eq(GetTopicRequest { name: TOPIC2.to_owned() }))
        .returning(|_| Ok(GetTopicResponse { name: TOPIC2.to_owned(), partition_count: 2 }));

    expect_list_brokers(&mut coordinator);

    Arc::new(coordinator)
}

fn expect_poll_batches_raw(broker: &mut broker::MockClient, topic: &str, partition: u32, expected: Vec<PollBatchesRawResponse>) {
    let expected = RefCell::new(expected);
    broker.expect_poll_batches_raw()
        .with(eq(topic.to_owned()), eq(partition), eq(CONSUMER_GROUP.to_owned()), eq(POLL_CONFIG))
        .returning(move |_, _, _, _| Ok(expected.borrow_mut().remove(0)));
}

fn assert_that_next_is(iter: &mut ConsumerRecordIter, topic: &str, partition: u32, offset: u64, value: &str) {
    assert_that!(iter.next().unwrap().ok()).has_value(ConsumerRecord { topic: topic.to_owned(), partition, record: new_record(value), offset });
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
