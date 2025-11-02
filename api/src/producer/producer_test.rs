use std::ops::Mul;
use crate::common::Config;
use crate::producer;
use crate::producer::mock_utils::{expect_get_topic, expect_list_brokers, expect_publish_raw_ignoring_timestamps, set_up_broker_resolver};
use protocol::record::{Record, RecordBatch};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use ntest_timeout::timeout;

///! Please note that these tests aren't ideal. In Java or Kotlin I would have mocked all the dependencies of producer::Client to ease testing and
///  allow going in depth, however with Rust and mockall I feel mocking is pretty heavyweight (I don't like introducing trait objects everywhere,
///  they're not super nice to work with), therefore I decided to test end-to-end with the exception of the clients that make network calls, of course.
///  As a consolation, I think it's nice to test concurrent code end-to-end rather than with mocks as it could highlight issues that couldn't be
///  unveiled if some of the components were mocked.

const PROTOCOL_VERSION: u8 = 14;

const TOPIC1: &'static str = "topic1";
const TOPIC2: &'static str = "topic2";

const PARTITION_COUNT1: u32 = 128;
const PARTITION_COUNT2: u32 = 64;

const P1: u32 = 80; // happens to be the hash of "key" modulo PARTITION_COUNT1
const P2: u32 = 16; // happens to be the hash of "key" modulo PARTITION_COUNT2

#[test]
fn test_send_record_single_topic_partition_flushed_by_batch_size() {
    let max_batch_size = 3;
    // make sure the linger duration is so high that there's no way it happens in this test
    let config = new_config(max_batch_size, Duration::from_secs(1000));

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);

    let expected_batch1 = new_record_batch(vec![new_record(1), new_record(2), new_record(3)]);
    let expected_batch2 = new_record_batch(vec![new_record(4), new_record(5), new_record(6)]);

    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch1);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch2);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(2)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(3)).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(4)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(5)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(6)).unwrap();

    producer.shutdown().unwrap();
}

#[test]
fn test_send_record_single_topic_partition_flushed_by_linger_duration() {
    let linger_duration = Duration::from_millis(100);
    // make sure the max batch size is so high that there's no way it happens in this test
    let config = new_config(1000, linger_duration);

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);

    let expected_batch1 = new_record_batch(vec![new_record(1), new_record(2)]);
    let expected_batch2 = new_record_batch(vec![new_record(3)]);
    let expected_batch3 = new_record_batch(vec![new_record(4), new_record(5)]);

    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch1);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch2);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch3);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(2)).unwrap();

    thread::sleep(linger_duration.mul(2)); // ensure a flush will happen

    producer.send_record(TOPIC1.to_owned(), new_record(3)).unwrap();
    thread::sleep(linger_duration.mul(2));

    producer.send_record(TOPIC1.to_owned(), new_record(4)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(5)).unwrap();
    thread::sleep(linger_duration.mul(2));

    producer.shutdown().unwrap();
}

#[test]
fn test_send_record_single_topic_partition_flushed_for_mixed_reasons() {
    let max_batch_size = 2;
    let linger_duration = Duration::from_millis(100);
    let config = new_config(max_batch_size, linger_duration);

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);

    let expected_batch1 = new_record_batch(vec![new_record(1), new_record(2)]); // flushed by size
    let expected_batch2 = new_record_batch(vec![new_record(3)]); // flushed by linger duration
    let expected_batch3 = new_record_batch(vec![new_record(4)]); // flushed by linger duration
    let expected_batch4 = new_record_batch(vec![new_record(5), new_record(6)]); // flushed by size

    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch1);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch2);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch3);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch4);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(2)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(3)).unwrap();

    thread::sleep(linger_duration.mul(2));

    producer.send_record(TOPIC1.to_owned(), new_record(4)).unwrap();
    thread::sleep(linger_duration.mul(2));

    producer.send_record(TOPIC1.to_owned(), new_record(5)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(6)).unwrap();

    producer.shutdown().unwrap();
}

#[test]
fn test_send_record_multiple_topic_partitions() {
    let max_batch_size = 2;
    let linger_duration = Duration::from_millis(100);
    let config = new_config(max_batch_size, linger_duration);

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);
    expect_get_topic(&mut coordinator, TOPIC2, PARTITION_COUNT2);

    let expected_batch1 = new_record_batch(vec![new_record(1), new_record(3)]);
    let expected_batch2 = new_record_batch(vec![new_record(2)]);
    let expected_batch3 = new_record_batch(vec![new_record(4)]);
    let expected_batch4 = new_record_batch(vec![new_record(5), new_record(6)]);

    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch1);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC2, P2, expected_batch2);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch3);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC2, P2, expected_batch4);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    producer.send_record(TOPIC2.to_owned(), new_record(2)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(3)).unwrap();

    thread::sleep(linger_duration.mul(2));

    producer.send_record(TOPIC1.to_owned(), new_record(4)).unwrap();
    thread::sleep(linger_duration.mul(2));

    producer.send_record(TOPIC2.to_owned(), new_record(5)).unwrap();
    producer.send_record(TOPIC2.to_owned(), new_record(6)).unwrap();

    producer.shutdown().unwrap();
}

#[test]
#[timeout(300)]
fn test_batch_reaper_releases_lock_before_sleeping() {
    let max_batch_size = 2;
    let linger_duration = Duration::from_secs(1000); // the sleep is going to be very long
    let config = new_config(max_batch_size, linger_duration);

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);

    let expected_batch = new_record_batch(vec![new_record(1), new_record(2)]);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    // Adding a record to the map of batches will waken the reaper task. We add some sleep to give it the chance to wake up and acquire the lock
    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    thread::sleep(Duration::from_millis(200));

    // This second call also requires acquiring the lock, so if the reaper task has kept the lock during its sleep, which will last about 1000 seconds,
    // then this call will never end before the test's timeout. Note that we do not shutdown in this test because the reaper thread doesn't check the
    // shutdown flag during its sleep.
    producer.send_record(TOPIC1.to_owned(), new_record(2)).unwrap();
}

#[test]
fn test_shut_down_flushes_remaining_batches() {
    // make sure that both the batch size and the linger duration are too large to be the cause of the flush in this test
    let config = new_config(1000, Duration::from_secs(1000));

    let mut coordinator = coordinator::MockClient::new();
    let mut broker = broker::MockClient::new();

    expect_list_brokers(&mut coordinator);
    expect_get_topic(&mut coordinator, TOPIC1, PARTITION_COUNT1);
    expect_get_topic(&mut coordinator, TOPIC2, PARTITION_COUNT2);

    let expected_batch1 = new_record_batch(vec![new_record(1), new_record(3)]);
    let expected_batch2 = new_record_batch(vec![new_record(2)]);

    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC1, P1, expected_batch1);
    expect_publish_raw_ignoring_timestamps(&mut broker, TOPIC2, P2, expected_batch2);

    let coordinator: Arc<dyn coordinator::Client> = Arc::new(coordinator);
    let broker: Arc<dyn broker::Client> = Arc::new(broker);
    let broker_resolver = set_up_broker_resolver(Arc::clone(&coordinator), broker);
    let mut producer = producer::Client::new_for_testing(config, coordinator, broker_resolver).unwrap();

    producer.send_record(TOPIC1.to_owned(), new_record(1)).unwrap();
    producer.send_record(TOPIC2.to_owned(), new_record(2)).unwrap();
    producer.send_record(TOPIC1.to_owned(), new_record(3)).unwrap();

    producer.shutdown().unwrap();
}

fn new_config(max_batch_size: usize, linger_duration: Duration) -> Config {
    Config { protocol_version: PROTOCOL_VERSION, max_batch_size, linger_duration }
}

fn new_record_batch(records: Vec<Record>) -> RecordBatch {
    RecordBatch { protocol_version: PROTOCOL_VERSION, records, base_timestamp: 0 }
}

fn new_record(index: u64) -> Record {
    Record {
        key: Some("key".to_owned()), // same key for all to hit a predictable partition
        value: format!("value-{index}"),
        headers: vec![],
        timestamp: 0, // ignored in this test's assertions
    }
}