use crate::broker::{Broker, Error, InvalidReadOffsetCause};
use crate::persistence::{indexing, AtomicWriteAction, RawBatch, LogManager, RotatingAppendOnlyLog};
use assertor::{assert_that, EqualityAssertion, OptionAssertion, ResultAssertion};
use broker::model::{PollBatchesRawResponse, PollConfig, RecordBatchWithOffset};
use client_utils::ApiError;
use coordinator::model::CoordinatorApiErrorKind::Internal;
use coordinator::model::*;
use file_test_utils::TempTestDir;
use ntest_timeout::timeout;
use protocol::record::{serialize_batch, serialize_batch_into, Header, Record, RecordBatch};
use std::io::Cursor;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use Error::InvalidReadOffset;
use InvalidReadOffsetCause::LessOrEqualToAckReadOffset;
use crate::broker::InvalidReadOffsetCause::{MoreThanWriteOffset, NoDataWrittenYet};

const TOPIC: &str = "chess-news";
const PARTITION: u32 = 116;
const CONSUMER_GROUP: &str = "danya-fans";

static POLL_CONFIG: PollConfig = PollConfig { max_batches: 3, max_wait: Duration::from_millis(100) };

#[test]
fn test_serialization_round_trip_from_first_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

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

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0).unwrap())
        .has_value(RecordBatchWithOffset { base_offset: 0, batch: batch_1 });

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 2).unwrap())
        .has_value(RecordBatchWithOffset { base_offset: 2, batch: batch_2 });

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3).unwrap()).is_none();
}

// this test was added due to an indexing bug I had when writing exactly at the threshold that triggers indexing
#[test]
fn test_write_more_than_indexing_threshold() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    for i in 0..2 * indexing::MAX_INDEX_GAP + 5 {
        let batch = new_record_batch(i, vec![new_record(i, 1), new_record(i, 2)]);
        broker.publish(TOPIC, PARTITION, batch).unwrap();
    }

    for i in 0..2 * indexing::MAX_INDEX_GAP + 5 {
        let batch = new_record_batch(i, vec![new_record(i, 1), new_record(i, 2)]);
        let base_offset = i * 2;
        assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), base_offset).unwrap())
            .has_value(RecordBatchWithOffset { base_offset, batch });
    }
}

#[test]
fn test_publish_write_offset_not_committed_if_failure_coordinator_failure() {
    let temp_dir = TempTestDir::create();

    let mut coordinator_client = coordinator::MockClient::new();
    coordinator_client.expect_get_write_offset()
        .returning(|_| Err(coordinator::Error::Api(ApiError { kind: Internal, message : String::from("Oopsy") })));

    coordinator_client.expect_increment_write_offset().never();

    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator_client);
    let broker = new_broker(&temp_dir, &coordinator_client);

    let base_timestamp = 15;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);
    match broker.publish(TOPIC, PARTITION, batch) {
        Err(Error::CoordinatorApi(ApiError {
            kind: CoordinatorApiErrorKind::Internal,
            message
        })) => assert_that!(message).is_equal_to(String::from("Oopsy")),
        _ => unreachable!(),
    }
}

#[test]
fn test_read_batch_initialize_from_exact_base_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

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

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 3, batch: batch_2 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 5))
        .has_ok(None);
}

#[test]
fn test_read_batch_initialize_with_offset_within_a_batch() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

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

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch_2 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 5))
        .has_ok(None);
}

#[test]
fn test_read_batch_initialize_with_offset_at_the_end_of_a_batch() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

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

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 1, batch: batch_2 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 4))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 4, batch: batch_3 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 6))
        .has_ok(None);
}

#[test]
fn test_read_batch_initialize_before_any_data_is_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(None);

    let invalid_offset = 1;
    match read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), invalid_offset) {
        Err(InvalidReadOffset(cause)) => {
            assert_that!(cause).is_equal_to(NoDataWrittenYet { invalid_offset });
        },
        _ => unreachable!()
    }

    let base_timestamp = 14;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);
    broker.publish(TOPIC, PARTITION, batch.clone()).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 1))
        .has_ok(None);
}

#[test]
fn test_read_batch_initialize_while_first_batch_is_being_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    let base_timestamp = 14;
    let batch = new_record_batch(base_timestamp, vec![new_record(base_timestamp, 1)]);

    let mut bytes = Vec::new();
    bytes.extend(0u64.to_le_bytes());
    serialize_batch_into(batch.clone(), &mut bytes);

    // simulate an unclean write, with only part of the data flushed to disk
    let log_manager = LogManager::new(temp_dir.path_as_str().to_owned());
    log_manager.atomic_write(TOPIC, PARTITION, WriteAndCommit(0, bytes[0..13].to_vec())).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(None);

    log_manager.atomic_write(TOPIC, PARTITION, WriteAndCommit(0, bytes[13..].to_vec())).unwrap();
    coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        inc: 1,
    }).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 1))
        .has_ok(None);
}

#[test]
fn test_read_batch_while_a_batch_is_being_written() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

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
    let log_manager = LogManager::new(temp_dir.path_as_str().to_owned());
    log_manager.atomic_write(TOPIC, PARTITION, WriteAndCommit(0, bytes[0..13].to_vec())).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch: batch_1 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 2))
        .has_ok(None);

    log_manager.atomic_write(TOPIC, PARTITION, WriteAndCommit(0, bytes[13..].to_vec())).unwrap();
    coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        inc: batch_2.records.len() as u32,
    }).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 2))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch_2 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 4))
        .has_ok(None);
}

#[test]
fn test_read_batch_invalid_offset_larger_than_write_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    let batch = new_record_batch(0, vec![new_record(0, 1)]);
    broker.publish(TOPIC, PARTITION, batch.clone()).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 1))
        .has_ok(None);

    let invalid_offset = 2;
    match read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), invalid_offset) {
        Err(InvalidReadOffset(cause)) => {
            assert_that!(cause).is_equal_to(MoreThanWriteOffset { write_offset: 0, invalid_offset });
        },
        _ => unreachable!()
    }

    // check that the failed read leaves us in a clean state from which we can perform correct reads
    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 1))
        .has_ok(None);
}

#[test]
fn test_read_batch_offset_more_than_unacknowledged_read_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    let batch1 = new_record_batch(0, vec![new_record(0, 1), new_record(0, 2)]);
    let batch2 = new_record_batch(1, vec![new_record(1, 1), new_record(1, 2)]);
    let batch3 = new_record_batch(2, vec![new_record(2, 1)]);
    broker.publish(TOPIC, PARTITION, batch1.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch2).unwrap();
    broker.publish(TOPIC, PARTITION, batch3.clone()).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch: batch1 }));

    // we skipped a batch, and the reader allowed it
    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 4))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 4, batch: batch3 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 5))
        .has_ok(None);
}

#[test]
fn test_read_batch_offset_smaller_than_unacknowledged_read_offset_but_larger_than_ack_read_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    let batch1 = new_record_batch(0, vec![new_record(0, 1), new_record(0, 2)]);
    let batch2 = new_record_batch(1, vec![new_record(1, 1), new_record(1, 2)]);
    broker.publish(TOPIC, PARTITION, batch1.clone()).unwrap();
    broker.publish(TOPIC, PARTITION, batch2.clone()).unwrap();

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 0, batch: batch1 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 2))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch2.clone() }));

    // the consumer managed to process the message with offset 2, but not the one with offset 3, so it requests offset 3 and since it's in the same
    // batch as offset 2, we have to return the whole batch again (since we do not decode the binary format on the broker side)
    ack_read_offset(&coordinator_client, 2);
    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch2.clone() }));

    let batch3 = new_record_batch(2, vec![new_record(2, 1), new_record(2, 2)]);
    broker.publish(TOPIC, PARTITION, batch3.clone()).unwrap();

    // same idea, but this hits a different code path because previously we were at eof, whereas now there is more data to read
    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 2, batch: batch2 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 4))
        .has_ok(Some(RecordBatchWithOffset { base_offset: 4, batch: batch3 }));

    assert_that!(read_batch(&broker, TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 6))
        .has_ok(None);
}

#[test]
#[timeout(150)]
fn test_poll_batches_requested_offset_lower_than_read_offset() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    let batch1 = new_record_batch(0, vec![new_record(0, 1), new_record(0, 2)]);
    let batch2 = new_record_batch(1, vec![new_record(1, 1), new_record(1, 2)]);

    broker.publish(TOPIC, PARTITION, batch1).unwrap();
    broker.publish(TOPIC, PARTITION, batch2.clone()).unwrap();

    let read_offset = 1;
    ack_read_offset(&coordinator_client, read_offset);

    for invalid_offset in 0..=read_offset {
        match broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), invalid_offset, &POLL_CONFIG) {
            Err(InvalidReadOffset(cause)) => {
                assert_that!(cause).is_equal_to(LessOrEqualToAckReadOffset { ack_read_offset: read_offset, invalid_offset });
            },
            _ => unreachable!()
        }
    }

    // check that the failed read leaves us in a clean state from which we can perform correct reads
    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 2, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(Some(read_offset), vec![raw_batch_for(2, batch2), ]));
}

#[test]
#[timeout(150)]
fn test_poll_batches_no_records() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = new_broker(&temp_dir, &coordinator_client);

    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(None, vec![]));
}

#[test]
#[timeout(150)]
fn test_poll_batches_waits_to_find_enough_batches() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = Arc::new(new_broker(&temp_dir, &coordinator_client));
    let broker_clone = Arc::clone(&broker);

    let batch_1 = new_record_batch(0, vec![new_record(0, 0)]);
    let batch_2 = new_record_batch(1, vec![new_record(1, 0), new_record(1, 1)]);
    let batch_3 = new_record_batch(2, vec![new_record(2, 1)]);

    let batch_1_clone = batch_1.clone();
    let batch_2_clone = batch_2.clone();
    let batch_3_clone = batch_3.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        broker_clone.publish(TOPIC, PARTITION, batch_1).unwrap();

        thread::sleep(Duration::from_millis(30));
        broker_clone.publish(TOPIC, PARTITION, batch_2).unwrap();

        thread::sleep(Duration::from_millis(10));
        broker_clone.publish(TOPIC, PARTITION, batch_3).unwrap();
    });

    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(None, vec![
            raw_batch_for(0, batch_1_clone),
            raw_batch_for(1, batch_2_clone),
            raw_batch_for(3, batch_3_clone),
        ]));
}

#[test]
#[timeout(150)]
fn test_poll_batches_multiple_waiters() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = Arc::new(new_broker(&temp_dir, &coordinator_client));
    let broker_clone = Arc::clone(&broker);

    let batch_1 = new_record_batch(0, vec![new_record(0, 0)]);
    let batch_1_clone = batch_1.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(90));
        broker_clone.publish(TOPIC, PARTITION, batch_1_clone).unwrap();
    });

    let broker_clone = Arc::clone(&broker);
    let batch_1_clone = batch_1.clone();

    let consumer1 = thread::spawn(move || {
        assert_that!(broker_clone.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0, &POLL_CONFIG))
            .has_ok(to_poll_batches_raw_response(None, vec![raw_batch_for(0, batch_1_clone)]));
    });

    let consumer2 = thread::spawn(move || {
        assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, "other_group".to_owned(), 0, &POLL_CONFIG))
            .has_ok(to_poll_batches_raw_response(None, vec![raw_batch_for(0, batch_1)]));
    });

    consumer1.join().unwrap();
    consumer2.join().unwrap();
}

#[test]
#[timeout(30)]
fn test_poll_batches_returns_immediately_if_enough_batches_present() {
    let temp_dir = TempTestDir::create();
    let dummy_coordinator = coordinator::DummyClient::new();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(dummy_coordinator);

    let broker = Arc::new(new_broker(&temp_dir, &coordinator_client));
    let broker_clone = Arc::clone(&broker);

    let batch_1 = new_record_batch(0, vec![new_record(0, 0)]);
    let batch_2 = new_record_batch(1, vec![new_record(1, 0), new_record(1, 1)]);
    let batch_3 = new_record_batch(2, vec![new_record(2, 1)]);

    broker_clone.publish(TOPIC, PARTITION, batch_1.clone()).unwrap();
    broker_clone.publish(TOPIC, PARTITION, batch_2.clone()).unwrap();
    broker_clone.publish(TOPIC, PARTITION, batch_3.clone()).unwrap();

    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(None, vec![
            raw_batch_for(0, batch_1),
            raw_batch_for(1, batch_2),
            raw_batch_for(3, batch_3),
        ]));
}

#[test]
#[timeout(225)]
fn test_poll_batches_returns_fewer_batches_if_timeout_exceeded() {
    let temp_dir = TempTestDir::create();
    let coordinator_client: Arc<dyn coordinator::Client> = Arc::new(coordinator::DummyClient::new());
    let broker = Arc::new(new_broker(&temp_dir, &coordinator_client));
    let broker_clone = Arc::clone(&broker);

    let batch_1 = new_record_batch(0, vec![new_record(0, 0)]);
    let batch_2 = new_record_batch(1, vec![new_record(1, 0), new_record(1, 1)]);
    let batch_3 = new_record_batch(2, vec![new_record(2, 1)]);

    let batch_1_clone = batch_1.clone();
    let batch_2_clone = batch_2.clone();
    let batch_3_clone = batch_3.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        broker_clone.publish(TOPIC, PARTITION, batch_1).unwrap();

        thread::sleep(Duration::from_millis(50));
        broker_clone.publish(TOPIC, PARTITION, batch_2).unwrap();

        thread::sleep(Duration::from_millis(60));
        broker_clone.publish(TOPIC, PARTITION, batch_3).unwrap();
    });

    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 0, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(
            None,
            vec![
                raw_batch_for(0, batch_1_clone),
                raw_batch_for(1, batch_2_clone),
            ])
        );

    assert_that!(broker.poll_batches_raw(TOPIC, PARTITION, CONSUMER_GROUP.to_owned(), 3, &POLL_CONFIG))
        .has_ok(to_poll_batches_raw_response(None, vec![raw_batch_for(3, batch_3_clone)]));
}

fn ack_read_offset(coordinator_client:&Arc<dyn coordinator::Client>, offset: u64) {
    coordinator_client.ack_read_offset(AckReadOffsetRequest {
        topic: TOPIC.to_owned(),
        partition: PARTITION,
        consumer_group: CONSUMER_GROUP.to_owned(),
        offset,
    }).unwrap();
}

fn read_batch(broker: &Broker, topic: &str, partition: u32, consumer_group: String, offset: u64) -> Result<Option<RecordBatchWithOffset>, Error> {
    Ok(match broker.read_batch_raw(topic, partition, consumer_group, offset)? {
        None => None,
        Some(RawBatch { base_offset, record_count, bytes }) => {
            let record_batch = protocol::record::read_next_batch(&mut Cursor::new(bytes))?;
            assert_that!(record_batch.records.len()).is_equal_to(record_count as usize);
            Some(RecordBatchWithOffset { batch: record_batch, base_offset })
        }
    })
}

fn assert_write_offset_is(dummy_coordinator: &dyn coordinator::Client, offset: Option<u64>) {
    assert_that!(dummy_coordinator.get_write_offset(GetWriteOffsetRequest { topic: TOPIC.to_owned(), partition: PARTITION }))
        .has_ok(GetWriteOffsetResponse { offset });
}

fn to_poll_batches_raw_response(ack_read_offset: Option<u64>, raw_batches: Vec<RawBatch>) -> PollBatchesRawResponse {
    let mut bytes = Vec::new();
    for RawBatch { base_offset, bytes: record_bytes, .. } in raw_batches {
        bytes.extend(base_offset.to_le_bytes());
        bytes.extend(record_bytes);
    }

    PollBatchesRawResponse { ack_read_offset, bytes }
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

fn raw_batch_for(base_offset: u64, batch: RecordBatch) -> RawBatch {
    RawBatch {
        base_offset,
        record_count: batch.records.len() as u32,
        bytes: serialize_batch(batch),
    }
}

fn new_broker(temp_dir: &TempTestDir, coordinator_client: &Arc<dyn coordinator::Client>) -> Broker {
    Broker {
        log_manager: LogManager::new(temp_dir.path_as_str().to_owned()),
        coordinator_client: Arc::clone(&coordinator_client),
    }
}

struct WriteAndCommit(u64, Vec<u8>);

impl AtomicWriteAction for WriteAndCommit {
    fn write_to(&self, log: &mut RotatingAppendOnlyLog) -> Result<u64, Error> {
        log.write_all_indexable(self.0, &self.1)?;
        log.flush()?;
        Ok(self.0)
    }
}