use super::*;
use crate::mock_utils::expect_get_topic;
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use coordinator::model::{CoordinatorApiErrorKind, GetTopicRequest};
use mockall::predicate;
use protocol::record::Record;
use std::sync::Arc;
use client_utils::ApiError;

const TOPIC1: &str = "topic1";
const TOPIC2: &str = "topic2";
const ERROR_MSG: &str = "Something went terribly wrong";

#[test]
fn test_select_partition_record_with_key() {
    let mut coordinator = coordinator::MockClient::new();
    expect_get_topic(&mut coordinator, TOPIC1, 128);
    expect_get_topic(&mut coordinator, TOPIC2, 64);

    let mut partition_selector = PartitionSelector::new(Arc::new(coordinator));

    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(Some(String::from("hello!"))))).has_ok(64);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(Some(String::from("hello!"))))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(Some(String::from("hi!"))))).has_ok(110);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(Some(String::from("hi!"))))).has_ok(46);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(Some(String::from("hiya!"))))).has_ok(16);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(Some(String::from("hiya!"))))).has_ok(16);
}

#[test]
fn test_select_partition_record_without_key() {
    let mut coordinator = coordinator::MockClient::new();
    expect_get_topic(&mut coordinator, TOPIC1, 3);
    expect_get_topic(&mut coordinator, TOPIC2, 2);

    let mut partition_selector = PartitionSelector::new(Arc::new(coordinator));
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(1);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(2);

    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(1);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(0);

    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(1);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(1);
}

#[test]
fn test_select_partition_mixed_records() {
    let mut coordinator = coordinator::MockClient::new();
    expect_get_topic(&mut coordinator, TOPIC1, 128);
    expect_get_topic(&mut coordinator, TOPIC2, 64);

    let mut partition_selector = PartitionSelector::new(Arc::new(coordinator));

    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(Some(String::from("hello!"))))).has_ok(64);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(Some(String::from("hello!"))))).has_ok(0);

    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(1);
    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(Some(String::from("hi!"))))).has_ok(110);

    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(0);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(Some(String::from("hi!"))))).has_ok(46);

    assert_that!(partition_selector.select_partition(TOPIC1, &new_record(None))).has_ok(2);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(1);
    assert_that!(partition_selector.select_partition(TOPIC2, &new_record(None))).has_ok(2);
}

#[test]
#[should_panic(expected = "Topic topic1 has no partitions, which should be impossible")]
fn test_select_partition_topic_has_no_partition() {
    let mut coordinator = coordinator::MockClient::new();
    expect_get_topic(&mut coordinator, TOPIC1, 0);

    let mut partition_selector = PartitionSelector::new(Arc::new(coordinator));
    partition_selector.select_partition(TOPIC1, &new_record(None)).unwrap();
}

#[test]
fn test_select_partition_coordinator_failure() {
    let mut coordinator = coordinator::MockClient::new();
    coordinator.expect_get_topic()
        .with(predicate::eq(GetTopicRequest { name: TOPIC1.to_owned() }))
        .returning(|_| { Err(coordinator::Error::Api(ApiError { kind: CoordinatorApiErrorKind::Internal, message: ERROR_MSG.to_owned() })) });

    let mut partition_selector = PartitionSelector::new(Arc::new(coordinator));
    match partition_selector.select_partition(TOPIC1, &new_record(None)) {
        Err(crate::common::Error::CoordinatorApi(ApiError {
             kind: CoordinatorApiErrorKind::Internal,
             message
         })) => assert_that!(message).is_equal_to(ERROR_MSG.to_owned()),
        _ => unreachable!(),
    }
}

fn new_record(key: Option<String>) -> Record {
    Record {
        key,
        timestamp: 56,
        headers: Vec::new(),
        value: String::from("world"),
    }
}
