use crate::common::broker_resolver::{BrokerResolver, MockBrokerClientFactory};
use broker::model::PublishResponse;
use coordinator::model::{GetTopicRequest, GetTopicResponse, HostAndPort, ListBrokersResponse};
use mockall::Predicate;
use predicates::ord::eq;
use predicates::prelude::predicate;
use protocol::record::{read_next_batch, RecordBatch};
use std::io::Cursor;
use std::sync::Arc;

// in Java I would mock the resolver entirely, but in Rust mocking is really not that convenient, so since it's a relatively simple struct I'll only
// partially mock it
pub(crate) fn set_up_broker_resolver(coordinator: Arc<dyn coordinator::Client>, broker: Arc<dyn broker::Client>) -> BrokerResolver {
    let mut broker_client_factory = MockBrokerClientFactory::new();
    broker_client_factory
        .expect_create()
        .times(1)
        .return_const(Arc::clone(&broker));

    BrokerResolver::new_with_factory(coordinator, &broker_client_factory).unwrap()
}

pub(crate) fn expect_list_brokers(coordinator: &mut coordinator::MockClient) {
    coordinator
        .expect_list_brokers()
        .returning(|| Ok(ListBrokersResponse { brokers: vec![HostAndPort::new(String::from("localhost"), 8000)] }));
}

pub(crate) fn expect_get_topic(coordinator: &mut coordinator::MockClient, topic: &str, partition_count: u32) {
    coordinator.expect_get_topic()
        .with(eq(GetTopicRequest { name: topic.to_owned() }))
        .returning(move |request| Ok(GetTopicResponse { name: request.name, partition_count }));
}

pub(crate) fn expect_publish_raw(broker: &mut broker::MockClient, topic: &str, partition: u32, batch: RecordBatch) {
    let record_count = batch.records.len() as u32;
    broker
        .expect_publish_raw()
        .with(eq(topic.to_owned()), eq(partition), eq_serialized_batch(batch), eq(record_count))
        .times(1)
        .returning(|_, _, _, _| Ok(PublishResponse { base_offset: 1 }));
}

pub fn eq_serialized_batch(batch: RecordBatch) -> impl Predicate<Vec<u8>> {
    predicate::function(move |bytes| read_next_batch(&mut Cursor::new(bytes)).unwrap() == batch)
}

// more shallow assertion than the above because I was too lazy to mock time to have predictable timestamps
pub(crate) fn expect_publish_raw_ignoring_timestamps(broker: &mut broker::MockClient, topic: &str, partition: u32, batch: RecordBatch) {
    let record_count = batch.records.len() as u32;
    broker
        .expect_publish_raw()
        .with(eq(topic.to_owned()), eq(partition), eq_serialized_batch_ignoring_timestamps(batch), eq(record_count))
        .times(1)
        .returning(|_, _, _, _| Ok(PublishResponse { base_offset: 1 }));
}

// more shallow assertion than the above because I was too lazy to mock time to have predictable timestamps
pub(crate) fn eq_serialized_batch_ignoring_timestamps(expected: RecordBatch) -> impl Predicate<Vec<u8>> {
    predicate::function(move |bytes| {
        let actual = read_next_batch(&mut Cursor::new(bytes)).unwrap();
        if actual.protocol_version != expected.protocol_version { return false }

        for (actual_record, expected_record) in actual.records.iter().zip(expected.records.iter()) {
            if actual_record.key != expected_record.key { return false }
            if actual_record.value != expected_record.value { return false }
            if actual_record.headers != expected_record.headers { return false }
        }
        true
    })
}