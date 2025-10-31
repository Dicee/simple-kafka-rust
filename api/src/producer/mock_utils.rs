use std::io::Cursor;
use std::sync::Arc;
use predicates::ord::eq;
use predicates::prelude::predicate;
use broker::model::PublishResponse;
use coordinator::model::{HostAndPort, ListBrokersResponse};
use protocol::record::{read_next_batch, RecordBatch};
use crate::common::broker_resolver::{BrokerResolver, MockBrokerClientFactory};

// in Java I would mock the resolver entirely, but in Rust mocking is really not that convenient, so since it's a relatively simple struct I'll only
// partially mock it
pub(crate) fn set_up_broker_resolver(coordinator: Arc<dyn coordinator::Client>, broker: Arc<dyn broker::Client>) -> BrokerResolver {
    let mut broker_client_factory = MockBrokerClientFactory::new();
    broker_client_factory
        .expect_create()
        .times(1)
        .return_const(Arc::clone(&broker));

    BrokerResolver::new_with_factory(Arc::clone(&coordinator), &broker_client_factory).unwrap()
}

pub(crate) fn mock_list_brokers(coordinator: &mut coordinator::MockClient) {
    coordinator
        .expect_list_brokers()
        .returning(|| Ok(ListBrokersResponse { brokers: vec![HostAndPort::new(String::from("localhost"), 8000)] }));
}

pub(crate) fn expect_publish_raw(broker_mock: &mut broker::MockClient, topic: &str, partition: u32, batch: RecordBatch) {
    broker_mock
        .expect_publish_raw()
        .with(eq(topic.to_owned()), eq(partition), eq_serialized_batch(batch), eq(1))
        .times(1)
        .returning(|_, _, _, _| Ok(PublishResponse { base_offset: 1 }));
}

pub fn eq_serialized_batch(batch: RecordBatch) -> impl predicates::Predicate<Vec<u8>> {
    predicate::function(move |bytes| read_next_batch(&mut Cursor::new(bytes)).unwrap() == batch)
}