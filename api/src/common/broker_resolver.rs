use crate::common::{map_coordinator_error, Result};
use coordinator::model::HostAndPort;
use std::sync::Arc;

#[cfg(test)]
#[path = "broker_resolver_test.rs"]
mod broker_resolver_test;

// only for testing
trait BrokerClientFactory {
    fn create(&self, host: &HostAndPort) -> Arc<dyn broker::Client>;
}

/// This struct maintains a pool of [broker::Client], one for each broker in the cluster, and selects the correct broker for a given partition, irrespective
/// of the topic. For obvious reasons, this should be used consistently across the `publisher` and `consumer` modules. 
pub struct BrokerResolver {
    ordered_broker_clients: Vec<Arc<dyn broker::Client>>,
}

impl BrokerResolver {
    pub fn new(coordinator_client: Arc<dyn coordinator::Client>) -> Result<Self> {
        Self::new_with_factory(coordinator_client, &DefaultBrokerClientFactory {})
    }    

    fn new_with_factory(coordinator_client: Arc<dyn coordinator::Client>, client_factory: &impl BrokerClientFactory) -> Result<Self> {
        // by contract, list_brokers returns the hosts in a stable order (registration order), and our simplifying hypotheses is that the broker
        // configuration is constant, so we can safely store them in a read-only list and use the same list forever
        let ordered_broker_clients: Vec<_> = coordinator_client.list_brokers().map_err(map_coordinator_error)?
            .brokers.iter()
            .map(|host| client_factory.create(host))
            .collect();
        
        if ordered_broker_clients.is_empty() { panic!("No brokers are registered to the coordinator") }

        Ok(Self { ordered_broker_clients })
    }
    
    pub fn client_for(&self, partition: u32) -> Arc<dyn broker::Client> {
        // as we mentioned in many places, to simplify we assume the configuration never changes and brokers are infinitely available, so we can safely
        // do a simple modulo, no consistent hashing or anything fancy
        let index = partition as usize % self.ordered_broker_clients.len();
        Arc::clone(&self.ordered_broker_clients[index])
    }
}

struct DefaultBrokerClientFactory {}
impl BrokerClientFactory for DefaultBrokerClientFactory {
    fn create(&self, host: &HostAndPort) -> Arc<dyn broker::Client> {
        Arc::new(broker::ClientImpl::new(host.full_domain()))
    }
}