use linked_hash_set::LinkedHashSet;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use coordinator::model::HostAndPort;

#[cfg(test)]
#[path = "./registry_test.rs"]
mod registry_test;

/// Allows registering a fleet of brokers to be aware of all of them in one place and allow producers and consumers to pick the right broker for a given
/// topic partition. Like the rest of the coordinator service, it is out-of-scope for us to build a realistic, robust solution. Here I decided to store
/// this part in memory rather than in SQLite like the state and metadata because alive brokers are a more dynamic thing that the data to persist, even
/// though under our assumptions the number of brokers within a test session is fixed, and they are infinitely available.
#[derive(Clone)]
pub struct BrokerRegistry {
    brokers: Arc<RwLock<LinkedHashSet<HostAndPort>>>
}

impl BrokerRegistry {
    pub fn new() -> Self {
        Self {
            brokers: Arc::new(RwLock::new(LinkedHashSet::new())),
        }
    }

    /// Registers a broker if and only if it has not been registered yet. If it has already been registered, calling this method again is a no-op.
    /// Note that brokers will be returned in a stable order when accessed later on, i.e. the order in which they were registered. This allows relying
    /// on the order for partitioning.
    pub fn register_broker(&mut self, host: HostAndPort) {
        self.brokers.write().unwrap().insert_if_absent(host);
    }

    /// Returns a read lock guard to the ordered set of brokers. Callers should obviously use the result as soon as possible and drop it right after
    /// they're done in order to avoid holding the lock for too long.
    pub fn brokers(&self) -> RwLockReadGuard<'_, LinkedHashSet<HostAndPort>> {
        self.brokers.read().unwrap()
    }
}