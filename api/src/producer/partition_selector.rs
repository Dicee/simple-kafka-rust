use std::hash::{DefaultHasher, Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use linked_hash_map::LinkedHashMap;
use coordinator::model::GetTopicRequest;
use protocol::record::Record;
use crate::common::{map_coordinator_error, Result};

#[cfg(test)]
#[path = "./partition_selector_test.rs"]
mod partition_selector_test;

pub struct PartitionSelector {
    coordinator: Arc<dyn coordinator::Client>,
    hash_selector: HashPartitionSelector,
    round_robin_selector: RoundRobinPartitionSelector,
}

impl PartitionSelector {
    pub fn new(coordinator: Arc<dyn coordinator::Client>) -> Self {
        Self {
            coordinator,
            hash_selector: HashPartitionSelector::new(),
            round_robin_selector: RoundRobinPartitionSelector::new(),
        }
    }

    /// Selects a partition for a given record and topic. If the record has a key, a [HashPartitionSelector] will be used,
    /// otherwise we will fall back to a [RoundRobinPartitionSelector].
    /// # Errors
    /// [Error::CoordinatorApi] or [Error::Ureq] if we failed to make a call to the coordinator service
    pub fn select_partition(&mut self, topic: &str, record: &Record) -> Result<u32> {
        // in real life we would cache this locally as it rarely changes, but to simplify we'll just fetch it every time, anyway it's all local
        // so it should not be an issue
        let partition_count = self.coordinator.get_topic(GetTopicRequest { name: topic.to_owned() })
            .map_err(map_coordinator_error)?.partition_count;

        if partition_count == 0 { panic!("Topic {} has no partitions, which should be impossible", topic) };

        Ok(self.hash_selector.try_select_partition(partition_count, &record)
            .unwrap_or_else(|| self.round_robin_selector.select_partition(topic, partition_count)))
    }
}

/// Hash-based partition selector, based on the record's key.
struct HashPartitionSelector {}
impl HashPartitionSelector {
    fn new() -> Self { Self{} }

    /// Selects a partition for a given record and partition count based on the key of the record. If the record has no key, [None] is returned.
    /// # Errors
    /// [Error::CoordinatorApi] or [Error::Ureq] if we failed to make a call to the coordinator service
    fn try_select_partition(&self, partition_count: u32, record: &Record) -> Option<u32> {
        match &record.key {
            None => None,
            Some(key) => {
                // As we mention in the README.md, we assume to simplify our project that the number of partitions is fixed, as well as the number of
                // broker instances. Therefore, we do not bother using consistent hashing or a similarly stable hash.
                let selected_partition = Self::calculate_hash(&key) % partition_count as u64;
                Some(selected_partition as u32) // safe cast since it's the result of a modulo with a u32 value
            }
        }
    }

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }
}

/// Selects a partition for a record in a round-robin fashion, with a simple modulo on the partition count.
struct RoundRobinPartitionSelector {
    next_partition: LinkedHashMap<String, u32>,
}

impl RoundRobinPartitionSelector {
    fn new() -> Self { Self { next_partition: LinkedHashMap::new() } }

    fn select_partition(&mut self, topic: &str, partition_count: u32) -> u32 {
        *self.next_partition.entry(topic.to_owned())
            .and_modify(|e| *e = (*e + 1) % partition_count)
            .or_insert(0)
    }
}