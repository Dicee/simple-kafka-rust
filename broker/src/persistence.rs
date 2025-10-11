pub(crate) mod append_only_log;

#[cfg(test)]
mod persistence_test;

use std::collections::HashMap;
use std::io;
use append_only_log::*;

/// Simple struct that uniquely identifies a log group (how files are split within a log group is an implementation detail of the [LogManager]).
#[derive(Eq, PartialEq, Hash, Debug)]
struct LogKey {
    pub topic: String,
    pub partition: u32,
}

impl LogKey {
    pub fn new(topic: &str, partition: u32) -> Self {
        LogKey { topic: topic.to_string(), partition }
    }
}

/// This struct manages a collection of log files under a root path. It decouples the caller, which is domain-specific code around
/// pub/sub, from the low-level layout of the data, while providing a simple point of access. From the outside, all the caller needs
/// to know about is topic names and partition numbers and how they relate to each broker. The [LogManager] will handle the rest, and
/// will assume the caller is correct in requesting a specific partition to be written to on the local disk.
pub struct LogManager {
    root_path: String,
    log_files: HashMap<LogKey, Box<dyn LogFile>>,
}

impl LogManager {
    const BASE_FILE_NAME: &'static str = "data";
    // 10 MB to make it interesting to watch on low-scale examples, but of course not optimized for larger scale
    const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;

    pub fn new(root_path: String) -> LogManager {
        LogManager {
            root_path,
            log_files: HashMap::new(),
        }
    }

    /// Writes, without committing, arbitrary bytes to a given topic and partition. To prevent potential data loss, [Self::commit] must be called
    /// when your business logic allows to ensure the data has been persisted. Similarly to the data layout we chose for [RotatingAppendOnlyLog],
    /// we'll only allow up to 1000 partitions, which is more than enough for the small scale we will test the project at.
    pub fn write(&mut self, topic: &str, partition: u32, buf: &[u8]) -> io::Result<()> {
        let log = self.log_files
            .entry(LogKey::new(topic, partition))
            .or_insert_with(|| {
                let log_group_root_path = format!("{}/{topic}/partition={partition:04}", self.root_path);
                match RotatingAppendOnlyLog::open_latest(log_group_root_path, Self::BASE_FILE_NAME, Self::MAX_FILE_SIZE_BYTES) {
                    Ok(log) => Box::new(log),
                    Err(e) => panic!("Failed opening log file for topic {topic} and partition {partition}: {:?}", e),
                }
            });

        log.write_all(buf)
    }

    /// Flushes all buffered changes for a given topic and partition, or does nothing if nothing has been written to it.
    pub fn commit(&mut self, topic: &str, partition: u32) -> io::Result<()> {
        match self.log_files.get_mut(&LogKey::new(topic, partition)) {
            Some(log) => log.flush(),
            None => Ok(())
        }
    }
}