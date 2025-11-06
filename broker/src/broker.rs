use crate::broker::Error::{Coordinator, Io};
use crate::persistence::indexing::{self, IndexLookupResult};
use crate::persistence::{AtomicReadAction, AtomicWriteAction, IndexedRecord, LogManager, RotatingAppendOnlyLog, RotatingLogReader};
use broker::model::{PollBatchesRawResponse, PollConfig};
use coordinator::model::{GetReadOffsetRequest, GetWriteOffsetRequest, IncrementWriteOffsetRequest};
use protocol::record::{deserialize_batch_metadata, read_batch_metadata, serialize_batch, RecordBatch, BATCH_METADATA_SIZE};
use std::ops::Sub;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::{io, thread};

#[cfg(test)]
#[path = "./broker_test.rs"]
mod broker_test;

pub struct Broker {
    log_manager: LogManager,
    coordinator_client: Arc<dyn coordinator::Client>,
}

#[derive(Debug)]
pub enum Error {
    // convert to string to not carry more and more complexity as layers go. We won't need more granularity for our error handling.
    Coordinator(String),
    Io(io::Error), // those errors are closer to us since we are directly making IO operations, so we keep them granular
    Internal(String),
}

impl Broker {
    pub fn new(log_manager: LogManager, coordinator_client: Arc<dyn coordinator::Client>) -> Self {
        Self { log_manager, coordinator_client }
    }

    /// Serializes and commits a batch of records to disk and returns the base offset of the batch. This method is meant for ease of testing
    /// at the moment and will be removed later, because serialization is normally performed by the publisher (which isn't written yet!).
    /// # Errors
    /// - [Error::Coordinator] if any error is encountered while calling the coordinator service
    /// - [Error::Io] if the write operation fails due to an IO error
    /// - [Error::Internal] if anything else fails
    pub fn publish(&self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<u64, Error> {
        let message_count = record_batch.records.len();
        self.publish_raw(topic, partition, serialize_batch(record_batch), message_count as u32)
    }

    /// Writes and commits to disk the provided bytes and returns the base offset of the batch
    /// # Errors
    /// - [Error::Coordinator] if any error is encountered while calling the coordinator service
    /// - [Error::Io] if the write operation fails due to an IO error
    /// - [Error::Internal] if anything else fails
    pub fn publish_raw(&self, topic: &str, partition: u32, bytes: Vec<u8>, record_count: u32) -> Result<u64, Error> {
        self.log_manager.atomic_write(topic, partition, WriteAndCommit {
            topic: topic.to_owned(),
            partition,
            bytes,
            record_count,
            coordinator_client: Arc::clone(&self.coordinator_client),
        })
    }

    /// Polls, without decoding, at most for the number of batches specified in the poll config, for a maximum amount of time configured in the same config.
    /// If the max duration is met before we find enough batches to max it out, we return whatever has been read so far, which could be nothing, in which case
    /// the vector will be empty.
    pub fn poll_batches_raw(&self, topic: &str, partition: u32, consumer_group: String, poll_config: &PollConfig) -> Result<PollBatchesRawResponse, Error> {
        let start = Instant::now();
        let write_notifier = self.log_manager.get_write_notifier(topic, partition)?;
        let ack_read_offset = self.coordinator_client.get_read_offset(GetReadOffsetRequest {
            topic: topic.to_owned(),
            partition,
            consumer_group: consumer_group.clone()
        })?.offset;

        let mut bytes = Vec::new();
        let mut batch_count = 0;

        loop {
            let mut eof = false;
            match self.read_next_batch_raw(topic, partition, consumer_group.clone(), ack_read_offset)? {
                None => eof = true,
                Some(IndexedRecord(base_offset, batch_bytes)) => {
                    bytes.extend(base_offset.to_le_bytes());
                    bytes.extend(batch_bytes);

                    batch_count += 1;
                },
            }
            
            let elapsed = start.elapsed();
            if batch_count == poll_config.max_batches || elapsed.ge(&poll_config.max_wait) {
                break;
            }

            if eof {
                let timeout = poll_config.max_wait.sub(elapsed);
                write_notifier.wait_new_write(timeout);
                if start.elapsed().ge(&poll_config.max_wait) { break; }
            }
        }

        Ok(PollBatchesRawResponse { ack_read_offset, bytes })
    }

    fn read_next_batch_raw(&self, topic: &str, partition: u32, consumer_group: String, ack_read_offset: Option<u64>) -> Result<Option<IndexedRecord>, Error> {
        let write_offset = self.coordinator_client.get_write_offset(GetWriteOffsetRequest { topic: topic.to_owned(), partition })?.offset;

        let action = ReadNextBatch { write_offset, ack_read_offset };
        let indexed_record = self.log_manager.atomic_read(topic, partition, consumer_group, action)?;

        Ok(if indexed_record.1.is_empty() { None } else { Some(indexed_record) })
    }

    /// Attempts shutting down the broker gracefully (terminating all background threads, closing all file handles etc)
    pub fn shutdown(self) -> thread::Result<()> {
        println!("Shutting down broker...");
        self.log_manager.shutdown()?;
        println!("Broker shutdown complete!");
        Ok(())
    }
}

struct WriteAndCommit {
    topic: String,
    partition: u32,
    coordinator_client: Arc<dyn coordinator::Client>,
    record_count: u32,
    bytes: Vec<u8>,
}

impl AtomicWriteAction for WriteAndCommit {
    fn write_to(&self, log: &mut RotatingAppendOnlyLog) -> Result<u64, Error> {
        let next_offset = self.coordinator_client.get_write_offset(GetWriteOffsetRequest { topic: self.topic.clone(), partition: self.partition })?
            .offset.map(|o| o + 1).unwrap_or(0);

        // Note that adding the offset has to be done by the broker as there may be multiple publishers, and only the broker can do this
        // while guaranteeing consistency.
        log.write_all_indexable(next_offset, &next_offset.to_le_bytes())?;
        log.write_all(&self.bytes)?;
        log.flush()?;

        self.coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
            topic: self.topic.clone(),
            partition: self.partition,
            inc: self.record_count,
        })?;

        Ok(next_offset)
    }
}

struct ReadNextBatch {
    write_offset: Option<u64>,
    ack_read_offset: Option<u64>,
}

impl AtomicReadAction for ReadNextBatch {
    fn initialize(&self, root_path: &str) -> Result<RotatingLogReader, Error> {
        Ok(match self.ack_read_offset {
            None => RotatingLogReader::open_for_index(root_path.to_owned(), 0)?,
            Some(ack_offset) => {
                let record_offset = ack_offset + 1;
                match indexing::look_up(Path::new(root_path), record_offset)? {
                    // no data written yet so we know the reader must be initialized to the first file with no offset
                    None => RotatingLogReader::open_for_index(root_path.to_owned(), 0)?,
                    Some(IndexLookupResult { log_file_path, byte_offset }) =>
                        open_log_at_record_offset(record_offset, log_file_path, byte_offset)?
                }
            }
        })
    }

    fn read_from(&self, reader: &mut RotatingLogReader) -> Result<IndexedRecord, Error> {
        let write_offset = match self.write_offset {
            None => return Ok(IndexedRecord(0, vec![])), // no data has ever been written
            Some(w) => w,
        };

        let offset = reader.read_u64_le()?;
        Ok(match offset {
            None => IndexedRecord(0, vec![]), // no more data to read, we're all caught up
            Some(offset) => {
                if offset > write_offset {
                    // we are reading unclean data that has not yet been committed, so let's return nothing for now, waiting for the commit
                    reader.seek(-8)?;
                    IndexedRecord(0, vec![])
                } else {
                    let metadata = read_batch_metadata(reader)?;
                    // TODO: a bit inefficient to read the metadata twice, but for now I don't want to expose the metadata type to the persistence module
                    reader.seek(-(BATCH_METADATA_SIZE as i64))?;
                    IndexedRecord(offset, reader.read(BATCH_METADATA_SIZE + metadata.payload_byte_size as usize)?)
                }
            }
        })
    }
}

fn open_log_at_record_offset(record_offset: u64, log_file_path: PathBuf, byte_offset: u64) -> io::Result<RotatingLogReader> {
    let mut log_reader = RotatingLogReader::open_at_offset(log_file_path, byte_offset)?;

    loop {
        match log_reader.read_u64_le()? {
            None => break, // the record with the desired offset doesn't exist yet, i.e. the consumer is all caught up
            Some(base_offset) =>
            {
                let metadata = deserialize_batch_metadata(&log_reader.read(BATCH_METADATA_SIZE)?);
                if base_offset <= record_offset &&  base_offset + metadata.record_count as u64 > record_offset {
                    log_reader.seek(-(BATCH_METADATA_SIZE as i64 + 8))?; // un-consume the metadata and the offset for the read logic later on
                    break; // we reached the batch that contains the desired offset
                }

                // skip the entire batch. The cast is safe as u32 always fits into i64.
                log_reader.seek(metadata.payload_byte_size as i64)?;
            }
        }
    }

    Ok(log_reader)
}

impl From<coordinator::Error> for Error {
    fn from(err: coordinator::Error) -> Self { Coordinator(format!("{err:?}")) }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self { Io(e) }
}