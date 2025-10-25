use crate::broker::Error::{Coordinator, Io, Unexpected};
use crate::persistence::{AtomicReadAction, IndexedRecord, LogManager, ReadError, RotatingLogReader, WriteError};
use coordinator::client::Client as CoordinatorClient;
use coordinator::model::{GetReadOffsetRequest, GetWriteOffsetRequest, IncrementWriteOffsetRequest};
use protocol::record::{deserialize_batch_metadata, read_batch_metadata, read_next_batch, serialize_batch_into, RecordBatch, BATCH_METADATA_SIZE};
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::persistence::indexing::{self, IndexLookupResult};

#[cfg(test)]
#[path = "./broker_test.rs"]
mod broker_test;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RecordBatchWithOffset {
    pub base_offset: u64,
    pub batch: RecordBatch,
}

pub struct Broker {
    log_manager: LogManager,
    coordinator_client: Arc<dyn CoordinatorClient>,
}

#[derive(Debug)]
pub enum Error {
    // convert to string to not carry more and more complexity as layers go. We won't need more granularity for our error handling.
    Coordinator(String),
    Io(io::Error), // those errors are closer to us since we are directly making IO operations, so we keep them granular
    Unexpected(String),
}

impl Broker {
    /// Commits a batch of records to disk and returns the base offset of the batch
    pub fn publish(&mut self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<u64, Error> {
        let message_count = record_batch.records.len();
        let next_offset = self.coordinator_client.get_write_offset(GetWriteOffsetRequest { topic: topic.to_owned(), partition })?
            .offset.map(|o| o + 1).unwrap_or(0);

        // Note that adding the offset has to be done by the broker as there may be multiple publishers, and only the broker can do this
        // while guaranteeing consistency.
        let mut bytes = Vec::new();
        bytes.extend(next_offset.to_le_bytes());
        serialize_batch_into(record_batch, &mut bytes);

        self.log_manager.write_and_commit(topic, partition, next_offset, &bytes)?;
        self.coordinator_client.increment_write_offset(IncrementWriteOffsetRequest {
            topic: topic.to_string(),
            partition,
            inc: u32::try_from(message_count).unwrap()
        })?;

        Ok(next_offset)
    }

    pub fn read_next_batch(&mut self, topic: &str, partition: u32, consumer_group: String) -> Result<Option<RecordBatchWithOffset>, Error> {
        let write_offset = self.coordinator_client.get_write_offset(GetWriteOffsetRequest { topic: topic.to_owned(), partition })?.offset;
        let ack_offset = self.coordinator_client.get_read_offset(GetReadOffsetRequest {
            topic: topic.to_owned(),
            partition,
            consumer_group: consumer_group.clone()
        })?.offset;

        let IndexedRecord(base_offset, bytes) = self.log_manager.atomic_read(topic, partition, consumer_group, ReadNextBatch {
            write_offset,
            read_ack_offset: ack_offset
        })?;

        Ok(if bytes.is_empty() { None }
        else {
            let record_batch = read_next_batch(&mut Cursor::new(bytes))?;
            Some(RecordBatchWithOffset {
                batch: record_batch,
                base_offset,
            })
        })
    }
}

struct ReadNextBatch {
    write_offset: Option<u64>,
    read_ack_offset: Option<u64>,
}

impl AtomicReadAction for ReadNextBatch {
    fn initialize(&self, root_path: &str) -> io::Result<RotatingLogReader> {
        match self.read_ack_offset {
            None => RotatingLogReader::open_for_index(root_path.to_owned(), 0),
            Some(ack_offset) => {
                let record_offset = ack_offset + 1;
                match indexing::look_up(Path::new(root_path), record_offset)? {
                    // no data written yet so we know the reader must be initialized to the first file with no offset
                    None => RotatingLogReader::open_for_index(root_path.to_owned(), 0),
                    Some(IndexLookupResult { log_file_path, byte_offset }) =>
                        open_log_at_record_offset(record_offset, log_file_path, byte_offset)
                }
            }
        }
    }

    fn read_from(&self, reader: &mut RotatingLogReader) -> Result<IndexedRecord, ReadError> {
        let write_offset = match self.write_offset {
            None => return Ok(IndexedRecord(0, vec![])), // no data hsa ever been written
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

impl From<coordinator::client::Error> for Error {
    fn from(err: coordinator::client::Error) -> Self { Coordinator(format!("{err:?}")) }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self { Io(e) }
}

impl From<WriteError> for Error {
    fn from(err: WriteError) -> Self {
        match err {
            WriteError::Io(e) => Io(e),
            _ => Unexpected(format!("{err:?}"))
        }
    }
}

impl From<ReadError> for Error {
    fn from(err: ReadError) -> Self {
        match err {
            ReadError::Io(e) => Io(e),
            _ => Unexpected(format!("{err:?}"))
        }
    }
}
