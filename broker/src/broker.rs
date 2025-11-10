use crate::broker::Error::{CoordinatorApi, Io, UnexpectedCoordinatorError};
use crate::persistence::indexing::{self, IndexLookupResult};
use crate::persistence::{AtomicReadAction, AtomicWriteAction, RawBatch, LogManager, RotatingAppendOnlyLog, RotatingLogReader};
use broker::model::{PollBatchesRawResponse, PollConfig};
use client_utils::ApiError;
use coordinator::model::{CoordinatorApiErrorKind, GetReadOffsetRequest, GetWriteOffsetRequest, IncrementWriteOffsetRequest};
use protocol::record::{deserialize_batch_metadata, read_batch_metadata, serialize_batch, RecordBatch, BATCH_METADATA_SIZE};
use std::fmt::{Display, Formatter};
use std::ops::Sub;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{io, thread};
use Error::InvalidReadOffset;
use InvalidReadOffsetCause::{LessOrEqualToAckReadOffset, MoreThanWriteOffset};
use crate::broker::InvalidReadOffsetCause::NoDataWrittenYet;

#[cfg(test)]
#[path = "./broker_test.rs"]
mod broker_test;

pub struct Broker {
    log_manager: LogManager,
    coordinator_client: Arc<dyn coordinator::Client>,
}

#[derive(Debug)]
pub enum Error {
    InvalidReadOffset(InvalidReadOffsetCause),
    CoordinatorApi(ApiError<CoordinatorApiErrorKind>),
    // convert to string to not carry more and more complexity as layers go. We won't need more granularity for our error handling.
    UnexpectedCoordinatorError(String),
    Io(io::Error), // those errors are closer to us since we are directly making IO operations, so we keep them granular
    Internal(String),
}

#[derive(Eq, PartialEq, Debug)]
pub enum InvalidReadOffsetCause {
    LessOrEqualToAckReadOffset { ack_read_offset: u64, invalid_offset: u64 },
    MoreThanWriteOffset { write_offset: u64, invalid_offset: u64 },
    NoDataWrittenYet { invalid_offset: u64 },
}

impl Broker {
    pub fn new(log_manager: LogManager, coordinator_client: Arc<dyn coordinator::Client>) -> Self {
        Self { log_manager, coordinator_client }
    }

    /// Serializes and commits a batch of records to disk and returns the base offset of the batch. This method is meant for ease of testing
    /// at the moment and will be removed later, because serialization is normally performed by the publisher (which isn't written yet!).
    /// # Errors
    /// - [Error::CoordinatorApi] if an error explicitly modeled by the coordinator API occured while calling it
    /// - [Error::UnexpectedCoordinatorError] if an unexpected error is encountered while calling the coordinator service
    /// - [Error::Io] if the write operation fails due to an IO error
    /// - [Error::Internal] if anything else fails
    pub fn publish(&self, topic: &str, partition: u32, record_batch: RecordBatch) -> Result<u64, Error> {
        let message_count = record_batch.records.len();
        self.publish_raw(topic, partition, serialize_batch(record_batch), message_count as u32)
    }

    /// Writes and commits to disk the provided bytes and returns the base offset of the batch
    /// # Errors
    /// - [Error::CoordinatorApi] if an error explicitly modeled by the coordinator API occured while calling it
    /// - [Error::UnexpectedCoordinatorError] if an unexpected error is encountered while calling the coordinator service
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

    /// Polls, without decoding, at most for the number of batches specified in the poll config from the given offset, for a maximum amount of time configured
    /// in the same config. If the max duration is met before we find enough batches to max it out, we return whatever has been read so far, which could be nothing,
    /// in which case the vector will be empty.
    /// # Errors
    /// - [Error::CoordinatorApi] if an error explicitly modeled by the coordinator API occured while calling it
    /// - [Error::UnexpectedCoordinatorError] if an unexpected error is encountered while calling the coordinator service
    /// - [Error::InvalidReadOffset] if the requested read offset is invalid. The cause will be detailed further in the wrapped [InvalidReadOffsetCause].
    /// - [Error::Io] if the write operation fails due to an IO error
    /// - [Error::Internal] if anything else fails
    pub fn poll_batches_raw(&self, topic: &str, partition: u32, consumer_group: String, offset: u64, poll_config: &PollConfig) -> Result<PollBatchesRawResponse, Error> {
        let start = Instant::now();
        let write_notifier = self.log_manager.get_write_notifier(topic, partition)?;
        let ack_read_offset = self.coordinator_client.get_read_offset(GetReadOffsetRequest {
            topic: topic.to_owned(), partition, consumer_group: consumer_group.clone() })?.offset;

        if let Some(read_offset) = ack_read_offset && read_offset >= offset {
            return Err(InvalidReadOffset(LessOrEqualToAckReadOffset { ack_read_offset: read_offset, invalid_offset: offset }));
        }

        let mut bytes = Vec::new();
        let mut batch_count = 0;
        let mut offset_to_read = offset;

        loop {
            let mut eof = false;
            match self.read_batch_raw(topic, partition, consumer_group.clone(), offset_to_read)? {
                None => eof = true,
                Some(RawBatch { base_offset, record_count, bytes: batch_bytes }) => {
                    bytes.extend(base_offset.to_le_bytes());
                    bytes.extend(batch_bytes);

                    batch_count += 1;
                    offset_to_read += record_count as u64;
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

    fn read_batch_raw(&self, topic: &str, partition: u32, consumer_group: String, offset: u64) -> Result<Option<RawBatch>, Error> {
        let write_offset = self.coordinator_client.get_write_offset(GetWriteOffsetRequest { topic: topic.to_owned(), partition })?.offset;

        match write_offset {
            None if offset > 0 => return Err(InvalidReadOffset(NoDataWrittenYet { invalid_offset: offset })),
            // It is legit to ask for the offset right after the write offset, it shows that the reader is caught up and waiting for the latest record.
            // It's not there yet though, so we return None.
            Some(write_offset) if offset == write_offset + 1 => return Ok(None),
            // there shouldn't be a valid scenario for requesting an offset more than 1 larger than the write offset, so it might be a bug on the consumer
            // side and we return an error to flag this
            Some(write_offset) if offset > write_offset + 1 => return Err(InvalidReadOffset(MoreThanWriteOffset { write_offset, invalid_offset: offset })),
            _ => {}
        }

        let action = ReadBatchAtOffset { write_offset, offset_to_read: offset };
        let raw_batch = self.log_manager.atomic_read(topic, partition, consumer_group, action)?;

        Ok(if raw_batch.bytes.is_empty() { None } else { Some(raw_batch) })
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

struct ReadBatchAtOffset {
    offset_to_read: u64,
    write_offset: Option<u64>,
}

impl AtomicReadAction for ReadBatchAtOffset {
    fn initialize(&self, root_path: &str) -> Result<RotatingLogReader, Error> {
        let mut reader = RotatingLogReader::open_for_index(root_path.to_owned(), 0)?;
        self.find_offset_to_read(root_path, &mut reader)?;
        Ok(reader)
    }

    fn read_from(&self, root_path: &str, reader: &mut RotatingLogReader) -> Result<RawBatch, Error> {
        let write_offset = match self.write_offset {
            None => return Ok(RawBatch::empty()), // no data has ever been written
            Some(w) => w,
        };

        match reader.try_read_u64_le()? {
            None => 
                // This is a branch that can occur when we have read all the available data but the consumer has not acknowledged it (or not fully),
                // maybe due to a temporary failure, and has to ask for it again. In this case, we have to reset the reader to go back to that offset.
                if self.offset_to_read <= write_offset { self.reset_and_read(root_path, reader) }
                // no more data to read, we're all caught up
                else { Ok(RawBatch::empty()) }
            Some(offset) => {
                if offset > write_offset {
                    // we are reading unclean data that has not yet been committed, so let's return nothing for now, waiting for the commit
                    reader.seek(-8)?;
                    return Ok(RawBatch::empty());
                }

                let metadata = read_batch_metadata(reader)?;
                // a bit inefficient to read the metadata twice, but it simplifies several aspects of the logic
                reader.seek(-(BATCH_METADATA_SIZE as i64))?;

                // if the batch we're currently aligned with doesn't contain the desired offset, we reset the reader
                if offset > self.offset_to_read || (offset + (metadata.record_count as u64)) <= self.offset_to_read {
                    return self.reset_and_read(root_path, reader);
                }

                Ok(RawBatch {
                    base_offset: offset,
                    record_count: metadata.record_count,
                    bytes: reader.read(BATCH_METADATA_SIZE + metadata.payload_byte_size as usize)?
                })
            }
        }
    }
}

impl ReadBatchAtOffset {
    fn reset_and_read(&self, root_path: &str, reader: &mut RotatingLogReader) -> Result<RawBatch, Error> {
        self.find_offset_to_read(root_path, reader)?;
        self.read_from(root_path, reader)
    }

    fn find_offset_to_read(&self, root_path: &str, reader: &mut RotatingLogReader) -> Result<(), Error> {
        match indexing::look_up(Path::new(root_path), self.offset_to_read)? {
            // Can only happen if there is no index log written yet, which means there can only be one log file at most. Since we already
            // instantiated a reader it means the file does exist, and we just want to read it from the start
            None => reader.reset_to_start()?,
            Some(IndexLookupResult { log_file_path, byte_offset }) => reader.reset_to_offset(log_file_path, byte_offset)?,
        };
        self.seek_exact_offset(reader)?;
        Ok(())
    }

    /// This function assumes that the given [RotatingLogReader] is already reading the correct file containing the desired `record_offset`,
    /// and the cursor is already positioned close to the correct byte offset (via an index search). From this position, the function will
    /// iterate, decoding just the header of each batch along the way and skip the rest if it is determined that the batch won't contain
    /// the desired record. The scan stops when the reader reaches a batch which contains the desired record, and the cursor ends up at the
    /// start of the batch, even if the desired record is not the first in the batch, since the smallest serialization unit is a batch.
    fn seek_exact_offset(&self, log_reader: &mut RotatingLogReader) -> io::Result<()> {
        let record_offset = self.offset_to_read;
        loop {
            match log_reader.try_read_u64_le()? {
                None => break, // the record with the desired offset doesn't exist yet, i.e. the consumer is all caught up
                Some(base_offset) => {
                    if self.write_offset.unwrap_or(0) <= base_offset {
                        log_reader.seek(-8)?; // un-consume the offset for the read logic later on
                        break; // we are seeing an unclean batch that has not yet been committed, so we break here
                    }

                    let metadata = deserialize_batch_metadata(&log_reader.read(BATCH_METADATA_SIZE)?);
                    if base_offset <= record_offset && base_offset + metadata.record_count as u64 > record_offset {
                        log_reader.seek(-(BATCH_METADATA_SIZE as i64 + 8))?; // un-consume the metadata and the offset for the read logic later on
                        break; // we reached the batch that contains the desired offset
                    }

                    // skip the entire batch. The cast is safe as u32 always fits into i64.
                    log_reader.seek(metadata.payload_byte_size as i64)?;
                }
            }
        }
        Ok(())
    }
}

impl From<coordinator::Error> for Error {
    fn from(e: coordinator::Error) -> Self {
        match e {
            coordinator::Error::Api(api_error) => CoordinatorApi(api_error),
            _ => UnexpectedCoordinatorError(format!("{e:?}")),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self { Io(e) }
}

impl Display for InvalidReadOffsetCause {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            LessOrEqualToAckReadOffset { ack_read_offset, invalid_offset } =>
                &format!("Invalid read offset {invalid_offset}: it is less than (or equal to) the acknowledged read offset of {ack_read_offset}, and we shouldn't go \
                    back in time before the last acknowledged read offset."),

            MoreThanWriteOffset { write_offset, invalid_offset } =>
                &format!("Invalid read offset {invalid_offset}: it exceeds than the write offset of {write_offset}, i.e. no such offset exists."),

            NoDataWrittenYet { invalid_offset} =>
                &format!("Invalid read offset {invalid_offset}: no data has been written (and committed) yet, therefore the only valid offset to request for \
                    reading is 0."),
        };
        f.write_str(message)
    }
}
