pub(crate) mod append_only_log;

#[cfg(test)]
mod persistence_test;
mod log_reader;

use crate::persistence::log_reader::RotatingLogReader;
use append_only_log::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{atomic, Arc};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, iter, thread};
use tokio::sync::oneshot;

const BASE_FILE_NAME: &'static str = "data";
// 10 MB to make it interesting to watch on low-scale examples, but of course not optimized for larger scale
const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
// We don't need to shut down very fast, so we can afford having a slow loop, and it saves CPU cycles. Note
// that we will still write as soon as a request arrives, we're only waiting when there is no request.
const LOOP_TIMEOUT: Duration = Duration::from_secs(5);

/// Simple struct that uniquely identifies a log group (how files are split within a log group is an implementation detail of the [LogManager]).
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
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
    log_files: HashMap<LogKey, PartitionLogManager>,
    loop_timeout: Duration,
    shutdown: Arc<AtomicBool>,
}

impl LogManager {
    pub fn new(root_path: String) -> LogManager { Self::new_with_loop_timeout(root_path, LOOP_TIMEOUT) }

    // only for testing
    fn new_with_loop_timeout(root_path: String, loop_timeout: Duration) -> LogManager {
        LogManager {
            root_path,
            log_files: HashMap::new(),
            loop_timeout,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Writes, without committing, arbitrary bytes to a given topic and partition. To prevent potential data loss, [Self::write_and_commit] must be called
    /// when your business logic needs to ensure the data has been persisted. Similarly to the data layout we chose for [RotatingAppendOnlyLog],
    /// we'll only allow up to 1000 partitions, which is more than enough for the small scale we will test the project at.
    /// # Errors
    /// - [WriteError::Io] if an IO error occurs during the write operation
    /// - [WriteError::WriterThreadDisconnected] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn write(&mut self, topic: &str, partition: u32, buf: &[u8]) -> Result<(), WriteError> {
        self.get_or_create_partition_manager(topic, partition, |e| WriteError::Io(e))?.write(buf, false)
    }

    /// Same as [Self::write] but it additionally flushes the data written so far.
    /// # Errors
    /// - [WriteError::Io] if an IO error occurs during the write operation
    /// - [WriteError::WriterThreadDisconnected] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn write_and_commit(&mut self, topic: &str, partition: u32, buf: &[u8]) -> Result<(), WriteError> {
        self.get_or_create_partition_manager(topic, partition, |e| WriteError::Io(e))?.write(buf, true)
    }

    /// Seeks the log file for a given topic, partition and consumer group by the requested amount of bytes from its current position. Caution,
    /// this only works within a single file, if the offset exceeds the file size, the cursor won't overflow to the next file and will stay at
    /// the EOF of the current one.
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the seek operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn seek(&mut self, topic: &str, partition: u32, consumer_group: String, byte_offset: i64) -> Result<(), ReadError> {
        self.get_or_create_partition_manager(topic, partition, |e| ReadError::Io(e))?.seek(consumer_group, byte_offset)
    }

    /// Reads the request amount of bytes from the current position for a given topic, partition and consumer group. Caution, this only works within a single
    /// file, if the requested length exceeds the file size, this method will fail **except** if the cursor is exactly at EOF, in which case either one of two
    /// things will happen:
    /// - if the next file already exists, rotation will take place automatically and the bytes will be read from the next file. In this case, the number of
    ///   bytes in the next file must still be equal or larger than the requested number of bytes.
    /// - if the next file doesn't exist, this method will return an empty vector to signal that there is no new data to be read at the moment.
    ///
    /// In short, reads with this method need to perfectly align with EOF for the current file (either not intersect EOF, or end exactly at EOF, or start
    /// exactly at EOF).
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the read operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn read(&mut self, topic: &str, partition: u32, consumer_group: String, len: usize) -> Result<Vec<u8>, ReadError> {
        self.get_or_create_partition_manager(topic, partition, |e| ReadError::Io(e))?.read(consumer_group, len)
    }

    fn get_or_create_partition_manager<F, Err>(&mut self, topic: &str, partition: u32, map_error: F) -> Result<&mut PartitionLogManager, Err>
    where F: Fn(io::Error) -> Err,
    {
        Ok(match self.log_files.entry(LogKey::new(topic, partition)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {

                let log_manager = PartitionLogManager::new(
                    &self.root_path,
                    topic, partition,
                    self.loop_timeout,
                    Arc::clone(&self.shutdown),
                );
                entry.insert(log_manager.map_err(map_error)?)
            }
        })
    }

    pub fn shutdown(self) -> thread::Result<()> {
        self.shutdown.store(true, atomic::Ordering::Relaxed);
        println!("Shutting down log manager... Waiting for {} partition log managers to shut down", self.log_files.len());

        self.log_files.into_iter()
            .map(|(_, partition_manager)| partition_manager.shutdown())
            .fold(Ok(()), |acc, result| if result.is_err() { result } else { acc })
    }
}

/// This struct manages a single partition of a topic, controlling a single thread for writes, and one thread per consumer group. The communication
/// between threads is handled with MPSC channels, so that multiple server threads can send read and write requests to the partition, while a single
/// thread is taking care of those requests since these are mutable resources (e.g. file being written, cursor of a file being read etc). Parallelism
/// comes from the fact that there are multiple partitions, so single-threaded handling of each partition is not an issue.
struct PartitionLogManager {
    topic: String,
    partition: u32,
    root_path: String,
    writer_tx: Sender<WriteRequest>,
    writer_thread: JoinHandle<()>,
    consumers: HashMap<String, ConsumerManager>,
    loop_timeout: Duration,
    shutdown: Arc<AtomicBool>,
}

/// Holds the resources associated with a single consumer group's reader thread
struct ConsumerManager {
    reader_tx: Sender<ReadRequest>,
    reader_thread: JoinHandle<()>,
}

impl PartitionLogManager {
    pub fn new(root_path: &str, topic: &str, partition: u32, loop_timeout: Duration, shutdown: Arc<AtomicBool>) -> io::Result<Self> {
        let partition_root_path = format!("{}/{topic}/partition={partition:04}", root_path);
        let (writer_tx, writer_rx) = std::sync::mpsc::channel();

        let mut writer = PartitionLogWriter {
            log: RotatingAppendOnlyLog::open_latest(partition_root_path.clone(), BASE_FILE_NAME, MAX_FILE_SIZE_BYTES)?,
            write_requests: writer_rx,
            shutdown: Arc::clone(&shutdown),
        };
        let writer_description = format!("Writer(topic={topic}, partition={partition})");
        let writer_thread = thread::spawn(move || writer.start_write_loop(loop_timeout, writer_description));

        Ok(Self {
            topic: topic.to_owned(),
            partition,
            root_path: partition_root_path,
            writer_tx,
            writer_thread,
            shutdown,
            loop_timeout,
            consumers: HashMap::new()
        })
    }

    /// Writes the bytes, optionally flushing them out if requested.
    /// # Errors
    /// - [WriteError::Io] if an IO error occurs during the write operation
    /// - [WriteError::WriterThreadDisconnected] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn write(&self, bytes: &[u8], flush: bool) -> Result<(), WriteError> {
        let (response_tx, response_rx) = oneshot::channel();

        let send_result = self.writer_tx.send(WriteRequest { bytes: Arc::from(bytes), flush, response_tx });
        if send_result.is_err() { return Err(WriteError::WriterThreadDisconnected) }

        response_rx.blocking_recv().unwrap_or_else(|_| Err(WriteError::WriterThreadDisconnected))
    }

    /// Seeks by a given byte offset in the log file for the given consumer.
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the seek operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn seek(&mut self, consumer_group: String, byte_offset: i64) -> Result<(), ReadError> {
        self.handle_read_request(consumer_group, ReadAction::Seek(byte_offset), |result| result.map(|response| match response {
            ReadResponse::SuccessfulRead(_) => panic!("Unexpected response SuccessfulRead when the request was a Seek"),
            ReadResponse::SuccessfulSeek => (),
        }))
    }

    /// Reads the request amount of bytes from the current offset for the given consumer.
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the read operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn read(&mut self, consumer_group: String, len: usize) -> Result<Vec<u8>, ReadError> {
        self.handle_read_request(consumer_group, ReadAction::Read(len), |result| result.map(|response| match response {
            ReadResponse::SuccessfulSeek => panic!("Unexpected response SuccessfulSeek when the request was a Read"),
            ReadResponse::SuccessfulRead(bytes) => bytes,
        }))
    }

    fn handle_read_request<T, F>(&mut self, consumer_group: String, action: ReadAction, map_result: F) -> Result<T, ReadError>
    where F: FnOnce(Result<ReadResponse, ReadError>) -> Result<T, ReadError>
    {
        let consumer_manager = match self.consumers.entry(consumer_group.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let (reader_tx, reader_rx) = std::sync::mpsc::channel();
                let mut reader = PartitionLogReader {
                    // we don't support lookup by offset yet so we always start from the start
                    reader: RotatingLogReader::open(self.root_path.clone(), BASE_FILE_NAME, 0).map_err(|e| ReadError::Io(e))?,
                    read_requests: reader_rx,
                    shutdown: Arc::clone(&self.shutdown),
                };

                let loop_timeout = self.loop_timeout;
                let reader_description = format!("Reader(topic={}, partition={}, consumer_group={})", self.topic, self.partition, consumer_group);
                let reader_thread = thread::spawn(move || reader.start_read_loop(loop_timeout, reader_description));
                entry.insert(ConsumerManager { reader_tx, reader_thread })
            }
        };

        let (response_tx, response_rx) = oneshot::channel();
        let send_result = consumer_manager.reader_tx.send(ReadRequest { action, response_tx });
        if send_result.is_err() { return Err(ReadError::ReaderThreadDisconnected) }

        match response_rx.blocking_recv() {
            Ok(result) => map_result(result),
            Err(_) => Err(ReadError::ReaderThreadDisconnected),
        }
    }

    fn shutdown(self) -> thread::Result<()> {
        println!("Shutting down partition under root path{}... Waiting for {} reader(s) and 1 writer to shut down.",
                 self.root_path, self.consumers.len());

        self.consumers.into_iter()
            .map(|(_, consumer_manager)| consumer_manager.reader_thread.join())
            .chain(iter::once(self.writer_thread.join()))
            .fold(Ok(()), |acc, result| if result.is_err() { result } else { acc })
    }
}

trait MpscRequest<Res, Err> {
    fn response_tx(self) -> oneshot::Sender<Result<Res, Err>>;
}

struct WriteRequest {
    bytes: Arc<[u8]>,
    flush: bool,
    response_tx: oneshot::Sender<Result<(), WriteError>>,
}

#[derive(Debug)]
pub enum WriteError {
    WriterThreadDisconnected,
    Io(io::Error),
}

/// Handles the resources and logic to continuously write to a rotating log file for a single partition.
struct PartitionLogWriter {
    log: RotatingAppendOnlyLog,
    write_requests: Receiver<WriteRequest>,
    shutdown: Arc<AtomicBool>
}

impl PartitionLogWriter {
    fn start_write_loop(&mut self, loop_timeout: Duration, description: String) {
        start_mpsc_request_loop(&self.write_requests, |request| {
            let mut result = self.log.write_all(&request.bytes);
            if result.is_ok() && request.flush  { result = self.log.flush() }
            result.map_err(|e| WriteError::Io(e))
        }, loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<(), WriteError> for WriteRequest {
    fn response_tx(self) -> oneshot::Sender<Result<(), WriteError>> { self.response_tx }
}

struct ReadRequest {
    action: ReadAction,
    response_tx: oneshot::Sender<Result<ReadResponse, ReadError>>,
}

enum ReadResponse {
    SuccessfulSeek,
    SuccessfulRead(Vec<u8>),
}

enum ReadAction {
    Seek(i64),
    Read(usize),
}

#[derive(Debug)]
pub enum ReadError {
    ReaderThreadDisconnected,
    Io(io::Error),
}

/// Handles the resources and logic to continuously read from a rotating log file for a single partition.
struct PartitionLogReader {
    reader: RotatingLogReader,
    read_requests: Receiver<ReadRequest>,
    shutdown: Arc<AtomicBool>,
}

impl PartitionLogReader {
    fn start_read_loop(&mut self, loop_timeout: Duration, description: String) {
        start_mpsc_request_loop(&self.read_requests, |request| {
            match request.action {
                ReadAction::Seek(byte_offset) => self.reader.seek(byte_offset).map(|_| ReadResponse::SuccessfulSeek),
                ReadAction::Read(len) => self.reader.read(len).map(|bytes| ReadResponse::SuccessfulRead(bytes)),
            }.map_err(|e| ReadError::Io(e))
        }, loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<ReadResponse, ReadError> for ReadRequest {
    fn response_tx(self) -> oneshot::Sender<Result<ReadResponse, ReadError>> { self.response_tx }
}

fn start_mpsc_request_loop<Req, Res, Err, F>(
    requests: &Receiver<Req>,
    mut do_process: F,
    loop_timeout: Duration,
    shutdown: &Arc<AtomicBool>,
    description: String,
) where
    Req: MpscRequest<Res, Err>,
    F: FnMut(&Req) -> Result<Res, Err>,
{
    loop {
        if shutdown.load(atomic::Ordering::Relaxed) { break }

        let request = match requests.recv_timeout(loop_timeout) {
            Ok(bytes) => bytes,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                eprintln!("{description} - Senders disconnected from the requests channel, exiting write loop");
                break
            },
        };

        let result = do_process(&request);
        match request.response_tx().send(result) {
            Err(_) => eprintln!("{description} - Failed sending a response to the requester. This may be a temporary or \
                isolated issue, so the loop will stay alive."),
            _ => {},
        }
    }
}