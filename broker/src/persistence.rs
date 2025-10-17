pub(crate) mod append_only_log;

#[cfg(test)]
mod persistence_test;
mod log_reader;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{io, thread};
use std::sync::{atomic, Arc};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::oneshot;
use append_only_log::*;
use crate::persistence::log_reader::RotatingLogReader;

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
    log_files: HashMap<LogKey, RotatingAppendOnlyLog>,
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
        let log = match self.log_files.entry(LogKey::new(topic, partition)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let path = format!("{}/{topic}/partition={partition:04}", self.root_path);
                let log = RotatingAppendOnlyLog::open_latest(path, Self::BASE_FILE_NAME, Self::MAX_FILE_SIZE_BYTES)?;
                entry.insert(log)
            }
        };


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

/// This struct manages a single partition of a topic, controlling a single thread for writes, and one thread per consumer group. The communication
/// between threads is handled with MPSC channels, so that multiple server threads can send read and write requests to the partition, while a single
/// thread is taking care of those requests since these are mutable resources (e.g. file being written, cursor of a file being read etc). Parallelism
/// comes from the fact that there are multiple partitions, so single-threaded handling of each partition is not an issue.
struct PartitionLogManager {
    root_path: String,
    base_file_name: &'static str,
    writer_tx: Sender<WriteRequest>,
    writer_thread: JoinHandle<()>,
    consumers: HashMap<String, ConsumerManager>,
    shutdown: Arc<AtomicBool>,
}

/// Holds the resources associated with a single consumer group's reader thread
struct ConsumerManager {
    reader_tx: Sender<ReadRequest>,
    reader_thread: JoinHandle<()>,
}

impl PartitionLogManager {
    pub fn new(root_path: String, base_file_name: &'static str, max_byte_size: u64) -> io::Result<Self> {
        let (writer_tx, writer_rx) = std::sync::mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut writer = PartitionLogWriter {
            log: RotatingAppendOnlyLog::open_latest(root_path.clone(), base_file_name, max_byte_size)?,
            write_requests: writer_rx,
            shutdown: Arc::clone(&shutdown),
        };
        let writer_thread = thread::spawn(move || writer.start_write_loop());

        Ok(Self { root_path, base_file_name, writer_tx, writer_thread, shutdown, consumers: HashMap::new() })
    }

    /// Writes the bytes, optionally flushing them out if requested.
    /// # Errors
    /// - [WriteError::Io] if an IO error occurs during the write operation
    /// - [WriteError::WriterThreadDisconnected] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn write(&self, bytes: Vec<u8>, flush: bool) -> Result<(), WriteError> {
        let (response_tx, response_rx) = oneshot::channel();

        let send_result = self.writer_tx.send(WriteRequest { bytes, flush, response_tx });
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
                    reader: RotatingLogReader::open(self.root_path.clone(), self.base_file_name, 0).map_err(|e| ReadError::Io(e))?,
                    read_requests: reader_rx,
                    shutdown: Arc::clone(&self.shutdown),
                };
                let reader_thread = thread::spawn(move || reader.start_read_loop());
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
        self.shutdown.store(true, atomic::Ordering::Relaxed);
        for (_, consumer_manager) in self.consumers {
            consumer_manager.reader_thread.join()?;
        }
        self.writer_thread.join()
    }
}

trait MpscRequest<T, Err> {
    fn response_tx(self) -> oneshot::Sender<Result<T, Err>>;
}

struct WriteRequest {
    bytes: Vec<u8>,
    flush: bool,
    response_tx: oneshot::Sender<Result<(), WriteError>>,
}

enum WriteError {
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
    fn start_write_loop(&mut self) {
        start_mpsc_request_loop(&mut self.write_requests, |request| {
            let mut result = self.log.write_all(&request.bytes);
            if result.is_ok() && request.flush  { result = self.log.flush() }
            result.map_err(|e| WriteError::Io(e))
        });
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

enum ReadError {
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
    fn start_read_loop(&mut self) {
        start_mpsc_request_loop(&mut self.read_requests, |request| {
            match request.action {
                ReadAction::Seek(byte_offset) => self.reader.seek(byte_offset).map(|_| ReadResponse::SuccessfulSeek),
                ReadAction::Read(len) => self.reader.read(len).map(|bytes| ReadResponse::SuccessfulRead(bytes)),
            }.map_err(|e| ReadError::Io(e))
        });
    }
}

impl MpscRequest<ReadResponse, ReadError> for ReadRequest {
    fn response_tx(self) -> oneshot::Sender<Result<ReadResponse, ReadError>> { self.response_tx }
}

fn start_mpsc_request_loop<Req, Res, Err, F>(requests: &mut Receiver<Req>, mut do_process: F)
where
    Req: MpscRequest<Res, Err>,
    F: FnMut(&Req) -> Result<Res, Err>,
{
    // We don't need to shut down very fast, so we can afford having a slow loop, and it saves CPU cycles. Note
    // that we will still write as soon as a request arrives, we're only waiting when there is no request.
    let timeout: Duration = Duration::from_secs(5);
    loop {
        let request = match requests.recv_timeout(timeout) {
            Ok(bytes) => bytes,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                eprintln!("Senders disconnected from the read requests channel, exiting write loop");
                break
            },
        };

        let result = do_process(&request);
        match request.response_tx().send(result) {
            Err(_) => eprintln!(
                "Failed sending a response to the read request. This may be an issue with a single consumer, so the read \
                     loop will stay alive."
            ),
            _ => {},
        }
    }
}