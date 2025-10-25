mod append_only_log;
mod log_reader;
pub mod indexing;

#[cfg(test)]
mod persistence_test;

pub use log_reader::RotatingLogReader;

use append_only_log::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{atomic, Arc};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, iter, thread};
use std::io::Error;
use std::path::PathBuf;
use mockall::automock;
use tokio::sync::oneshot;

const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

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
    pub fn write(&mut self, topic: &str, partition: u32, index: u64, buf: &[u8]) -> Result<(), WriteError> {
        self.get_or_create_partition_manager(topic, partition, |e| WriteError::Io(e))?.write(index, buf, false)
    }

    /// Same as [Self::write] but it additionally flushes the data written so far.
    /// # Errors
    /// - [WriteError::Io] if an IO error occurs during the write operation
    /// - [WriteError::WriterThreadDisconnected] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn write_and_commit(&mut self, topic: &str, partition: u32, index: u64, buf: &[u8]) -> Result<(), WriteError> {
        self.get_or_create_partition_manager(topic, partition, |e| WriteError::Io(e))?.write(index, buf, true)
    }

    /// Atomically performs arbitrary read operations specified by the [AtomicReadAction] with the guarantee that no other threads will be able to interleave
    /// any read operation to the same [RotatingLogReader] as long as this action is running.
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the read operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn atomic_read(&mut self, topic: &str, partition: u32, consumer_group: String, atomic_read: impl AtomicReadAction) -> Result<IndexedRecord, ReadError> {
        self.get_or_create_partition_manager(topic, partition, |e| ReadError::Io(e))?.atomic_read(consumer_group, atomic_read)
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
            log: RotatingAppendOnlyLog::open_latest(partition_root_path.clone(), MAX_FILE_SIZE_BYTES)?,
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
    fn write(&self, index: u64, bytes: &[u8], flush: bool) -> Result<(), WriteError> {
        let (response_tx, response_rx) = oneshot::channel();

        let send_result = self.writer_tx.send(WriteRequest { index, bytes: Arc::from(bytes), flush, response_tx });
        if send_result.is_err() { return Err(WriteError::WriterThreadDisconnected) }

        response_rx.blocking_recv().unwrap_or_else(|_| Err(WriteError::WriterThreadDisconnected))
    }

    /// Atomically performs arbitrary read operations specified by the [AtomicReadAction] with the guarantee that no other threads will be able to interleave
    /// any read operation to the same [RotatingLogReader] as long as this action is running.
    /// # Errors
    /// - [ReadError::Io] if an IO error occurs during the read operation
    /// - [ReadError::ReaderThreadDisconnected] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn atomic_read(&mut self, consumer_group: String, atomic_action: impl AtomicReadAction) -> Result<IndexedRecord, ReadError> {
        let atomic_action = Box::new(atomic_action);
        let consumer_manager = match self.consumers.entry(consumer_group.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let (reader_tx, reader_rx) = std::sync::mpsc::channel();
                let mut reader = PartitionLogReader {
                    reader: atomic_action.initialize(&self.root_path)?,
                    read_requests: reader_rx,
                    shutdown: Arc::clone(&self.shutdown),
                };

                let loop_timeout = self.loop_timeout; // avoid capturing self in the closure
                let reader_description = format!("Reader(topic={}, partition={}, consumer_group={})", self.topic, self.partition, consumer_group);
                let reader_thread = thread::spawn(move || reader.start_read_loop(loop_timeout, reader_description));
                entry.insert(ConsumerManager { reader_tx, reader_thread })
            }
        };

        let (response_tx, response_rx) = oneshot::channel();
        let send_result = consumer_manager.reader_tx.send(ReadRequest { atomic_action, response_tx });
        if send_result.is_err() { return Err(ReadError::ReaderThreadDisconnected) }

        response_rx.blocking_recv().unwrap_or_else(|_| Err(ReadError::ReaderThreadDisconnected))
    }

    fn shutdown(self) -> thread::Result<()> {
        println!("Shutting down partition under root path {}... Waiting for {} reader(s) and 1 writer to shut down.",
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
    index: u64,
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
            let mut result = self.log.write_all(request.index, &request.bytes);
            if result.is_ok() && request.flush { result = self.log.flush() }
            result.map_err(|e| WriteError::Io(e))
        }, loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<(), WriteError> for WriteRequest {
    fn response_tx(self) -> oneshot::Sender<Result<(), WriteError>> { self.response_tx }
}

struct ReadRequest {
    atomic_action: Box<dyn AtomicReadAction>,
    response_tx: oneshot::Sender<Result<IndexedRecord, ReadError>>,
}

/// This trait allows to inject read external logic with the guarantee that the provided [RotatingLogReader] is only available on a single thread,
/// and no other thread can make any operation until [AtomicReadAction::read_from] has completed. This means that implementations of this trait can
/// make any number of accesses to the disk without worrying about another producer thread sending a read request in the middle and producing unexpected
/// outcomes. We opted for this design to decouple [LogManager] from the protocol layer. We wanted it to remain purely about low-level IO handling, and
/// not requiring any knowledge about the binary format. That way, only minimal changes (if any) would be required to this code even if we entirely changed
/// our approach for the binary format.
#[automock]
pub trait AtomicReadAction : Send + 'static {
    fn initialize(&self, root_path: &str) -> io::Result<RotatingLogReader>;

    fn read_from(&self, reader: &mut RotatingLogReader) -> io::Result<IndexedRecord>;
}

#[derive(Debug)]
pub struct IndexedRecord(pub u64, pub Vec<u8>);

#[derive(Debug)]
pub enum ReadError {
    ReaderThreadDisconnected,
    Io(io::Error),
}

impl From<io::Error> for ReadError {
    fn from(e: Error) -> Self { ReadError::Io(e) }
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
            request.atomic_action.read_from(&mut self.reader).map_err(|e| ReadError::Io(e))
        }, loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<IndexedRecord, ReadError> for ReadRequest {
    fn response_tx(self) -> oneshot::Sender<Result<IndexedRecord, ReadError>> { self.response_tx }
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

fn get_log_path(root_path: &str, first_index: u64) -> PathBuf {
    PathBuf::from(root_path).join(get_log_name(first_index))
}

fn get_log_name(first_index: u64) -> String {
    format!("{first_index:05}.{LOG_EXTENSION}")
}

fn get_index_name(first_index: u64) -> String {
    format!("{first_index:05}.{INDEX_EXTENSION}")
}