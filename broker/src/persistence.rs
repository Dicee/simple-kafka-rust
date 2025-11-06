use crate::broker::Error as BrokerError;
use mockall::automock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{atomic, Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, iter, thread};
use tokio::sync::oneshot;

pub use append_only_log::RotatingAppendOnlyLog;
pub use log_reader::RotatingLogReader;

mod append_only_log;
mod log_reader;
pub mod indexing;

#[cfg(test)]
mod persistence_test;

const LOG_EXTENSION: &str = "log";
const INDEX_EXTENSION: &str = "index";

// 10 MB to make it interesting to watch on low-scale examples, but of course not optimized for larger scale
const MAX_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
// We don't need to shut down very fast, so we can afford having a slow loop, and it saves CPU cycles. Note
// that we will still write as soon as a request arrives, we're only waiting when there is no request.
const LOOP_TIMEOUT: Duration = Duration::from_secs(5);

const WORKER_THREAD_DISCONNECTED: &str = "The channel to the single-threaded worker that performing the read or write task \
    for this topic/partition(/consumer group) has been disconnected at one end or the other";

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
    log_files: Arc<Mutex<HashMap<LogKey, Arc<PartitionLogManager>>>>,
    loop_timeout: Duration,
    shutdown: Arc<AtomicBool>,
}

impl LogManager {
    pub fn new(root_path: String) -> LogManager { Self::new_with_loop_timeout(root_path, LOOP_TIMEOUT) }

    // only for testing
    fn new_with_loop_timeout(root_path: String, loop_timeout: Duration) -> LogManager {
        LogManager {
            root_path,
            log_files: Arc::new(Mutex::new(HashMap::new())),
            loop_timeout,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Atomically performs arbitrary write operations specified by the [AtomicWriteAction] with the guarantee that no other threads will be able to interleave
    /// any write operation to the same [RotatingAppendOnlyLog] as long as this action is running. Note that similarly to the data layout we chose for
    /// [RotatingAppendOnlyLog], we'll only allow up to 1000 partitions, which is more than enough for the small scale we will test the project at.
    /// # Errors
    /// - [BrokerError::Io] if an IO error occurs during the write operation
    /// - [BrokerError::Internal] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn atomic_write(&self, topic: &str, partition: u32, atomic_write: impl AtomicWriteAction) -> Result<u64, BrokerError> {
        self.get_or_create_partition_manager(topic, partition, |e| BrokerError::Io(e))?.atomic_write(atomic_write)
    }

    /// Atomically performs arbitrary read operations specified by the [AtomicReadAction] with the guarantee that no other threads will be able to interleave
    /// any read operation to the same [RotatingLogReader] as long as this action is running.
    /// # Errors
    /// - [BrokerError::Io] if an IO error occurs during the read operation
    /// - [BrokerError::Internal] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    pub fn atomic_read(&self, topic: &str, partition: u32, consumer_group: String, atomic_read: impl AtomicReadAction) -> Result<IndexedRecord, BrokerError> {
        self.get_or_create_partition_manager(topic, partition, |e| BrokerError::Io(e))?.atomic_read(consumer_group, atomic_read)
    }

    /// Obtains a shared reference to the [WriteNotifier] for a given topic and partition. Note that this notifier is shared across all consumer groups.
    pub fn get_write_notifier(&self, topic: &str, partition: u32) -> Result<Arc<WriteNotifier>, BrokerError> {
        Ok(self.get_or_create_partition_manager(topic, partition, |e| BrokerError::Io(e))?.write_notifier())
    }

    fn get_or_create_partition_manager<F, Err>(&self, topic: &str, partition: u32, map_error: F) -> Result<Arc<PartitionLogManager>, Err>
    where F: Fn(io::Error) -> Err
    {
        Ok(Arc::clone(match self.log_files.lock().unwrap().entry(LogKey::new(topic, partition)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let log_manager = PartitionLogManager::new(
                    &self.root_path,
                    topic, partition,
                    self.loop_timeout,
                    Arc::clone(&self.shutdown),
                );
                entry.insert(Arc::new(log_manager.map_err(map_error)?))
            }
        }))
    }

    /// Attempts shutting down the log manager gracefully (terminating all background threads, closing all file handles etc)
    pub fn shutdown(self) -> thread::Result<()> {
        self.shutdown.store(true, atomic::Ordering::Relaxed);

        let log_files = Arc::into_inner(self.log_files)
            .expect("LogManager::shutdown called, but other Arc clones of log_files still exist.")
            .into_inner()
            .expect("Mutex poisoned. Partition managers may be in an inconsistent state.");

        println!("Shutting down log manager... Waiting for {} partition log managers to shut down", log_files.len());

        log_files.into_iter()
            .map(|(_, partition_manager)|
                Arc::into_inner(partition_manager)
                    .expect("LogManager::shutdown called, but other Arc clones of this PartitionLogManager still exist.")
                    .shutdown()
            )
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
    write_notifier: Arc<WriteNotifier>,
    consumers: Mutex<HashMap<String, ConsumerManager>>,
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
            write_notifier: Arc::new(WriteNotifier::new()),
            shutdown,
            loop_timeout,
            consumers: Mutex::new(HashMap::new()),
        })
    }

    /// Atomically performs arbitrary write operations specified by the [AtomicWriteAction] with the guarantee that no other threads will be able to interleave
    /// any write operation to the same [RotatingAppendOnlyLog] as long as this action is running.
    /// # Errors
    /// - [BrokerError::Io] if an IO error occurs during the write operation
    /// - [BrokerError::Internal] if the writer thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn atomic_write(&self, atomic_write: impl AtomicWriteAction) -> Result<u64, BrokerError> {
        let (response_tx, response_rx) = oneshot::channel();

        let send_result = self.writer_tx.send(WriteRequest { atomic_write: Box::new(atomic_write), response_tx });
        if send_result.is_err() { return Err(new_worker_thread_disconnected_error()) }

        let offset = response_rx.blocking_recv().unwrap_or_else(|_| Err(new_worker_thread_disconnected_error()))?;
        self.write_notifier.notify_readers();

        Ok(offset)
    }

    /// Atomically performs arbitrary read operations specified by the [AtomicReadAction] with the guarantee that no other threads will be able to interleave
    /// any read operation to the same [RotatingLogReader] as long as this action is running.
    /// # Errors
    /// - [BrokerError::Io] if an IO error occurs during the read operation
    /// - [BrokerError::Internal] if the reader thread has been dropped. Should not happen, of course. If it does, rebooting the broker
    ///   is likely required.
    fn atomic_read(&self, consumer_group: String, atomic_action: impl AtomicReadAction) -> Result<IndexedRecord, BrokerError> {
        let atomic_action = Box::new(atomic_action);
        let mut consumers_guard = self.consumers.lock().unwrap();
        let consumer_manager = match consumers_guard.entry(consumer_group.clone()) {
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
        if send_result.is_err() { return Err(new_worker_thread_disconnected_error()) }

        response_rx.blocking_recv().unwrap_or_else(|_| Err(new_worker_thread_disconnected_error()))
    }

    /// Attempts shutting down the partition manager gracefully (terminating all background threads, closing all file handles etc)
    fn shutdown(self) -> thread::Result<()> {
        let consumers = self.consumers.into_inner().expect("Mutex poisoned. This partition manager may be in an inconsistent state.");
        println!("Shutting down partition under root path {}... Waiting for {} reader(s) and 1 writer to shut down.",
                 self.root_path, consumers.len());

        consumers.into_iter()
            .map(|(_, consumer_manager)| consumer_manager.reader_thread.join())
            .chain(iter::once(self.writer_thread.join()))
            .fold(Ok(()), |acc, result| if result.is_err() { result } else { acc })
    }

    fn write_notifier(&self) -> Arc<WriteNotifier> { Arc::clone(&self.write_notifier) }
}

/// This struct allows readers to be notified whenever a new write has occurred in the same topic/partition. This is helpful to implement
/// long polling efficiently.
pub struct WriteNotifier {
    cvar: Condvar,
    dummy_lock: Mutex<()>, // I need the Condvar only for waiting and notifying, there's no shared state behind it
}

impl WriteNotifier {
    fn new() -> Self {
        Self {
            cvar: Condvar::new(),
            dummy_lock: Mutex::new(()),
        }
    }

    /// Wakes up all threads that are currently waiting for new data to be written. Private as only the persistence layer is the one that knows when
    /// a write happened.
    fn notify_readers(&self) {
        self.cvar.notify_one(); // notify ALL because there might be multiple consumer groups waiting for new data to be written
    }

    /// Waits for a new write to happen on the partition associated to this [WriteNotifier], with the provided timeout.
    pub fn wait_new_write(&self, timeout: Duration) {
        let _ = self.cvar.wait_timeout(self.dummy_lock.lock().unwrap(), timeout).unwrap();
    }
}

impl Debug for PartitionLogManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("PartitionLogManager(topic={}, partition={})", self.topic, self.partition))
    }
}

trait MpscRequest<Res, Err> {
    fn response_tx(self) -> oneshot::Sender<Result<Res, Err>>;
}

struct WriteRequest {
    atomic_write: Box<dyn AtomicWriteAction>,
    response_tx: oneshot::Sender<Result<u64, BrokerError>>,
}

/// This trait allows to inject read external logic with the guarantee that the provided [RotatingLogReader] is only available on a single thread,
/// and no other thread can make any operation until [AtomicReadAction::read_from] has completed. This means that implementations of this trait can
/// make any number of accesses to the disk without worrying about another producer thread sending a read request in the middle and producing unexpected
/// outcomes. We opted for this design to decouple [LogManager] from the protocol layer. We wanted it to remain purely about low-level IO handling, and
/// not requiring any knowledge about the binary format. That way, only minimal changes (if any) would be required to this code even if we entirely changed
/// our approach for the binary format.
pub trait AtomicWriteAction : Send + 'static {
    fn write_to(&self, log: &mut RotatingAppendOnlyLog) -> Result<u64, BrokerError>;
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
            request.atomic_write.write_to(&mut self.log)
        }, loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<u64, BrokerError> for WriteRequest {
    fn response_tx(self) -> oneshot::Sender<Result<u64, BrokerError>> { self.response_tx }
}

struct ReadRequest {
    atomic_action: Box<dyn AtomicReadAction>,
    response_tx: oneshot::Sender<Result<IndexedRecord, BrokerError>>,
}

/// This trait allows to inject read external logic with the guarantee that the provided [RotatingLogReader] is only available on a single thread,
/// and no other thread can make any operation until [AtomicReadAction::read_from] has completed. This means that implementations of this trait can
/// make any number of accesses to the disk without worrying about another producer thread sending a read request in the middle and producing unexpected
/// outcomes. We opted for this design to decouple [LogManager] from the protocol layer. We wanted it to remain purely about low-level IO handling, and
/// not requiring any knowledge about the binary format. That way, only minimal changes (if any) would be required to this code even if we entirely changed
/// our approach for the binary format.
#[automock]
pub trait AtomicReadAction : Send + 'static {
    fn initialize(&self, root_path: &str) -> Result<RotatingLogReader, BrokerError>;

    fn read_from(&self, reader: &mut RotatingLogReader) -> Result<IndexedRecord, BrokerError>;
}

#[derive(Eq, PartialEq, Debug)]
pub struct IndexedRecord(pub u64, pub Vec<u8>);

/// Handles the resources and logic to continuously read from a rotating log file for a single partition.
struct PartitionLogReader {
    reader: RotatingLogReader,
    read_requests: Receiver<ReadRequest>,
    shutdown: Arc<AtomicBool>,
}

impl PartitionLogReader {
    fn start_read_loop(&mut self, loop_timeout: Duration, description: String) {
        start_mpsc_request_loop(&self.read_requests, |request| request.atomic_action.read_from(&mut self.reader),
            loop_timeout, &self.shutdown, description);
    }
}

impl MpscRequest<IndexedRecord, BrokerError> for ReadRequest {
    fn response_tx(self) -> oneshot::Sender<Result<IndexedRecord, BrokerError>> { self.response_tx }
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

fn new_worker_thread_disconnected_error() -> BrokerError {
    BrokerError::Internal(WORKER_THREAD_DISCONNECTED.to_owned())
}