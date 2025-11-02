use super::{AtomicReadAction, AtomicWriteAction, IndexedRecord, LogManager, MockAtomicReadAction, RotatingAppendOnlyLog};
use crate::broker::Error as BrokerError;
use crate::persistence::log_reader::RotatingLogReader;
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use file_test_utils::{assert_file_has_content, TempTestDir};
use std::io::ErrorKind;
use std::thread;
use std::time::Duration;

const DEFAULT_LOOP_TIMEOUT: Duration = Duration::from_millis(5);
const GROUP1: &str = "group1";
const GROUP2: &str = "group2";

// We won't test rotation because this is an implementation detail of LogManager, and it's tested against RotatingAppendOnlyLog.
// As long as LogManager uses it, its correctness in terms of rotation is guaranteed.
#[test]
fn test_read_seek_and_write_to_several_topics_and_shutdown() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    write_to(&mut log_manager, "topic1", 0, "Hi sir!");
    write_to(&mut log_manager, "topic1", 0, " How are you");
    write_to(&mut log_manager, "topic1", 0, " on this fine morning?");

    write_to(&mut log_manager, "topic1", 1, "We're not");
    write_to(&mut log_manager, "topic1", 1, " quite as polite");
    write_to(&mut log_manager, "topic1", 1, " in this partition");
    write_to(&mut log_manager, "topic1", 1, " so get lost.");

    write_to(&mut log_manager, "topic2", 128, "Here we are");
    write_to(&mut log_manager, "topic2", 128, " civilized, sir. We do greet our guests");

    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "Hi sir!");
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP2.to_owned(), SingleRead(22)), "Hi sir! How are you on");

    assert_that!(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleSeek(9))).is_ok();
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(3)), "you");
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP2.to_owned(), SingleRead(5)), " this");

    assert_read_bytes_are(log_manager.atomic_read("topic1", 1, GROUP2.to_owned(), SingleRead(9)), "We're not");

    assert_that!(log_manager.atomic_read("topic2", 128, GROUP2.to_owned(), SingleSeek(12))).is_ok();
    assert_read_bytes_are(log_manager.atomic_read("topic2", 128, GROUP2.to_owned(), SingleRead(9)), "civilized");

    log_manager.shutdown().unwrap();

    // an extra check just to test the layout of the files
    assert_file_has_content(&format!("{}/topic1/partition=0000/00000.log", root_path), "Hi sir! How are you on this fine morning?");
    assert_file_has_content(&format!("{}/topic1/partition=0001/00000.log", root_path), "We're not quite as polite in this partition so get lost.");
    assert_file_has_content(&format!("{}/topic2/partition=0128/00000.log", root_path), "Here we are civilized, sir. We do greet our guests");
}

#[test]
fn test_read_at_eof_and_then_write() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());
    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    write_to(&mut log_manager, "topic1", 0, "Hi sir!");

    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "Hi sir!");
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "");

    write_to(&mut log_manager, "topic1", 0, "It's an honor");
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "It's an");

    log_manager.shutdown().unwrap();
}

// not yet using multiple threads as LogManager is not yet thread-safe due to mutations to a HashMap
#[test]
fn test_atomic_read() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());
    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    write_to(&mut log_manager, "topic1", 0, "Hi sir! It's an honor! I've been wishing to meet you since I heard about you");

    let mut read_action = MockAtomicReadAction::new();

    read_action.expect_initialize().returning(|root_path| Ok(RotatingLogReader::open_for_index(root_path.to_owned(), 0)?));
    read_action
        .expect_read_from()
        .returning(|log_reader| {
            let mut bytes = Vec::new();
            log_reader.seek(5)?;
            thread::sleep(Duration::from_millis(10));

            bytes.extend(log_reader.read(7)?); // "r! It's"
            log_reader.seek(3)?;
            thread::sleep(Duration::from_millis(100));

            bytes.extend(log_reader.read(4)?); // " hon"
            Ok(IndexedRecord(0, bytes))
        });

    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), read_action), "r! It's hon");
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "or! I'v");

    log_manager.shutdown().unwrap();
}

#[test]
fn test_read_before_writing_anything() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    // It's empty rather than failing because instantiating the LogManager creates a writer thread, which will create an empty file
    // even before anything has been written.
    assert_read_bytes_are(log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(7)), "");

    log_manager.shutdown().unwrap();
}

#[test]
fn test_read_past_eof() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);
    write_to(&mut log_manager, "topic1", 0, "Hi sir!");

    match log_manager.atomic_read("topic1", 0, GROUP1.to_owned(), SingleRead(50)) {
        Err(BrokerError::Io(e)) => assert_eq!(e.kind(), ErrorKind::UnexpectedEof),
        _ => unreachable!(),
    }
}

fn write_to(log_manager: &mut LogManager, topic: &str, partition: u32, content: &str) {
    log_manager.atomic_write(topic, partition, WriteAndCommit(0, content.as_bytes().to_vec())).unwrap();
}

fn assert_read_bytes_are(result: Result<IndexedRecord, BrokerError>, expected: &str) {
    let actual: &str = &String::from_utf8(result.unwrap().1).unwrap();
    assert_that!(actual).is_equal_to(expected);
}

struct WriteAndCommit(u64, Vec<u8>);

impl AtomicWriteAction for WriteAndCommit {
    fn write_to(&self, log: &mut RotatingAppendOnlyLog) -> Result<u64, BrokerError> {
        log.write_all_indexable(self.0, &self.1)?;
        log.flush()?;
        Ok(self.0)
    }
}

struct SingleRead(usize);

impl AtomicReadAction for SingleRead {
    fn initialize(&self, root_path: &str) -> Result<RotatingLogReader, BrokerError> {
        Ok(RotatingLogReader::open_for_index(root_path.to_owned(), 0)?)
    }

    fn read_from(&self, reader: &mut RotatingLogReader) -> Result<IndexedRecord, BrokerError> {
        Ok(IndexedRecord(0, reader.read(self.0)?))
    }
}

struct SingleSeek(i64);

impl AtomicReadAction for SingleSeek {
    fn initialize(&self, root_path: &str) -> Result<RotatingLogReader, BrokerError> {
        Ok(RotatingLogReader::open_for_index(root_path.to_owned(), 0)?)
    }
    
    fn read_from(&self, reader: &mut RotatingLogReader) -> Result<IndexedRecord, BrokerError> {
        reader.seek(self.0)?;
        Ok(IndexedRecord(0, vec![]))
    }
}


///! I don't normally test internal code (private, not exposed externally), but here error handling for some edge cases is likely impossible to test from
/// the public API because they can only happen if a programming error was made or a thread has crashed unexpectedly (which is likely also a programming
/// error). Some of those are possible to trigger with mocking but it would be unpractical because [LogManager] instantiates readers and writers dynamically.
/// I don't really want to have factories left and right just for testing. Thus, I'll test this handling by calling an internal API.
mod internal {
    use crate::persistence::{start_mpsc_request_loop, MpscRequest};
    use super::BrokerError;
    use assertor::{assert_that, EqualityAssertion};
    use ntest_timeout::timeout;
    use std::io::ErrorKind;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::Duration;
    use std::{io, thread};
    use tokio::sync::oneshot;

    #[test]
    #[timeout(300)]
    fn test_mpsc_loop_shutdown() {
        let loop_timeout = Duration::from_millis(150);

        let (tx, rx) = std::sync::mpsc::channel::<SimpleRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            start_mpsc_request_loop(&rx, |request| Ok(request.message.clone()), loop_timeout, &shutdown_clone, String::from("test loop"));
        });

        short_sleep(); // let some time for the loop to start

        end_loop(shutdown, handle);

        println!("{:?}", tx) // just to make sure the variable isn't dropped, which would close the write loop independently of the shutdown signal
    }

    #[test]
    #[timeout(300)]
    fn test_mpsc_loop_exits_if_sender_disconnects() {
        let loop_timeout = Duration::from_millis(150);

        let (tx, rx) = std::sync::mpsc::channel::<SimpleRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            start_mpsc_request_loop(
                &rx, |request| { Ok(format!("{} world", request.message)) },
                loop_timeout, &shutdown_clone, String::from("test loop")
            );
        });

        short_sleep(); // let some time for the loop to start

        drop(tx);
        handle.join().unwrap();
    }

    #[test]
    #[timeout(1000)]
    fn test_mpsc_loop_survives_oneshot_disconnection() {
        let loop_timeout = Duration::from_millis(150);

        let (tx, rx) = std::sync::mpsc::channel::<SimpleRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            start_mpsc_request_loop(
                &rx,
                |request| {
                    short_sleep(); // give some time to the main thread to drop the receiver
                    Ok(format!("{} world", request.message))
                },
                loop_timeout,
                &shutdown_clone,
                String::from("test loop")
            );
        });

        short_sleep(); // let some time for the loop to start

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(SimpleRequest { response_tx, message: String::from("Hello") }).unwrap();

        drop(response_rx);

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(SimpleRequest { response_tx, message: String::from("Goodbye") }).unwrap();
        assert_that!(response_rx.blocking_recv().unwrap().unwrap()).is_equal_to(String::from("Goodbye world"));

        end_loop(shutdown, handle);
    }

    #[test]
    #[timeout(300)]
    fn test_mpsc_loop_handler_error() {
        let loop_timeout = Duration::from_millis(150);

        let (tx, rx) = std::sync::mpsc::channel::<SimpleRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            start_mpsc_request_loop(
                &rx,
                |_| Err(BrokerError::Io(io::Error::new(ErrorKind::UnexpectedEof, "Ohhhhh snap!"))),
                loop_timeout,
                &shutdown_clone,
                String::from("test loop")
            );
        });

        short_sleep(); // let some time for the loop to start

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(SimpleRequest { response_tx, message: String::from("Goodbye") }).unwrap();

        match response_rx.blocking_recv().unwrap() {
            Err(BrokerError::Io(io_err)) => assert_that!(io_err.kind()).is_equal_to(ErrorKind::UnexpectedEof),
            _ => unreachable!(),
        }

        end_loop(shutdown, handle);
    }

    fn end_loop<T>(shutdown: Arc<AtomicBool>, handle: JoinHandle<T>) {
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().unwrap();
    }

    fn short_sleep() {
        thread::sleep(Duration::from_millis(50));
    }

    #[derive(Debug)]
    struct SimpleRequest {
        message: String,
        response_tx: oneshot::Sender<Result<String, BrokerError>>,
    }

    impl MpscRequest<String, BrokerError> for SimpleRequest {
        fn response_tx(self) -> oneshot::Sender<Result<String, BrokerError>> { self.response_tx }
    }
}