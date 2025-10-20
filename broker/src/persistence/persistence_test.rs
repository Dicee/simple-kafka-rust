use super::{LogManager, ReadError};
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use file_test_utils::{assert_file_has_content, TempTestDir};
use std::io::ErrorKind;
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

    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "Hi sir!");
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP2.to_owned(), 22), "Hi sir! How are you on");

    assert_that!(log_manager.seek("topic1", 0, GROUP1.to_owned(), 9)).is_ok();
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 3), "you");
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP2.to_owned(), 5), " this");

    assert_read_bytes_are(log_manager.read("topic1", 1, GROUP2.to_owned(), 9), "We're not");

    assert_that!(log_manager.seek("topic2", 128, GROUP2.to_owned(), 12)).is_ok();
    assert_read_bytes_are(log_manager.read("topic2", 128, GROUP2.to_owned(), 9), "civilized");

    log_manager.shutdown().unwrap();

    // an extra check just to test the layout of the files
    assert_file_has_content(&format!("{}/topic1/partition=0000/00000.log", root_path), "Hi sir! How are you on this fine morning?");
    assert_file_has_content(&format!("{}/topic1/partition=0001/00000.log", root_path), "We're not quite as polite in this partition so get lost.");
    assert_file_has_content(&format!("{}/topic2/partition=0128/00000.log", root_path), "Here we are civilized, sir. We do greet our guests");
}

#[test]
fn test_write_without_flushing() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    log_manager.write("topic1", 0, 0, b"Hi").unwrap();
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 2), "");

    log_manager.write_and_commit("topic1", 0, 1, b" boyz").unwrap();
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "Hi boyz");

    log_manager.shutdown().unwrap();
}

#[test]
fn test_read_at_eof_and_then_write() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());
    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    write_to(&mut log_manager, "topic1", 0, "Hi sir!");

    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "Hi sir!");
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "");

    write_to(&mut log_manager, "topic1", 0, "It's an honor");
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "It's an");

    log_manager.shutdown().unwrap();
}

#[test]
fn test_read_before_writing_anything() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);

    // It's empty rather than failing because instantiating the LogManager creates a writer thread, which will create an empty file
    // even before anything has been written.
    assert_read_bytes_are(log_manager.read("topic1", 0, GROUP1.to_owned(), 7), "");

    log_manager.shutdown().unwrap();
}

#[test]
fn test_read_past_eof() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path_as_str());

    let mut log_manager = LogManager::new_with_loop_timeout(root_path.clone(), DEFAULT_LOOP_TIMEOUT);
    write_to(&mut log_manager, "topic1", 0, "Hi sir!");

    match log_manager.read("topic1", 0, GROUP1.to_owned(), 50) {
        Err(ReadError::Io(e)) => assert_eq!(e.kind(), ErrorKind::UnexpectedEof),
        _ => unreachable!(),
    }
}

fn write_to(log_manager: &mut LogManager, topic: &str, partition: u32, content: &str) {
    log_manager.write_and_commit(topic, partition, 0, content.as_bytes()).unwrap();
}

fn assert_read_bytes_are(result: Result<Vec<u8>, ReadError>, expected: &str) {
    let actual: &str = &String::from_utf8(result.unwrap()).unwrap();
    assert_that!(actual).is_equal_to(expected);
}

///! I don't normally test internal code (private, not exposed externally), but here error handling for some edge cases is likely impossible to test from
/// the public API because they can only happen if a programming error was made or a thread has crashed unexpectedly (which is likely also a programming
/// error). Some of those are possible to trigger with mocking but it would be unpractical because [LogManager] instantiates readers and writers dynamically.
/// I don't really want to have factories left and right just for testing. Thus, I'll test this handling by calling an internal API.
mod internal {
    use crate::persistence::{start_mpsc_request_loop, MpscRequest, ReadError};
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
                |_| Err(ReadError::Io(io::Error::new(ErrorKind::UnexpectedEof, "Ohhhhh snap!"))),
                loop_timeout,
                &shutdown_clone,
                String::from("test loop")
            );
        });

        short_sleep(); // let some time for the loop to start

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(SimpleRequest { response_tx, message: String::from("Goodbye") }).unwrap();

        match response_rx.blocking_recv().unwrap() {
            Err(ReadError::Io(io_err)) => assert_that!(io_err.kind()).is_equal_to(ErrorKind::UnexpectedEof),
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
        response_tx: oneshot::Sender<Result<String, ReadError>>,
    }

    impl MpscRequest<String, ReadError> for SimpleRequest {
        fn response_tx(self) -> oneshot::Sender<Result<String, ReadError>> { self.response_tx }
    }
}