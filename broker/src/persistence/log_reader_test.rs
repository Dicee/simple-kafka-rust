use crate::persistence::log_reader::{LogReader, RotatingLogReader};
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use file_test_utils::{TempTestDir, TempTestFile};
use std::{fs, io};
use crate::persistence::get_log_path;
use crate::persistence::indexing::{LogIndexWriter};

#[test]
fn test_log_reader_open_and_read_until_eof() {
    let temp_file = TempTestFile::create();

    fs::write(temp_file.path(), "hello world! Nice to meet you!").unwrap();

    let mut log_reader = LogReader::open(temp_file.path()).unwrap();

    assert_read_bytes_are(log_reader.read_exact(12), "hello world!");
    assert_read_bytes_are(log_reader.read_exact(1), " ");
    assert_read_bytes_are(log_reader.read_exact(8), "Nice to ");
    assert_read_bytes_are(log_reader.read_exact(9), "meet you!");
}

#[test]
fn test_log_reader_open_at_offset() {
    let temp_file = TempTestFile::create();

    fs::write(temp_file.path(), "hello world! Nice to meet you!").unwrap();

    let mut log_reader = LogReader::open_at_offset(temp_file.path(), 10).unwrap();

    assert_read_bytes_are(log_reader.read_exact(12), "d! Nice to m");
    assert_read_bytes_are(log_reader.read_exact(1), "e");
    assert_read_bytes_are(log_reader.read_exact(7), "et you!");
}

#[test]
fn test_log_reader_open_at_offset_more_than_bytes_in_file() {
    let temp_file = TempTestFile::create();

    fs::write(temp_file.path(), "hello world! Nice to meet you!").unwrap();

    let mut log_reader = LogReader::open_at_offset(temp_file.path(), 100).unwrap();
    match log_reader.read_exact(1) {
        Err(e) => assert_that!(e.kind()).is_equal_to(io::ErrorKind::UnexpectedEof),
        _ => unreachable!(),
    }
}

#[test]
#[should_panic(expected = "failed to fill whole buffer")]
fn test_log_reader_read_exact_eof() {
    let temp_file = TempTestFile::create();

    fs::write(temp_file.path(), "hello world!").unwrap();

    let mut log_reader = LogReader::open(temp_file.path()).unwrap();
    log_reader.read_exact(13).unwrap();
}

#[test]
fn test_log_reader_read_u64_le() {
    let temp_file = TempTestFile::create();

    let first: u64 = 678;
    let second: u64 = 9843;

    let mut bytes = Vec::new();
    bytes.extend(first.to_le_bytes());
    bytes.extend(second.to_le_bytes());
    fs::write(temp_file.path(), &bytes).unwrap();

    let mut log_reader = LogReader::open(temp_file.path()).unwrap();

    assert_that!(log_reader.read_u64_le()).has_ok(first);
    assert_that!(log_reader.read_u64_le()).has_ok(second);
}

#[test]
fn test_log_reader_read_u64_le_nof_enough_bytes() {
    let temp_file = TempTestFile::create();
    fs::write(temp_file.path(), &12u64.to_le_bytes()[0..7]).unwrap();

    let mut log_reader = LogReader::open(temp_file.path()).unwrap();
    match log_reader.read_u64_le() {
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
        Ok(_) => unreachable!(),
    }
}

#[test]
fn test_rotating_log_reader_open_and_read_until_and_past_eof() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "00003.log", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();

    assert_read_bytes_are(log_reader.read(12), "hello world!");
    assert_read_bytes_are(log_reader.read(1), " ");
    assert_read_bytes_are(log_reader.read(8), "Nice to ");
    assert_read_bytes_are(log_reader.read(9), "meet you!");
    assert_read_bytes_are(log_reader.read(5), ""); // contrarily to the LogReader, this returns empty bytes, not an error
    assert_read_bytes_are(log_reader.read(10), "");
}

#[test]
fn test_rotating_log_reader_open_at_offset() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());
    fs::create_dir_all(root_path.clone()).unwrap();

    let file_name = "00003.log";
    write_to_file(&root_path, file_name, "hello world! Nice to meet you!");

    let log_path = temp_dir.resolve(&root_path).join(file_name);
    let mut log_reader = RotatingLogReader::open_at_offset(log_path, 10).unwrap();

    assert_read_bytes_are(log_reader.read(12), "d! Nice to m");
    assert_read_bytes_are(log_reader.read(1), "e");
    assert_read_bytes_are(log_reader.read(7), "et you!");
    assert_read_bytes_are(log_reader.read(1), "");
}

#[test]
fn test_rotating_log_reader_open_at_offset_more_than_bytes_in_file() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());
    fs::create_dir_all(root_path.clone()).unwrap();

    let file_name = "00003.log";
    write_to_file(&root_path, file_name, "hello world! Nice to meet you!");

    let log_path = temp_dir.resolve(&root_path).join(file_name);
    let mut log_reader = RotatingLogReader::open_at_offset(log_path, 100).unwrap();
    assert_read_bytes_are(log_reader.read(1), "");
}

#[test]
fn test_rotating_log_reader_read_moves_to_next_file() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());
    fs::create_dir_all(root_path.clone()).unwrap();

    let content1 = "hello world! Nice to meet you!";
    write_to_file(&root_path, "00003.log", content1);
    write_to_file(&root_path, "00004.log", "It's beautiful around here!");

    ensure_index_file_contains_reference_to_next(&root_path, 3, content1.len() as u64);

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();
    assert_read_bytes_are(log_reader.read(12), "hello world!");
    assert_read_bytes_are(log_reader.read(18), " Nice to meet you!");
    assert_read_bytes_are(log_reader.read(15), "It's beautiful ");
    assert_read_bytes_are(log_reader.read(12), "around here!");
}

#[test]
fn test_rotating_log_reader_read_u64_le_moves_to_next_file() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());
    fs::create_dir_all(root_path.clone()).unwrap();

    let mut content1 = Vec::new();
    content1.extend(123u64.to_le_bytes());
    content1.extend(456u64.to_le_bytes());

    fs::write(&format!("{root_path}00003.log"), &content1).unwrap();
    fs::write(&format!("{root_path}00004.log"), &789u64.to_le_bytes()).unwrap();

    ensure_index_file_contains_reference_to_next(&root_path, 3, content1.len() as u64);

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();
    assert_that!(log_reader.read_u64_le()).has_ok(Some(123));
    assert_that!(log_reader.read_u64_le()).has_ok(Some(456));
    assert_that!(log_reader.read_u64_le()).has_ok(Some(789));
    assert_that!(log_reader.read_u64_le()).has_ok(None);
}

#[test]
fn test_rotating_log_reader_seek_back_and_forth() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "00003.log", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();
    assert_read_bytes_are(log_reader.read(12), "hello world!");

    log_reader.seek(6).unwrap();
    assert_read_bytes_are(log_reader.read(12), "to meet you!");

    log_reader.seek(-18).unwrap();
    assert_read_bytes_are(log_reader.read(6), " Nice ");
}

#[test]
fn test_rotating_log_reader_seek_to_eof_and_rotate() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());

    fs::create_dir_all(root_path.clone()).unwrap();
    let content1 = "hello world! Nice to meet you!";
    write_to_file(&root_path, "00003.log", content1);
    write_to_file(&root_path, "00004.log", "It's beautiful around here!");

    ensure_index_file_contains_reference_to_next(&root_path, 3, content1.len() as u64);

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();

    assert_read_bytes_are(log_reader.read(12), "hello world!");

    log_reader.seek(150).unwrap();
    assert_read_bytes_are(log_reader.read(14), "It's beautiful");

    log_reader.seek(150).unwrap();
    assert_read_bytes_are(log_reader.read(14), "");
}

#[test]
#[should_panic]
fn test_rotating_log_reader_negative_seek_beyond_file_start() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path_as_str());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "00003.log", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open_for_index(root_path, 3).unwrap();

    log_reader.seek(-1).unwrap();
}

fn write_to_file(root_path: &String, file_name: &str, content: &str) {
    fs::write(&format!("{root_path}{file_name}"), content).unwrap();
}

fn ensure_index_file_contains_reference_to_next(root_path: &str, index: u64, final_byte_offset: u64) {
    LogIndexWriter::open_for_log_file(&get_log_path(&root_path, index)).unwrap()
        .ack_rotation(index + 1, final_byte_offset).unwrap();
}

fn assert_read_bytes_are(result: io::Result<Vec<u8>>, expected: &str) {
    let actual: &str = &String::from_utf8(result.unwrap()).unwrap();
    assert_that!(actual).is_equal_to(expected);
}
