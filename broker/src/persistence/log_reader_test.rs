use crate::persistence::log_reader::{LogReader, RotatingLogReader};
use assertor::{assert_that, EqualityAssertion};
use file_test_utils::{TempTestDir, TempTestFile};
use std::{fs, io};

const BASE_FILE_NAME: &'static str = "data";

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
#[should_panic(expected = "failed to fill whole buffer")]
fn test_log_reader_read_exact_eof() {
    let temp_file = TempTestFile::create();

    fs::write(temp_file.path(), "hello world!").unwrap();

    let mut log_reader = LogReader::open(temp_file.path()).unwrap();
    log_reader.read_exact(13).unwrap();
}

#[test]
fn test_rotating_log_reader_open_and_read_until_and_past_eof() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open(root_path, BASE_FILE_NAME, 3).unwrap();

    assert_read_bytes_are(log_reader.read(12), "hello world!");
    assert_read_bytes_are(log_reader.read(1), " ");
    assert_read_bytes_are(log_reader.read(8), "Nice to ");
    assert_read_bytes_are(log_reader.read(9), "meet you!");
    assert_read_bytes_are(log_reader.read(5), ""); // contrarily to the LogReader, this returns empty bytes, not an error
    assert_read_bytes_are(log_reader.read(10), "");
}

#[test]
fn test_rotating_log_reader_moves_to_next_file() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", "hello world! Nice to meet you!");
    write_to_file(&root_path, "data.00004", "It's beautiful around here!");

    let mut log_reader = RotatingLogReader::open(root_path, BASE_FILE_NAME, 3).unwrap();
    assert_read_bytes_are(log_reader.read(12), "hello world!");
    assert_read_bytes_are(log_reader.read(18), " Nice to meet you!");
    assert_read_bytes_are(log_reader.read(15), "It's beautiful ");
    assert_read_bytes_are(log_reader.read(12), "around here!");
}

#[test]
fn test_rotating_log_reader_seek_back_and_forth() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open(root_path, BASE_FILE_NAME, 3).unwrap();
    assert_read_bytes_are(log_reader.read(12), "hello world!");

    log_reader.seek(6).unwrap();
    assert_read_bytes_are(log_reader.read(12), "to meet you!");

    log_reader.seek(-18).unwrap();
    assert_read_bytes_are(log_reader.read(6), " Nice ");
}

#[test]
fn test_rotating_log_reader_seek_to_eof_and_rotate() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", "hello world! Nice to meet you!");
    write_to_file(&root_path, "data.00004", "It's beautiful around here!");

    let mut log_reader = RotatingLogReader::open(root_path, BASE_FILE_NAME, 3).unwrap();

    assert_read_bytes_are(log_reader.read(12), "hello world!");

    log_reader.seek(150).unwrap();
    assert_read_bytes_are(log_reader.read(14), "It's beautiful");
}

#[test]
#[should_panic]
fn test_rotating_log_reader_negative_seek_beyond_file_start() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", "hello world! Nice to meet you!");

    let mut log_reader = RotatingLogReader::open(root_path, BASE_FILE_NAME, 3).unwrap();

    log_reader.seek(-1).unwrap();
}

fn assert_read_bytes_are(result: io::Result<Vec<u8>>, expected: &str) {
    let actual: &str = &String::from_utf8(result.unwrap()).unwrap();
    assert_that!(actual).is_equal_to(expected);
}

fn write_to_file(root_path: &String, file_name: &str, content: &str) {
    fs::write(&format!("{root_path}{file_name}"), content).unwrap();
}