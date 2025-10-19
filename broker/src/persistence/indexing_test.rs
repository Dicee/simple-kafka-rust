use std::io::BufRead;
use assertor::{assert_that, EqualityAssertion};
use file_test_utils::TempTestDir;
use super::*;

#[test]
fn test_writer_open_new_index_file_log_file_with_extension() {
    let temp_dir = TempTestDir::create();
    let index_writer = new_index_writer(&temp_dir, "0000.log");

    assert_that!(index_writer.last_written_index).is_equal_to(0);
    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![])
}

#[test]
fn test_writer_open_new_index_file_log_file_without_extension() {
    let temp_dir = TempTestDir::create();
    let index_writer = new_index_writer(&temp_dir, "0000");

    assert_that!(index_writer.last_written_index).is_equal_to(0);
    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![])
}

#[test]
fn test_writer_open_existing_index_file() {
    let temp_dir = TempTestDir::create();
    let last_index = 3 * MAX_INDEX_GAP;

    {
        let mut index_writer = new_index_writer(&temp_dir, "0000.log");
        index_writer.ack_bytes_written(MAX_INDEX_GAP, 5).unwrap();
        index_writer.ack_bytes_written(2 * MAX_INDEX_GAP, 10).unwrap();
        index_writer.ack_bytes_written(last_index, 15).unwrap();
    }

    let index_writer = new_index_writer(&temp_dir, "0000.log");
    assert_that!(index_writer.last_written_index).is_equal_to(last_index);
    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![
        (MAX_INDEX_GAP, 5),
        (2 * MAX_INDEX_GAP, 10),
        (last_index, 15),
    ])
}

#[test]
#[should_panic(expected = "Log file names should be composed solely of a u64 index (first index in the log) and an optional extension but was: hello.log")]
fn test_writer_open_invalid_file_name_with_extension() {
    let temp_dir = TempTestDir::create();
    new_index_writer(&temp_dir, "hello.log");
}

#[test]
#[should_panic(expected = "Log file names should be composed solely of a u64 index (first index in the log) and an optional extension but was: hello")]
fn test_writer_open_invalid_file_name_without_extension() {
    let temp_dir = TempTestDir::create();
    new_index_writer(&temp_dir, "hello");
}

#[test]
fn test_writer_ack_bytes_written() {
    let temp_dir = TempTestDir::create();

    {
        let mut index_writer = new_index_writer(&temp_dir, "0000.log");
        index_writer.ack_bytes_written(17, 1).unwrap(); // small gap, ignored
        index_writer.ack_bytes_written(MAX_INDEX_GAP / 2, 2).unwrap(); // small gap, ignored
        index_writer.ack_bytes_written(MAX_INDEX_GAP + 1, 3).unwrap();
        index_writer.ack_bytes_written(MAX_INDEX_GAP + 2, 3).unwrap(); // small gap, ignored
        index_writer.ack_bytes_written(2 * MAX_INDEX_GAP + 6, 4).unwrap();
        index_writer.ack_bytes_written(3 * MAX_INDEX_GAP + 5, 5).unwrap(); // just one short of being written
        index_writer.ack_bytes_written(5 * MAX_INDEX_GAP, 6).unwrap();
    }

    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![
        (MAX_INDEX_GAP + 1, 3),
        (2 * MAX_INDEX_GAP + 6, 4),
        (5 * MAX_INDEX_GAP, 6),
    ]);
}

#[test]
#[should_panic(expected = "Indices must monotonically increase, but tried to write 17 while the last written index is 1000")]
fn test_writer_ack_bytes_written_index_smaller_than_last_written() {
    let temp_dir = TempTestDir::create();

    let mut index_writer = new_index_writer(&temp_dir, "0000.log");
    index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
    index_writer.ack_bytes_written(17, 2).unwrap();
}

#[test]
fn test_writer_ack_rotation() {
    let temp_dir = TempTestDir::create();

    {
        let mut index_writer = new_index_writer(&temp_dir, "0000.log");
        index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
        index_writer.ack_bytes_written(2 * MAX_INDEX_GAP + 10, 2).unwrap();

        index_writer.ack_rotation(&temp_dir.resolve("2011.log")).unwrap();

        index_writer.ack_bytes_written(3 * MAX_INDEX_GAP, 3).unwrap(); // gap not large enough, ignored
        index_writer.ack_bytes_written(3 * MAX_INDEX_GAP + 11, 4).unwrap();
    }

    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![
        (MAX_INDEX_GAP, 1),
        (2 * MAX_INDEX_GAP + 10, 2),
    ]);

    assert_index_file_contains(&temp_dir.resolve("2011.index"), vec![(3 * MAX_INDEX_GAP + 11, 4)]);
}

#[test]
#[should_panic(expected = "Indices must monotonically increase, but tried to write 17 while the last written index is 1000")]
fn test_writer_ack_rotation_index_smaller_than_last_written() {
    let temp_dir = TempTestDir::create();

    let mut index_writer = new_index_writer(&temp_dir, "0000.log");
    index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
    index_writer.ack_rotation(&temp_dir.resolve("0017.log")).unwrap();
}

#[test]
fn test_writer_flush() {
    let temp_dir = TempTestDir::create();
    let mut index_writer = new_index_writer(&temp_dir, "0000.log");

    index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![]);

    index_writer.flush().unwrap();
    assert_index_file_contains(&temp_dir.resolve("0000.index"), vec![(MAX_INDEX_GAP, 1)]);
}

fn assert_index_file_contains(path: &Path, rows: Vec<(u64, u64)>) {
    assert_that!(parse_index_file(path)).is_equal_to(rows);
}

fn parse_index_file(log_file_path: &Path) -> Vec<(u64, u64)> {
    let mut rows = Vec::new();
    let mut reader = BufReader::new(File::open(log_file_path).unwrap());
    let mut buf = [0u8; 8];

    while reader.fill_buf().unwrap().len() > 0 {
        reader.read_exact(&mut buf).unwrap();
        let index = u64::from_le_bytes(buf);

        reader.read_exact(&mut buf).unwrap();
        let byte_offset = u64::from_le_bytes(buf);

        rows.push((index, byte_offset));
    }

    rows
}

fn new_index_writer(temp_dir: &TempTestDir, log_file_path: &str) -> BinaryLogIndexWriter {
    let index_writer = BinaryLogIndexWriter::open_for_log_file(&temp_dir.resolve(log_file_path)).unwrap();
    index_writer
}

