use assertor::{assert_that, EqualityAssertion, ResultAssertion};
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
        let mut index_writer = new_index_writer(&temp_dir, "00000.log");
        index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
        index_writer.ack_bytes_written(2 * MAX_INDEX_GAP + 10, 2).unwrap();

        index_writer.ack_rotation(2011, 3).unwrap();

        index_writer.ack_bytes_written(3 * MAX_INDEX_GAP, 4).unwrap(); // gap not large enough, ignored
        index_writer.ack_bytes_written(3 * MAX_INDEX_GAP + 11, 5).unwrap();
    }

    assert_index_file_contains(&temp_dir.resolve("00000.index"), vec![
        (MAX_INDEX_GAP, 1),
        (2 * MAX_INDEX_GAP + 10, 2),
        (2 * MAX_INDEX_GAP + 11, 3), // the first index outside of the index file is always written at the end
    ]);

    assert_index_file_contains(&temp_dir.resolve("02011.index"), vec![(3 * MAX_INDEX_GAP + 11, 5)]);
}

#[test]
#[should_panic(expected = "Indices must monotonically increase, but tried to write 17 while the last written index is 1000")]
fn test_writer_ack_rotation_index_smaller_than_last_written() {
    let temp_dir = TempTestDir::create();

    let mut index_writer = new_index_writer(&temp_dir, "0000.log");
    index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
    index_writer.ack_rotation(17, 2).unwrap();
}

#[test]
fn test_writer_flush() {
    let temp_dir = TempTestDir::create();
    let mut index_writer = new_index_writer(&temp_dir, "00000.log");

    index_writer.ack_bytes_written(MAX_INDEX_GAP, 1).unwrap();
    assert_index_file_contains(&temp_dir.resolve("00000.index"), vec![]);

    index_writer.flush().unwrap();
    assert_index_file_contains(&temp_dir.resolve("00000.index"), vec![(MAX_INDEX_GAP, 1)]);
}

fn assert_index_file_contains(path: &Path, rows: Vec<(u64, u64)>) {
    assert_that!(parse_index_file(path).expect(&format!("Failed to parse index file {path:?}")))
        .is_equal_to(rows);
}

#[test]
fn test_lookup_no_index_file() {
    let temp_dir = TempTestDir::create();
    assert_that!(look_up(temp_dir.path(), 17)).has_ok(None);
}

#[test]
fn test_lookup() {
    let temp_dir = TempTestDir::create();

    let mut index_writer = new_index_writer(&temp_dir, "00000.log");
    index_writer.ack_bytes_written(1500, 1).unwrap();
    index_writer.ack_bytes_written(2700, 2).unwrap();

    index_writer.ack_rotation(3009, 3).unwrap();
    index_writer.ack_bytes_written(4015, 4).unwrap();
    index_writer.ack_bytes_written(5078, 5).unwrap();

    index_writer.ack_rotation(6000, 6).unwrap();
    index_writer.ack_bytes_written(7200, 7).unwrap();

    index_writer.ack_rotation(8500, 8).unwrap();
    index_writer.flush().unwrap();

    // one test case for each possible position with the above files, except exact index rows which I'm testing only once with 2700,
    // and same thing for exact start index (tested once with 3009)
    assert_look_up_returns(&temp_dir, 0, "00000.log", 0);
    assert_look_up_returns(&temp_dir, 17, "00000.log", 0);
    assert_look_up_returns(&temp_dir, 1700, "00000.log", 1);
    assert_look_up_returns(&temp_dir, 2700, "00000.log", 2);
    assert_look_up_returns(&temp_dir, 3009, "03009.log", 0);
    assert_look_up_returns(&temp_dir, 4000, "03009.log", 0);
    assert_look_up_returns(&temp_dir, 5000, "03009.log", 4);
    assert_look_up_returns(&temp_dir, 5090, "03009.log", 5);
    assert_look_up_returns(&temp_dir, 6700, "06000.log", 0);
    assert_look_up_returns(&temp_dir, 7500, "06000.log", 7);
    assert_look_up_returns(&temp_dir, 8800, "08500.log", 0);
}

fn assert_look_up_returns(temp_dir: &TempTestDir, index: u64, expected_log_file: &str, expected_byte_offset: u64) {
    assert_that!(look_up(temp_dir.path(), index)).has_ok(Some(IndexLookupResult {
        log_file_path: temp_dir.resolve(expected_log_file),
        byte_offset: expected_byte_offset
    }));
}

fn new_index_writer(temp_dir: &TempTestDir, log_file_path: &str) -> BinaryLogIndexWriter {
    let index_writer = BinaryLogIndexWriter::open_for_log_file(&temp_dir.resolve(log_file_path)).unwrap();
    index_writer
}