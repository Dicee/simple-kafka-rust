use std::fs;
use std::path::Path;
use super::{AppendOnlyLog, LogFile, RotatingAppendOnlyLog};
use crate::test_utils::{assert_file_has_content, TempTestDir, TempTestFile};

const BASE_FILE_NAME: &'static str = "data";
const DEFAULT_MAX_BYTES: u64 = 30; // can fit two "Hello world!" but not more

#[test]
fn test_open_and_write_to_new_file() {
    let temp_file = TempTestFile::create();
    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();

    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.flush().unwrap();

    temp_file.assert_has_content(content);
}

#[test]
fn test_open_and_write_to_new_file_with_dirs_to_create() {
    let temp_file = TempTestFile::create_within("does/not/exist");

    let parent_dir = Path::new(temp_file.path()).parent().unwrap();
    assert!(!parent_dir.exists(), "Parent directory {parent_dir:?} should not exist yet");

    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();
    assert!(parent_dir.exists(), "Parent directory {parent_dir:?} should now exist");

    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.flush().unwrap();

    temp_file.assert_has_content(content);
}

#[test]
fn test_open_and_write_to_existing_file() {
    let temp_file = TempTestFile::create_with_content("Hey bro\n");
    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();

    let content = "How are you doing mate?";
    log.write(content.as_bytes()).unwrap();
    log.flush().unwrap();

    temp_file.assert_has_content("Hey bro\nHow are you doing mate?");
}

#[test]
fn test_flush() {
    let temp_file = TempTestFile::create();
    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();

    let content = "How are you doing mate?";
    log.write(content.as_bytes()).unwrap();

    temp_file.assert_has_content("");

    log.flush().unwrap();
    temp_file.assert_has_content(content);
}

#[test]
fn test_drop() {
    let temp_file = TempTestFile::create();
    let content = "How are you doing mate?";

    {
        let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();
        log.write(content.as_bytes()).unwrap();
        temp_file.assert_has_content("");
    }

    temp_file.assert_has_content(content);
}

#[test]
fn test_open_new_rotated_log() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    // just to prove we ignore non-data files
    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "trash_file", "whatever");
    fs::create_dir(format!("{root_path}trash_dir")).unwrap();

    let mut log = new_rotated_log(root_path);
    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.flush().unwrap();

    assert_file_has_content(&format!("{}data.00000", log.root_path), content);
}

#[test]
fn test_open_existing_rotated_log() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003","3");
    write_to_file(&root_path, "data.00004", "4");

    // just to prove we ignore non-data files
    write_to_file(&root_path, "trash_file", "whatever");
    fs::create_dir(format!("{root_path}trash_dir")).unwrap();

    let mut log = new_rotated_log(root_path);
    log.write("Hello world!".as_bytes()).unwrap();
    log.flush().unwrap();

    assert_file_has_content(&format!("{}data.00003", log.root_path), "3");
    assert_file_has_content(&format!("{}data.00004", log.root_path), "4Hello world!");
}

#[test]
fn test_open_rotated_log_already_at_max_bytes() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    let existing_file_path = format!("{root_path}data.00003");
    let existing_content = "Woooow this is already too long buddy!";

    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&existing_file_path, existing_content).unwrap();

    let mut log = new_rotated_log(root_path);

    let new_content = "Hello world!";
    log.write(new_content.as_bytes()).unwrap();
    log.flush().unwrap();

    assert_file_has_content(&existing_file_path, existing_content);
    assert_file_has_content(&format!("{}data.00004", log.root_path), new_content);
}

#[test]
#[should_panic(expected = "Invalid file name data00003, it should have at least one . character")]
fn test_open_rotated_malformed_index_no_dot() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    let existing_file_path = format!("{root_path}data00003");

    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&existing_file_path, "Yay!").unwrap();

    new_rotated_log(root_path);
}

#[test]
#[should_panic(expected = "Invalid file name data.00$03, failed to parse the index number")]
fn test_open_rotated_malformed_index_NaN() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    let existing_file_path = format!("{root_path}data.00$03");

    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&existing_file_path, "Yay!").unwrap();

    new_rotated_log(root_path);
}

#[test]
fn test_rotate() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    let mut log = new_rotated_log(root_path);

    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();

    temp_dir.assert_exactly_contains_files(&vec![
        "my-topic/partition=12/data.00000".to_string(),
        "my-topic/partition=12/data.00001".to_string(),
        "my-topic/partition=12/data.00002".to_string(),
    ]);

    // those two should already be flushed
    assert_file_has_content(&format!("{}data.00000", log.root_path), "Hello world!Hello world!");
    assert_file_has_content(&format!("{}data.00001", log.root_path), "Hello world!Hello world!");

    log.flush().unwrap();
    assert_file_has_content(&format!("{}data.00002", log.root_path), "Hello world!");
}

#[test]
#[should_panic(expected = "Next rotated file should not already exist, but did")]
fn test_rotate_while_next_file_exists() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());

    let content = "1234";
    fs::create_dir_all(root_path.clone()).unwrap();
    write_to_file(&root_path, "data.00003", content);

    let mut log = RotatingAppendOnlyLog::open_latest(root_path, BASE_FILE_NAME, content.len() as u64 + 1).unwrap();
    write_to_file(&log.root_path, "data.00004", content);

    log.write(&[55, 56]).unwrap(); // triggers a rotation, which should fail because the file with index 4 is already present
}

#[test]
fn test_drop_rotated_log() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let expected_path = format!("{root_path}data.00000");

    let content = "How are you doing mate?";

    {
        let mut log = new_rotated_log(root_path);
        log.write(content.as_bytes()).unwrap();
        assert_file_has_content(&expected_path, "");
    }

    assert_file_has_content(&expected_path, content);
}

fn write_to_file(root_path: &String, file_name: &str, content: &str) {
    fs::write(&format!("{root_path}{file_name}"), content).unwrap();
}

fn new_rotated_log(root_path: String) -> RotatingAppendOnlyLog {
    RotatingAppendOnlyLog::open_latest(root_path, BASE_FILE_NAME, DEFAULT_MAX_BYTES).unwrap()
}
