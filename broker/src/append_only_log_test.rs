use std::fs;
use std::path::Path;
use crate::append_only_log::{AppendOnlyLog, LogFile, RotatingAppendOnlyLog};
use crate::test_utils::{assert_file_has_content, TempTestDir, TempTestFile};

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
fn test_rotate() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let base_file_name = String::from("data.txt");

    let mut log = RotatingAppendOnlyLog::open(root_path, base_file_name, 3, 30).unwrap();

    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();
    log.write(content.as_bytes()).unwrap();

    temp_dir.assert_exactly_contains_files(&vec![
        "my-topic/partition=12/data.txt.00003".to_string(),
        "my-topic/partition=12/data.txt.00004".to_string(),
        "my-topic/partition=12/data.txt.00005".to_string(),
    ]);

    // those two should already be flushed
    assert_file_has_content(&format!("{}data.txt.00003", log.root_path), "Hello world!Hello world!");
    assert_file_has_content(&format!("{}data.txt.00004", log.root_path), "Hello world!Hello world!");

    log.flush().unwrap();
    assert_file_has_content(&format!("{}data.txt.00005", log.root_path), "Hello world!");
}

#[test]
fn test_open_rotated_log_already_at_max_bytes() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let base_file_name = String::from("data.txt");

    let existing_file_path = format!("{root_path}data.txt.00003");
    let existing_content = "Woooow this is already too long buddy!";

    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&existing_file_path, existing_content).unwrap();

    let mut log = RotatingAppendOnlyLog::open(root_path, base_file_name, 3, 30).unwrap();

    let new_content = "Hello world!";
    log.write(new_content.as_bytes()).unwrap();
    log.flush().unwrap();

    assert_file_has_content(&existing_file_path, existing_content);
    assert_file_has_content(&format!("{}data.txt.00004", log.root_path), new_content);
}

#[test]
#[should_panic(expected = "Next rotated file should not already exist, but did")]
fn test_open_rotated_log_already_at_max_bytes_but_next_file_exists() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let base_file_name = String::from("data.txt");

    let content = "Woooow this is already too long buddy!";
    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&format!("{root_path}data.txt.00003"), content).unwrap();
    fs::write(&format!("{root_path}data.txt.00004"), content).unwrap();

    let mut log = RotatingAppendOnlyLog::open(root_path, base_file_name, 3, 30).unwrap();
    log.write(&[55]).unwrap();
}

#[test]
#[should_panic(expected = "Next rotated file should not already exist, but did")]
fn test_rotate_while_next_file_exists() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let base_file_name = String::from("data.txt");

    let content = "Woooow this is already too long buddy!";
    fs::create_dir_all(root_path.clone()).unwrap();
    fs::write(&format!("{root_path}data.txt.00004"), content).unwrap();

    let mut log = RotatingAppendOnlyLog::open(root_path, base_file_name, 3, 5).unwrap();
    log.write("1234".as_bytes()).unwrap();
    log.write(&[55]).unwrap(); // triggers a rotation, which should fail because the file with index 4 is already present
}

#[test]
fn test_drop_simple_log() {
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
fn test_drop_rotated_log() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/my-topic/partition=12/", temp_dir.path());
    let base_file_name = String::from("data.txt");
    let expected_path = format!("{root_path}data.txt.00000");

    let content = "How are you doing mate?";

    {
        let mut log = RotatingAppendOnlyLog::open(root_path, base_file_name, 0, 30).unwrap();
        log.write(content.as_bytes()).unwrap();
        assert_file_has_content(&expected_path, "");
    }

    assert_file_has_content(&expected_path, content);
}