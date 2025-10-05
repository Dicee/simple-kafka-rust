use crate::append_only_log::AppendOnlyLog;
use crate::test_utils::TempTestFile;

#[test]
fn test_open_and_write_to_new_file() {
    let temp_file = TempTestFile::create();
    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();

    let content = "Hello world!";
    log.write(content.as_bytes()).unwrap();
    log.close().unwrap();

    temp_file.assert_has_content(content);
}

#[test]
fn test_open_and_write_to_existing_file() {
    let temp_file = TempTestFile::create_with_content("Hey bro\n");
    let mut log = AppendOnlyLog::open(temp_file.path()).unwrap();

    let content = "How are you doing mate?";
    log.write(content.as_bytes()).unwrap();
    log.close().unwrap();

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