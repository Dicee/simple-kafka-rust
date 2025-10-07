use super::LogManager;
use crate::test_utils::{assert_file_has_content, TempTestDir};

// We won't test rotation because this is an implementation detail of LogManager, and it's tested against RotatingAppendOnlyLog.
// As long as LogManager uses it, its correctness in terms of rotation is guaranteed.
#[test]
fn test_writing_to_several_topics_and_drop() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path());

    {
        let mut log_manager = LogManager::new(root_path.clone());

        write_to(&mut log_manager, "topic1", 0, "Hi sir!");
        write_to(&mut log_manager, "topic1", 0, " How are you");
        write_to(&mut log_manager, "topic1", 0, " on this fine morning?");

        write_to(&mut log_manager, "topic1", 1, "We're not");
        write_to(&mut log_manager, "topic1", 1, " quite as polite");
        write_to(&mut log_manager, "topic1", 1, " in this partition");
        write_to(&mut log_manager, "topic1", 1, " so get lost.");

        write_to(&mut log_manager, "topic2", 128, "Here we are");
        write_to(&mut log_manager, "topic2", 128, " civilized, sir. We do greet our guests");
    }

    assert_file_has_content(&format!("{}/topic1/partition=0000/data.00000", root_path), "Hi sir! How are you on this fine morning?");
    assert_file_has_content(&format!("{}/topic1/partition=0001/data.00000", root_path), "We're not quite as polite in this partition so get lost.");
    assert_file_has_content(&format!("{}/topic2/partition=0128/data.00000", root_path), "Here we are civilized, sir. We do greet our guests");
}

#[test]
fn test_commit() {
    let temp_dir = TempTestDir::create();
    let root_path = format!("{}/topics", temp_dir.path());
    let mut log_manager = LogManager::new(root_path.clone());

    let topic = "topic1";
    let partition = 598;
    let expected_path = format!("{}/{topic}/partition=0{partition}/data.00000", root_path);

    let content = "Hi sir!";
    write_to(&mut log_manager, topic, partition, content);
    assert_file_has_content(&expected_path, "");

    log_manager.commit(topic, partition).unwrap();
    assert_file_has_content(&expected_path, content);
}

fn write_to(log_manager: &mut LogManager, topic: &str, partition: u32, content: &str) {
    log_manager.write(topic, partition, content.as_bytes()).unwrap();
}