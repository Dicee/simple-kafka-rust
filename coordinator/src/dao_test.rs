use super::*;
use assertor::{assert_that, EqualityAssertion, ResultAssertion};
use file_test_utils::TempTestFile;
use crate::dao::InvalidOffset::{NoDataWritten, TooLarge, TooSmall};

const TOPIC_NAME: &str = "topic";
const PARTITION: u32 = 36;
const CONSUMER_GROUP: &str = "consumer_group";

#[test]
fn test_create_topic() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic1 = Topic { name: String::from("topic1"), partition_count: 128};
    let topic2 = Topic { name: String::from("topic2"), partition_count: 64};

    dao.create_topic(&topic1.name, topic1.partition_count).unwrap();
    dao.create_topic(&topic2.name, topic2.partition_count).unwrap();

    assert_that!(dao.get_topic(&topic1.name).unwrap()).is_equal_to(topic1);
    assert_that!(dao.get_topic(&topic2.name).unwrap()).is_equal_to(topic2);
}

#[test]
fn test_create_topic_conflict() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 128};
    dao.create_topic(&topic.name, topic.partition_count).unwrap();

    assert_that!(dao.create_topic(&topic.name, 64)).has_err(Error::TopicAlreadyExists(topic.name.clone()));
}

#[test]
fn test_get_topic_does_not_exist() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.get_topic(&topic)).has_err(Error::TopicNotFound(topic));
}

#[test]
fn test_inc_write_offset_by() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    dao.create_topic(TOPIC_NAME, 64).unwrap();

    let inc1 = 5;
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, inc1).unwrap();
    // the first time, we increment by offset - 1 since 0 is the first offset
    assert_that!(dao.get_write_offset(TOPIC_NAME, PARTITION)).has_ok(Some(inc1 as u64 - 1));

    let inc2 = 22;
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, inc2).unwrap();
    assert_that!(dao.get_write_offset(TOPIC_NAME, PARTITION)).has_ok(Some((inc1 + inc2 - 1) as u64));
}

#[test]
fn test_inc_write_offset_by_topic_does_not_exist() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.inc_write_offset_by(TOPIC_NAME, PARTITION, 5)).has_err(Error::TopicNotFound(topic));
}

#[test]
fn test_inc_write_offset_by_out_of_range_partition() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    let invalid_partition = topic.partition_count + 1;
    assert_that!(dao.inc_write_offset_by(&topic.name, invalid_partition, 5))
        .has_err(Error::OutOfRangePartition { topic, invalid_partition });
}

#[test]
fn test_get_write_offset_no_offset_yet() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    assert_that!(dao.get_write_offset(&topic.name, PARTITION)).has_ok(None);
}

#[test]
fn test_get_write_offset_topic_does_not_exist() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.get_write_offset(&topic, 36)).has_err(Error::TopicNotFound(topic));
}

#[test]
fn test_get_write_offset_out_of_range_partition() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    let invalid_partition = topic.partition_count + 1;
    assert_that!(dao.get_write_offset(&topic.name, invalid_partition))
        .has_err(Error::OutOfRangePartition { topic, invalid_partition });
}

#[test]
fn test_ack_read_offset_no_offset_yet() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    dao.create_topic(TOPIC_NAME, 64).unwrap();

    let offset1 = 5;
    let offset2 = 22;
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, offset1 as u32).unwrap();
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION + 1, offset2 as u32).unwrap();

    dao.ack_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP, offset1 - 1).unwrap();
    dao.ack_read_offset(TOPIC_NAME, PARTITION + 1, CONSUMER_GROUP, offset2 - 1).unwrap();

    assert_that!(dao.get_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP)).has_ok(Some(offset1 - 1));
    assert_that!(dao.get_read_offset(TOPIC_NAME, PARTITION + 1, CONSUMER_GROUP)).has_ok(Some(offset2 - 1));
}

#[test]
fn test_ack_read_offset_overwrites_offset() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    dao.create_topic(TOPIC_NAME, 64).unwrap();

    let offset1 = 5;
    let offset2 = 22;
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, offset2 as u32).unwrap();

    dao.ack_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP, offset1).unwrap();
    assert_that!(dao.get_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP)).has_ok(Some(offset1));

    dao.ack_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP, offset2 - 2).unwrap();
    assert_that!(dao.get_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP)).has_ok(Some(offset2 - 2));
}

#[test]
fn test_ack_read_offset_topic_does_not_exist() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.ack_read_offset(&topic, PARTITION, CONSUMER_GROUP, 5)).has_err(Error::TopicNotFound(topic));
}

#[test]
fn test_ack_read_offset_out_of_range_partition() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    let invalid_partition = topic.partition_count + 1;
    assert_that!(dao.ack_read_offset(TOPIC_NAME, invalid_partition, CONSUMER_GROUP, 5))
        .has_err(Error::OutOfRangePartition { topic, invalid_partition });
}

#[test]
fn test_ack_read_offset_no_data_written() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    dao.create_topic(TOPIC_NAME, 64).unwrap();

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.ack_read_offset(&topic, PARTITION, CONSUMER_GROUP, 5)).has_err(InvalidReadOffset(NoDataWritten));
}

#[test]
fn test_ack_read_offset_exceeds_write_offset() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let offset = 5;
    dao.create_topic(TOPIC_NAME, 64).unwrap();
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, offset as u32).unwrap();

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.ack_read_offset(&topic, PARTITION, CONSUMER_GROUP, offset))
        .has_err(InvalidReadOffset(TooLarge { max_offset: 4, invalid_offset: offset }));
}

#[test]
fn test_ack_read_offset_less_than_read_offset() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let offset = 5;
    dao.create_topic(TOPIC_NAME, 64).unwrap();
    dao.inc_write_offset_by(TOPIC_NAME, PARTITION, offset as u32).unwrap();
    dao.ack_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP, offset - 1).unwrap();

    let topic = String::from(TOPIC_NAME);
    let invalid_offset = offset - 2;
    assert_that!(dao.ack_read_offset(&topic, PARTITION, CONSUMER_GROUP, invalid_offset))
        .has_err(InvalidReadOffset(TooSmall { min_offset: 4, invalid_offset }));
}


#[test]
fn test_get_read_offset_no_offset_yet() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    assert_that!(dao.get_read_offset(TOPIC_NAME, PARTITION, CONSUMER_GROUP)).has_ok(None);
}

#[test]
fn test_get_read_offset_topic_does_not_exist() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = String::from(TOPIC_NAME);
    assert_that!(dao.get_read_offset(&topic, PARTITION, CONSUMER_GROUP)).has_err(Error::TopicNotFound(topic));
}

#[test]
fn test_get_read_offset_out_of_range_partition() {
    let temp_file = TempTestFile::create();
    let dao = new_dao(&temp_file);

    let topic = Topic { name: String::from(TOPIC_NAME), partition_count: 64 };
    dao.create_topic(&topic.name, 64).unwrap();

    let invalid_partition = topic.partition_count + 1;
    assert_that!(dao.get_read_offset(&topic.name, invalid_partition, CONSUMER_GROUP))
        .has_err(Error::OutOfRangePartition { topic, invalid_partition });
}

fn new_dao(temp_file: &TempTestFile) -> MetadataAndStateDao {
    MetadataAndStateDao::new(&Path::new(temp_file.path())).unwrap()
}
