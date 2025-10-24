use std::io::{Cursor, Read};
use assertor::{assert_that, EqualityAssertion};
use crate::record::{deserialize_batch, read_batch_metadata, read_next_batch, serialize_batch, Header, Record, RecordBatch};

#[test]
fn test_record_batch_serialization_round_trip() {
    let batch = new_batch();
    let bytes = serialize_batch(batch.clone());
    let deserialized_batch = read_next_batch(&mut Cursor::new(bytes)).unwrap();
    assert_that!(deserialized_batch).is_equal_to(batch);
}

#[test]
#[should_panic(expected = "Expected exactly 82 bytes for compressed records but had 83")]
fn test_record_batch_deserialization_excess_bytes() {
    let batch = new_batch();
    let mut bytes = serialize_batch(batch);
    bytes.push(90);

    let mut reader = Cursor::new(bytes);
    let metadata = read_batch_metadata(&mut reader).unwrap();

    let mut remaining_bytes = Vec::new();
    reader.read_to_end(&mut remaining_bytes).unwrap();

    deserialize_batch(metadata, &remaining_bytes);
}

fn new_batch() -> RecordBatch {
    let base_timestamp = 12345678912;
    RecordBatch {
        protocol_version: 5,
        base_timestamp,
        records: vec![
            Record {
                key: None,
                value: String::from("value1"),
                timestamp: base_timestamp + 10520,
                headers: Vec::new(),
            },
            Record {
                key: Some(String::from("key2")),
                value: String::from("value2"),
                timestamp: base_timestamp + 10520230,
                headers: vec![
                    Header {
                        key: String::from("header_key_21"),
                        value: String::from("header_value_21"),
                    },
                    Header {
                        key: String::from("header_key_22"),
                        value: String::from("header_value_22"),
                    },
                ],
            },
        ]
    }
}

