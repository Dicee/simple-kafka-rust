use std::io::{Cursor, Read, Write};
use crate::primitives::*;

#[cfg(test)]
#[path="record_test.rs"]
mod record_test;

/// Format inspired (but simplified) from [here](https://kafka.apache.org/documentation/#recordbatch)
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct RecordBatch {
    pub protocol_version: u8,
    pub base_timestamp: u64,
    // should normally add a limit of number of records and total byte size but to simplify the code I'll skip that. Not important for a learning project.
    pub records: Vec<Record>,
}

/// Format inspired (but simplified) from [here](https://kafka.apache.org/documentation/#record)
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Record {
    pub timestamp: u64,
    pub key: Option<String>,
    pub value: String,
    pub headers: Vec<Header>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: String,
}

pub fn serialize_batch(batch: RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(batch.protocol_version);
    bytes.extend(&batch.base_timestamp.to_le_bytes());

    let vec = &batch.records;
    bytes.extend(vec_len_as_u32(vec).to_le_bytes());

    let mut record_bytes = Vec::new();
    for (i, record) in batch.records.iter().enumerate() {
        let payload_bytes = serialize_record(record, u32::try_from(i + 1).unwrap(), batch.base_timestamp);
        record_bytes.extend(encode_var_int(vec_len_as_u32(&payload_bytes)));
        record_bytes.extend(payload_bytes);
    }

    let compressed_record_bytes = snappy_compress(record_bytes);
    bytes.extend(vec_len_as_u32(&compressed_record_bytes).to_le_bytes());
    bytes.extend(compressed_record_bytes);

    bytes
}

fn serialize_record(record: &Record, offset_delta: u32, base_timestamp: u64) -> Vec<u8> {
    if record.timestamp < base_timestamp {
        panic!(
            "The base timestamp is expected to be the minimum of all timestamps in the batch but found {}, which is smaller \
            than {base_timestamp}. We do not support negative delta encoding.", record.timestamp
        );
    }

    let mut payload_bytes = Vec::new();
    payload_bytes.extend(encode_var_long(record.timestamp - base_timestamp));
    payload_bytes.extend(encode_var_int(offset_delta));

    serialize_option_string(&record.key, &mut payload_bytes);
    serialize_string(&record.value, &mut payload_bytes);
    payload_bytes.extend(encode_var_int(vec_len_as_u32(&record.headers)));

    for header in record.headers.iter() {
        serialize_header(header, &mut payload_bytes);
    }

    payload_bytes
}

fn serialize_header(header: &Header, bytes: &mut Vec<u8>) {
    serialize_string(&header.key, bytes);
    serialize_string(&header.value, bytes);
}

fn serialize_option_string(s: &Option<String>, bytes: &mut Vec<u8>) {
    match s {
        Some(v) => serialize_string(v, bytes),
        None => bytes.extend(encode_var_int(0)),
    }
}

fn serialize_string(value: &str, bytes: &mut Vec<u8>) {
    let value_bytes = value.as_bytes();
    bytes.extend(encode_var_int(u32::try_from(value_bytes.len()).unwrap()));
    bytes.extend(value_bytes);
}

fn snappy_compress(bytes: Vec<u8>) -> Vec<u8> {
    let mut compressed_bytes = Vec::new();

    {
        let mut encoder = snap::write::FrameEncoder::new(&mut compressed_bytes);
        encoder.write_all(&bytes).unwrap();
    }

    compressed_bytes
}

pub fn deserialize_batch(bytes: Vec<u8>) -> RecordBatch {
    let protocol_version = bytes[0];
    let base_timestamp = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
    let record_count = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
    let record_byte_count = u32::from_le_bytes(bytes[13..17].try_into().unwrap());

    let compressed_record_bytes = &bytes[17..];
    let actual_record_byte_count = compressed_record_bytes.len();
    if actual_record_byte_count as u32 != record_byte_count {
        panic!("Expected exactly {record_byte_count} bytes for compressed records but had {actual_record_byte_count}");
    }

    let record_bytes = snappy_decompress(compressed_record_bytes);

    let mut remaining_record_bytes: &[u8] = &record_bytes;
    let mut records = Vec::new();

    for _ in 0..record_count {
        let (rest, record) = read_next_record(remaining_record_bytes, base_timestamp);

        remaining_record_bytes = rest;
        records.push(record);
    }

    if !remaining_record_bytes.is_empty() {
        panic!("Parsing completed with an excess of {} unprocessed bytes", remaining_record_bytes.len());
    }

    RecordBatch { protocol_version, base_timestamp, records }
}

fn read_next_record(bytes: &[u8], base_timestamp: u64) -> (&[u8], Record) {
    let (bytes, record_length) = decode_next_var_int(bytes);

    let initial_slice_length = bytes.len();
    let (bytes, timestamp_delta) = decode_next_var_long(bytes);
    let (bytes, _) = decode_next_var_int(bytes); // TODO: do we need the offset?

    let (bytes, key_length) = decode_next_var_int(bytes);
    let (bytes, key) = read_string_of_length(bytes, key_length);

    let (bytes, value_length) = decode_next_var_int(bytes);
    let (bytes, value) = read_string_of_length(bytes, value_length);

    let (bytes, headers_count) = decode_next_var_int(bytes);
    let mut headers = Vec::with_capacity(headers_count as usize);

    let mut remaining_bytes = bytes;

    for _ in 0..headers_count {
        let (rest, header) = read_next_header(remaining_bytes);
        remaining_bytes = rest;
        headers.push(header);
    }

    let actual_record_length = u32::try_from(initial_slice_length - remaining_bytes.len()).unwrap();
    if actual_record_length != record_length {
        panic!("Expected exactly {record_length} bytes for record, but found {actual_record_length}");
    }

    (remaining_bytes, Record {
        timestamp: base_timestamp + timestamp_delta,
        key: if key_length == 0 { None } else { Some(key) },
        value,
        headers,
    })
}

fn read_next_header(bytes: &[u8]) -> (&[u8], Header) {
    let (bytes, key_length) = decode_next_var_int(bytes);
    let (bytes, key) = read_string_of_length(bytes, key_length);

    let (bytes, value_length) = decode_next_var_int(bytes);
    let (bytes, value) = read_string_of_length(bytes, value_length);

    (bytes, Header { key, value })
}

fn snappy_decompress(bytes: &[u8]) -> Vec<u8> {
    let mut decompressed_bytes = Vec::new();

    {
        let mut decoder = snap::read::FrameDecoder::new(Cursor::new(&bytes));
        decoder.read_to_end(&mut decompressed_bytes).unwrap();
    }

    decompressed_bytes
}

fn read_string_of_length(bytes: &[u8], key_length: u32) -> (&[u8], String) {
    let s = str::from_utf8(&bytes[..key_length as usize]).unwrap().to_string();
    (&bytes[key_length as usize..], s)
}

// I want collection sizes to have a known number of bytes in the binary. u32 is more than enough to store the number of records in a batch,
// and it's unreasonable to expect the size to be any larger.
fn vec_len_as_u32<T>(vec: &[T]) -> u32 {
    u32::try_from(vec.len()).unwrap()
}
