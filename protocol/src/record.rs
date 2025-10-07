

// notes:
// - limit timestamp delta to 24 hours (maximum 27 bits)
// - write var int/long byte by byte with the first bit telling to continue or not. The int value is written in base 128 (ZigZag)
// - Kafka uses Snappy framing format, not raw Snappy blocks.
// - to simplify, always use Snappy compression (good enough compression, works well for repeated values, fast)

pub struct Record {
    length: u32,
    timestamp: u64,
    offset: u32,
    key: String,
    value: String,
    headers: Vec<Header>,
}

pub struct Header {
    key: String,
    value: String,
}

// pub struct Record {
//     length: u32,
//     timestamp_delta: u64, // delta compared to the base timestamp in the batch the record belongs to
//     offset_delta: u32, // delta compared to the base timestamp in the batch the record belongs to
//     key_length: u32,
//     key: [u8],
//     value_length: u32,
//     value: [u8],
//     headers_count: u32,
//     headers: [Header]
// }
//
// pub struct Header {
//     key_length: u32,
//     key: [u8],
//     value_length: u32,
//     value: [u8],
// }