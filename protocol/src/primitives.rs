#[cfg(test)]
#[path="primitives_test.rs"]
mod primitives_test;

/// We encode integers with variable length to compress records. The encoding proceeds byte by byte, where each byte's
/// most significant bit is 1 if there is a next byte needed to encode the value, or 0 otherwise. The other 7 bits are
/// used to encode the value itself, splitting it into base 128 chunks. The first byte represents the least significant
/// bits of the input integer, and the next bytes represent increasingly more significant bits.
///
/// Note that this encoding degrades space usage for large values (since one in 8 every bits is encoding overhead),
/// however we expect to encode mostly small values since we will encode things like offset and timestamp deltas. The
/// delta between two timestamps in the same batch should usually not exceed a few minutes, and a batch shouldn't contain
/// more than a few hundred records in general. Especially in the case of timestamps, we will get huge savings compared
/// to storing a full [u64] every time.
pub fn encode_var_long(n: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut rest = n;

    while rest > 0 {
        let mut byte = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest > 0 {
            byte = byte | 0x80;
        }
        bytes.push(byte);
    }

    bytes
}

/// See [encode_var_long] to understand the format
pub fn decode_var_long(bytes: &[u8]) -> u64 {
    let mut result = 0u64;

    for (i, byte) in bytes.iter().enumerate() {
        let n = byte & 0x7f;
        result += (n as u64) << (i * 7);
    }

    result
}

/// Same format as [encode_var_long] but with [u32] as input
pub fn encode_var_int(n: u32) -> Vec<u8> {
    encode_var_long(n as u64)
}

/// See [encode_var_long] to understand the format
pub fn decode_var_int(bytes: &[u8]) -> u32 {
    decode_var_long(bytes) as u32
}
