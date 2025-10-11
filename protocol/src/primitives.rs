#[cfg(test)]
#[path="primitives_test.rs"]
mod primitives_test;

/// We encode integers with variable length to compress records. The encoding proceeds byte by byte, where each byte's
/// most significant bit (MSB) is 1 if there is a next byte needed to encode the value, or 0 otherwise. The other 7 bits are
/// used to encode the value itself, splitting it into base 128 chunks. The first byte represents the least significant
/// bits of the input integer, and the next bytes represent increasingly more significant bits.
///
/// Note that this encoding degrades space usage for large values (since one in 8 every bits is encoding overhead),
/// however we expect to encode mostly small values since we will encode things like offset and timestamp deltas. The
/// delta between two timestamps in the same batch should usually not exceed a few minutes, and a batch shouldn't contain
/// more than a few hundred records in general. Especially in the case of timestamps, we will get huge savings compared
/// to storing a full [u64] every time.
pub fn encode_var_long(n: u64) -> Vec<u8> {
    if n == 0 {
        return vec![0];
    }

    let mut bytes = Vec::new();
    let mut rest = n;

    while rest > 0 {
        let mut byte = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest > 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
    }

    bytes
}

/// Same format as [encode_var_long] but with [u32] as input
pub fn encode_var_int(n: u32) -> Vec<u8> {
    encode_var_long(n as u64)
}

macro_rules! impl_decode_next_var_sized {
    (
        $doc:literal,
        $fn_name:ident,
        $ret_ty:ty,
        $type_name:literal,
        $decode_fn:ident
    ) => {
        #[doc=$doc]
        pub fn $fn_name(bytes: &[u8]) -> (&[u8], $ret_ty) {
            let stop_byte_index = bytes.iter().position(|b| b & 0x80 == 0)
                .expect("Expected a variable size value but could not find a stop byte (MSB = 0)");

            let bytes_count = stop_byte_index + 1;
            (&bytes[bytes_count..], $decode_fn(&bytes[0..bytes_count]))
        }
    };
}

macro_rules! impl_decode_var_sized {
    (
        $doc:literal,
        $fn_name:ident, $ret_ty:ty,
        $max_bytes:literal,
        $max_last_byte_value:literal,
        $type_name:literal
    ) => {
        #[doc=$doc]
        pub fn $fn_name(bytes: &[u8]) -> $ret_ty {
            if bytes.is_empty() {
                panic!("At least one byte is required to encode a variable size {}", $type_name);
            }

            let bytes_count = bytes.len();
            if bytes_count > $max_bytes {
                panic!("A variable size {} value cannot be encoded on more than {} bytes, but {bytes_count} had MSB = 1",
                    $type_name, $max_bytes);
            }

            if bytes_count == $max_bytes && bytes[bytes.len() - 1] > $max_last_byte_value {
                panic!("Value will overflow if decoded")
            }

            let mut result = 0 as $ret_ty;
            for (i, byte) in bytes.iter().enumerate() {
                let is_stop_byte = *byte & 0x80 == 0;

                if i == bytes_count - 1 && !is_stop_byte {
                    panic!("Last byte {byte} is not a stop byte")
                } else if i < bytes_count - 1 && is_stop_byte {
                    panic!("Byte with index {i} and value {byte} is not the last byte of the slice but is a stop byte")
                }

                let n = *byte & 0x7f;
                // the cast is safe because we panic earlier when the value will overflow
                result += (n as $ret_ty) << (i * 7);
            }

            result
        }
    };
}

impl_decode_next_var_sized!(
    "Reads within the slice of bytes until it finds a stopping byte (MSB = 0) and returns the remaining slice along with the
    decoded leading bytes representing a long value.",
    decode_next_var_long, // Function name
    u64,                  // Decoded return type
    "long",               // The type's name
    decode_var_long       // The function to call to decode the bytes
);

impl_decode_next_var_sized!(
    "Reads within the slice of bytes until it finds a stopping byte (MSB = 0) and returns the remaining slice along with the
    decoded leading bytes representing an int value.",
    decode_next_var_int, // Function name
    u32,                 // Decoded return type
    "int",               // The type's name
    decode_var_int       // The function to call to decode the bytes
);

impl_decode_var_sized!(
    "See [decode_var_long] to understand the format",
    decode_var_long, // Function name
    u64,             // Return type
    10,              // Max bytes
    1,               // Max last byte value
    "long"           // Type name for errors
);

impl_decode_var_sized!(
    "See [decode_var_int] to understand the format",
    decode_var_int, // Function name
    u32,            // Return type
    5,              // Max bytes
    15,             // Max last byte value
    "int"           // Type name for errors
);
