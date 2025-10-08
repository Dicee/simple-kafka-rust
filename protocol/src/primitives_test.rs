use super::*;

use assertor::{assert_that, EqualityAssertion};
use rand::{Rng, SeedableRng};
use rand::distr::{Uniform};
use rand::rngs::StdRng;

#[test]
fn test_encode_var_long_small_values() {
    for i in 0u64..=5000 {
        assert_that!(decode_var_long(&encode_var_long(i))).is_equal_to(i);
    }
}

#[test]
fn test_decode_var_long_max_value() {
    let x =  encode_var_long(u64::MAX);
    assert_that!(decode_var_long(&x)).is_equal_to(u64::MAX);
}

#[test]
#[should_panic(expected = "A variable size long value cannot be encoded on more than 10 bytes, but 11 had MSB = 1")]
fn test_decode_var_long_too_many_bytes() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_long(u64::MAX));
    *bytes.last_mut().unwrap() |= 0x80;
    bytes.push(1);

    decode_var_long(&bytes);
}

#[test]
#[should_panic(expected = "Value will overflow if decoded")]
fn test_decode_var_long_overflow() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_long(u64::MAX));
    *bytes.last_mut().unwrap() += 1;

    decode_var_long(&bytes);
}

#[test]
#[should_panic(expected = "Last byte 160 is not a stop byte")]
fn test_decode_var_long_no_stop_byte() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_long(32));
    *bytes.last_mut().unwrap() |= 0x80;

    decode_var_long(&bytes);
}

#[test]
#[should_panic(expected = "Byte with index 1 and value 22 is not the last byte of the slice but is a stop byte")]
fn test_decode_var_long_non_final_byte_is_stop_byte() {
    let mut bytes = Vec::new();
    // has to be greater than 2^16 to have at least 3 bytes in the encoded form
    bytes.extend(&encode_var_long(68423));

    let second_byte = &mut bytes[1];
    *second_byte &= !(1 << 7);

    decode_var_long(&bytes);
}

#[test]
fn test_encode_var_long_random_values() {
    // we'll use a seed to make the test deterministic
    let rng = StdRng::seed_from_u64(42);
    let distr = Uniform::new(0, u64::MAX).unwrap();

    for i in rng.sample_iter(distr).take(10_000) {
        assert_that!(decode_var_long(&encode_var_long(i))).is_equal_to(i);
    }
}

#[test]
#[should_panic(expected = "At least one byte is required")]
fn test_decode_var_long_empty_bytes() {
    decode_var_long(&Vec::<u8>::new());
}

#[test]
fn test_decode_next_var_long_several_values() {
    let values: Vec<u64> = vec!(0, 10, 968, 0xFFFFF, 0xFFFFFEDCBA);
    let mut bytes = Vec::new();

    for value in &values {
        bytes.extend(encode_var_long(*value));
    }

    let mut decoded_values = Vec::new();
    let mut remaining_bytes: &[u8] = &bytes;

    for _ in 0..values.len() {
        let (rest, decoded_value) = decode_next_var_long(remaining_bytes);
        remaining_bytes = rest;
        decoded_values.push(decoded_value);
    }

    assert_that!(remaining_bytes.len()).is_equal_to(0);
    assert_that!(decoded_values).is_equal_to(values);
}

#[test]
#[should_panic(expected = "Expected a variable size value but could not find a stop byte (MSB = 0)")]
fn test_decode_next_var_long_no_stop_byte() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_long(185));
    *bytes.last_mut().unwrap() = 0x80;

    decode_next_var_long(&bytes);
}

#[test]
#[should_panic(expected = "A variable size long value cannot be encoded on more than 10 bytes, but 11 had MSB = 1")]
fn test_decode_next_var_long_too_many_bytes() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_long(u64::MAX));
    *bytes.last_mut().unwrap() |= 0x80;
    bytes.push(1);

    decode_next_var_long(&bytes);
}

#[test]
fn test_encode_var_int_small_values() {
    for i in 0u32..=5000 {
        assert_that!(decode_var_int(&encode_var_int(i))).is_equal_to(i);
    }
}

#[test]
fn test_encode_var_int_random_values() {
    // we'll use a seed to make the test deterministic
    let rng = StdRng::seed_from_u64(42);
    let distr = Uniform::new(0, u32::MAX).unwrap();

    for i in rng.sample_iter(distr).take(10_000) {
        let val = encode_var_int(i);
        assert_that!(decode_var_int(&val)).is_equal_to(i);
    }
}

#[test]
#[should_panic(expected = "At least one byte is required")]
fn test_decode_var_int_empty_bytes() {
    decode_var_int(&Vec::<u8>::new());
}

#[test]
fn test_decode_var_int_max_value() {
    assert_that!(decode_var_int(&encode_var_int(u32::MAX))).is_equal_to(u32::MAX);
}

#[test]
#[should_panic(expected = "A variable size int value cannot be encoded on more than 5 bytes, but 6 had MSB = 1")]
fn test_decode_var_int_too_many_bytes() {
    let mut bytes = Vec::new();
    let vec = encode_var_int(u32::MAX);
    bytes.extend(&vec);
    *bytes.last_mut().unwrap() |= 0x80;
    bytes.push(1);

    decode_var_int(&bytes);
}

#[test]
#[should_panic(expected = "Value will overflow if decoded")]
fn test_decode_var_int_overflow() {
    let mut bytes = Vec::new();
    let vec = encode_var_int(u32::MAX);
    bytes.extend(&vec);
    *bytes.last_mut().unwrap() += 1;

    decode_var_int(&bytes);
}

#[test]
#[should_panic(expected = "Last byte 160 is not a stop byte")]
fn test_decode_var_int_no_stop_byte() {
    let mut bytes = Vec::new();
    bytes.extend(encode_var_int(32));
    *bytes.last_mut().unwrap() |= 0x80;

    decode_var_int(&bytes);
}

#[test]
#[should_panic(expected = "Byte with index 1 and value 22 is not the last byte of the slice but is a stop byte")]
fn test_decode_var_int_non_final_byte_is_stop_byte() {
    let mut bytes = Vec::new();
    // has to be greater than 2^16 to have at least 3 bytes in the encoded form
    bytes.extend(&encode_var_int(68423));

    let second_byte = &mut bytes[1];
    *second_byte &= !(1 << 7);

    decode_var_int(&bytes);
}

#[test]
fn test_decode_next_var_int_several_values() {
    let values: Vec<u32> = vec!(0, 10, 968, 0xFFFFF, 0xFFFFFEDC);
    let mut bytes = Vec::new();

    for value in &values {
        bytes.extend(encode_var_int(*value));
    }

    let mut decoded_values = Vec::new();
    let mut remaining_bytes: &[u8] = &bytes;

    for _ in 0..values.len() {
        let (rest, decoded_value) = decode_next_var_int(remaining_bytes);
        remaining_bytes = rest;
        decoded_values.push(decoded_value);
    }

    assert_that!(remaining_bytes.len()).is_equal_to(0);
    assert_that!(decoded_values).is_equal_to(values);
}
