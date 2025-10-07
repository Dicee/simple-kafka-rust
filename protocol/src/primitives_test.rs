use super::*;

use assertor::{assert_that, EqualityAssertion};
use rand::{Rng, SeedableRng};
use rand::distr::{Alphanumeric, Uniform};
use rand::rngs::StdRng;

#[test]
fn test_encode_varlong_small_values() {
    for i in 128u64..=5000 {
        assert_that!(decode_var_long(&encode_var_long(i))).is_equal_to(i);
    }
}

#[test]
fn test_encode_varlong_random_values() {
    // we'll use a seed to make the test deterministic
    let rng = StdRng::seed_from_u64(42);
    let distr = Uniform::new(0, u64::MAX).unwrap();

    for i in rng.sample_iter(distr).take(10_000) {
        assert_that!(decode_var_long(&encode_var_long(i))).is_equal_to(i);
    }
}

#[test]
fn test_encode_varint_small_values() {
    for i in 128u32..=5000 {
        assert_that!(decode_var_int(&encode_var_int(i))).is_equal_to(i);
    }
}

#[test]
fn test_encode_varint_random_values() {
    // we'll use a seed to make the test deterministic
    let rng = StdRng::seed_from_u64(42);
    let distr = Uniform::new(0, u32::MAX).unwrap();

    for i in rng.sample_iter(distr).take(10_000) {
        let val = encode_var_int(i);
        assert_that!(decode_var_int(&val)).is_equal_to(i);
    }
}