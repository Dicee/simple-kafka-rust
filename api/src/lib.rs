//! This crate contains the producer and consumer clients that constitute the user-facing API of our small Kafka project.

pub mod producer;
pub mod consumer;

pub mod common;

#[cfg(test)]
mod mock_utils;

mod semaphore;