//! Like in Kafka, we use a single binary format that producers, brokers and consumers can  understand without any need
//! for conversion or copies, to enhance performance. This crate implements this protocol, based on a simplified version
//! of the Kafka binary format, which can be found [here](https://kafka.apache.org/documentation/#recordbatch).

pub mod record;
mod primitives;

