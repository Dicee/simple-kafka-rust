FROM rust:slim-bookworm AS build-stage


RUN mkdir simple-kafka-rust
COPY api simple-kafka-rust/api
COPY broker simple-kafka-rust/broker
COPY client_utils simple-kafka-rust/client_utils
COPY coordinator simple-kafka-rust/coordinator
COPY file_test_utils simple-kafka-rust/file_test_utils
COPY protocol simple-kafka-rust/protocol
COPY Cargo.lock simple-kafka-rust
COPY Cargo.toml simple-kafka-rust

RUN cd simple-kafka-rust && cargo build -p coordinator --release

FROM debian:bookworm-slim AS final-stage

# needed for the health check
RUN apt-get update && apt-get install -y git curl

COPY --from=build-stage /simple-kafka-rust/target/release/coordinator /usr/local/bin/coordinator

RUN mkdir /var/lib/simple-kafka-rust 

ENTRYPOINT ["/usr/local/bin/coordinator"]
CMD ["--host", "0.0.0.0", "--port", "5000", "--db-path", "/var/lib/simple-kafka-rust/coordinator.db"]
