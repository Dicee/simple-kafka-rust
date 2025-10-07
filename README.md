# simple-kafka-rust
Pet project to learn about Rust and implement some recently learned implementation details of Kafka, on a small scale and with a very limited set of features.

## Learning objectives
- master and implement some distributed systems concepts and techniques employed by Kafka
- learn Rust to the point of reasonable proficiency, gaining the ability to implement fairly complex logic in a project with several crates/modules, proper testing etc
- leverage Docker and possibly Docker Compose to manage a set of heterogeneous containers working together (e.g. producers, consumers, brokers, a controller etc)

## Targeted features

### High priority
- create a topic
- subscribe to a topic
- send messages to a topic from a single producer (local)
- persist messages on disk
- consume messages from a topic from a fixed set of consumers (all local) in a consumer group
- start a broker process listening for requests
- implement a test producer and consumers to run repeatable test scenarios

### Medium priority
- support multiple producers (all local)
- support the dynamic addition and removal of consumers into a consumer group (all local)
- replicate the data across several brokers
- implement at-least-once and at-most-once modes

### Low priority
- implement batching at the producer side and in the broker to improve on IO performance
- implement filtering based on key-value headers on records

### Out-of-scope
The below features seem too difficult to implement for the amount of time I would like to dedicate to this project, and go beyond my learning objectives:
- transactional writes
- exactly-once semantic (requires transactional writes)
- non-local hosting of the containers
- adding or removing brokers, shrinking or growing the number of partitions
  - we'll assume a static number of partitions distributed on a static number of brokers, with a static number of replicas
  - and thus we won't bother with consistent hashing
- custom retention and expiry. We won't delete any data.