# TODO

## Features

- add an API on the broker to initialize a reader to the proper offset. This is in case a consumer dies and then reconnects, without having acknowledged the
  records it had read. This allows the client to rewind the stream to where it left off.

## Improvements

- return fine-grained, strongly typed deserializable errors from my HTTP clients for API 4xx
- add proper logging

## Optimizations

- process several batches together on the broker write path, waiting (a limited amount of time) to receive a few messages from the channel and write them all at once
- avoid negative seeks and reading batch metadata twice (however since these are small seeks it's almost certain that they happen fully in memory, and do not
  have a high cost)
- use async IO rather than threads with blocking operations (e.g. Tokio's `BufReader/BufWriter`)

## Scalability

- change the multithreading model in the broker as having one thread per topic/partition for writes, and one per topic/partition/consumer group is a simple model
  to ensure atomicity, but wouldn't be scalable to a large number of topics/partitions, or would require spreading them across a very large number of brokers

## Bugs

- we might need to use `read` rather than `read_exact` to implement `read_u64_le` because if we're unlucky the write buffer might be flush while writing the offset,
  before all bytes of the offset have been written, and the reader might see this data and fail to read a `u64`.

# Done

## Improvements

- consider having writes to the `LogManager` concurrent by having one thread per log file, or a bounded pool paired with a collection of locks (one per file)
- ~~consider replacing panicking with custom error types in the protocol's ser/de~~ I think that's the right handling after all
- use GET for read-only requests and serialize/deserialize the arguments in the URI rather than the body. I skipped this because I didn't want to write
  all this undifferentiated code, it's not the point of this project. Normally, you have frameworks taking care of that.

## Optimizations

- perform writes the same way as reads to avoid copying the data once in a vec and still have atomicity

## Bugs

- move from simple-server to actix-web, as it seems like even in simple use cases simple-server has a bug causing transient issues with the body being empty
  even when the client sent a non-empty body