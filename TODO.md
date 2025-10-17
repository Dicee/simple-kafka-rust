## Improvements

- consider replacing panicking with custom error types in the protocol's ser/de
- consider having writes to the `LogManager` concurrent by having one thread per log file, or a
  bounded pool paired with a collection of locks (one per file)
- return fine-grained, strongly typed deserializable errors from my HTTP clients for API 4xx
- use GET for read-only requests and serialize/deserialize the arguments in the URI rather than the body. I skipped this because I didn't want to write
  all this undifferentiated code, it's not the point of this project. Normally, you have frameworks taking care of that.
- add proper logging

### Optimizations

- process several batches together on the broker write path, waiting (a limited amount of time) to receive a few messages from the channel and write them all at once

## Bugs

- move from simple-server to actix-web, as it seems like even in simple use cases simple-server has a bug causing transient issues with the body being empty
  even when the client sent a non-empty body
