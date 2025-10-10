## Improvements

- consider replacing panicking with custom error types in the protocol's ser/de
- consider having writes to the `LogManager` concurrent by having one thread per log file, or a
  bounded pool paired with a collection of locks (one per file)