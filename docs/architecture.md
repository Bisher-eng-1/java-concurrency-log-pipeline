# Architecture Overview

## 1. System Context & Responsibilities

The **Java Concurrency Log Pipeline** separates ingestion, parsing/computation, concurrency coordination, aggregation, benchmarking, and reporting into cohesive packages under `com.bisher.logpipeline`:

```text
com.bisher.logpipeline
├── LogProcessor          # CLI entry point and mode coordinator
├── model/                # Immutable records (LogEvent, ProcessingResult, PipelineConfig, LogLevel)
├── ingestion/            # Streaming sources (LogSource, SyntheticLogGenerator, FileLogSource)
├── processing/           # Stateless schema parsing and deterministic per-record computation
├── aggregation/          # SequentialAggregator vs. thread-safe ConcurrentAggregator
├── concurrency/          # SequentialPipeline and bounded producer-consumer BoundedLogPipeline
├── benchmark/            # BenchmarkRunner (warm-up, repeated runs, logical equivalence check)
└── reporting/            # ConsoleReporter formatting structured comparison tables
```

## 2. Streaming Ingestion Model

Both `SyntheticLogGenerator` and `FileLogSource` implement `LogSource` (`Iterable<String> + AutoCloseable`). Rather than preloading 50,000+ raw strings into a `List<String>` in heap memory, records are pulled lazily one line at a time via an `Iterator<String>`. This keeps producer heap allocation near constant `O(queueCapacity)` during bounded concurrent runs.

## 3. Sequential vs. Concurrent Data Flow

- **Sequential Mode (`SequentialPipeline`)**:
  Reads one record at a time on the calling thread, passes it to `RecordProcessor`, and updates `SequentialAggregator` using plain `long` fields and a `HashMap<String, Long>`. Zero locking or atomic overhead.
- **Concurrent Mode (`BoundedLogPipeline`)**:
  The coordinator thread acts as the producer, pushing `QueueEnvelope` items into an `ArrayBlockingQueue` of fixed capacity. A fixed pool of worker threads (`ExecutorService`) drains the queue, runs `RecordProcessor`, and updates `ConcurrentAggregator` using `LongAdder`, `AtomicInteger`, and `ConcurrentHashMap`.

## 4. Failure Isolation Boundaries

1. **Malformed Records**: Caught inside `RecordProcessor.process()` via `MalformedLogException`, incrementing `invalidRecords` without interrupting valid records.
2. **Unexpected Runtime Faults**: Caught separately in `RecordProcessor.process()`, incrementing `processingFailures` so a single corrupt record does not kill a worker thread or corrupt the aggregation.
3. **Missing or Unreadable Input Files**: Fails fast at `FileLogSource` construction with an explicit `NoSuchFileException` / `IOException` and non-zero CLI exit code.
