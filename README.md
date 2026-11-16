# Java Concurrency Log Pipeline

Java 17 log ingestion and aggregation pipeline comparing single-threaded execution (`SequentialPipeline`) against a bounded producer-consumer worker pool (`BoundedLogPipeline`).

## Design

- **Ingestion (`ingestion/`)**: streams pipe-delimited records (`<timestamp>|<level>|<service>|<eventId>|<message>`) lazily from disk (`FileLogSource`) or deterministic PRNG fixtures (`SyntheticLogGenerator`).
- **Bounded Queue & Backpressure (`BoundedLogPipeline`)**: feeds an `ArrayBlockingQueue` of fixed capacity. When workers fall behind, the producer blocks in `queue.put(...)` while tracking backpressure events and peak queue depth. Workers exit on per-worker `POISON_PILL` sentinels; if all workers abort abnormally via a fatal `Error`, the coordinator is woken immediately instead of hanging on a full queue.
- **Contention-Reduced Aggregation (`ConcurrentAggregator`)**: uses `LongAdder` for high-frequency counters (`INFO`/`WARN`/`ERROR`/`DEBUG`, totals) and `ConcurrentHashMap<String, LongAdder>` for per-service error grouping.
- **Per-Record Checksum Parity**: each worker accumulates an XOR fingerprint across processed events and merges via commutative XOR (`accumulateAndGet`) so concurrent runs match `SequentialPipeline.lastChecksum()`.

## Build & Run

Run tests and build the executable JAR (Maven + JDK 17):

```bash
mvn clean test
mvn package -DskipTests
```

Run comparison or single modes:

```bash
java -jar target/java-concurrency-log-pipeline.jar --mode sequential --events 10000
java -jar target/java-concurrency-log-pipeline.jar --mode concurrent --workers 4 --queue-capacity 500 --events 10000
java -jar target/java-concurrency-log-pipeline.jar --mode compare --workers 4 --events 50000
```

Or via Docker:

```bash
docker build -t java-concurrency-log-pipeline .
docker run --rm java-concurrency-log-pipeline --mode compare --workers 4 --events 50000
```

## Limitations

- `BenchmarkRunner` measures in-process wall-clock time after warm-up iterations rather than JMH-forked microbenchmarks, so numbers include JVM GC and host scheduling variance.
- `BoundedLogPipeline` processes records in process memory without durable offset checkpointing across JVM crashes.
