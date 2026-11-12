# Concurrency Design & Trade-Offs

## 1. Producer-Consumer with Bounded Backpressure

`BoundedLogPipeline` decouples log ingestion from log parsing/processing using an `ArrayBlockingQueue<QueueEnvelope>` initialized with a configurable `--queue-capacity`.

### Why Bounded Instead of Unbounded?
An unbounded queue (such as `LinkedBlockingQueue` with `Integer.MAX_VALUE` capacity) masks throughput mismatches by buffering arbitrarily many items in heap memory until an `OutOfMemoryError` occurs. A bounded `ArrayBlockingQueue` applies natural backpressure:
- The producer first probes `queue.offer(envelope)`.
- When full (`offer` returns `false`), it increments `blockedProducerEvents` for observability and calls `queue.put(envelope)`, suspending the producer thread until a worker frees a slot.

## 2. End-of-Stream Strategy (`POISON_PILL`)

Once the `LogSource` iterator completes, the producer enqueues exactly `workers` instances of a dedicated static sentinel `POISON_PILL`. Each worker thread breaks out of its consumption loop upon reading `POISON_PILL`, ensuring every queued log record ahead of the sentinel is drained before shutdown.

### Known Limitation of Poison Pills
If a worker thread terminates abnormally before consuming its poison pill, remaining poison pills could leave another worker blocked or one pill unclaimed. To prevent silent hangs:
- `RecordProcessor` isolates per-record exceptions internally.
- The coordinator tracks `List<Future<?>>` and invokes `future.get()`, propagating unexpected `ExecutionException` causes.
- `shutdownGracefully()` enforces a bounded `awaitTermination` followed by `shutdownNow()`.

## 3. Why `java.util.concurrent` Over Manual `synchronized` Blocks

Wrapping every counter and map update in coarse-grained `synchronized(lock)` blocks serializes worker threads at the aggregation boundary, re-creating a single-threaded bottleneck inside a multi-threaded program. Manual `wait()` / `notifyAll()` on a shared buffer is also error-prone (spurious wakeups, missed signals, deadlock ordering).

Instead, this pipeline uses:
1. **`LongAdder`**: Stripes increments across dynamic thread-local cells under contention, avoiding CAS retry storms on `totalRecords`, `validRecords`, and log-level counters.
2. **`AtomicInteger`**: Used for lower-contention error counters (`invalidRecords`, `processingFailures`) and bounded telemetry (`maxObservedQueueDepth`).
3. **`ConcurrentHashMap<String, LongAdder>`**: Uses `computeIfAbsent(service, k -> new LongAdder()).increment()` so workers updating different services rarely contend, and threads updating the same service stripe into a `LongAdder`.

Manual `synchronized` is not inherently incorrect—for simple invariants across multiple fields modified together, explicit locking or `ReentrantLock` is often clearer. For independent monotonic counters and single-key map increments, lock-free/striped `java.util.concurrent` primitives deliver higher throughput and simpler reasoning.

## 4. Interruption & Shutdown Semantics

When `BoundedLogPipeline.execute()` catches `InterruptedException`:
1. It immediately cancels running tasks with `executor.shutdownNow()`.
2. It restores the thread's interrupt status (`Thread.currentThread().interrupt()`).
3. It rethrows `InterruptedException` so callers never observe an aborted run as a complete result.
