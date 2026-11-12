# Benchmark Methodology & Limitations

## 1. Measurement Design

`BenchmarkRunner` compares `SequentialPipeline` and `BoundedLogPipeline` on the exact same dataset (identical `--events`, `--seed`, `--malformed-rate`, and `--work-iterations`, or identical `--input` file).

1. **Warm-Up Phase (`--warmup`)**: Executes discarded iterations of both pipelines before timing begins so class loading and C2 JIT compilation stabilize.
2. **Alternating Order Across Repeats (`--repeats`)**: Even repeats run Sequential &rarr; Concurrent; odd repeats run Concurrent &rarr; Sequential to reduce ordering and GC-drift bias.
3. **Correctness Gate Before Speed Comparison**: Every repeat asserts `seq.isLogicallyEquivalent(conc)` (all record counts, malformed counts, failure counts, severity counts, and `errorsByService` maps must match exactly). Performance numbers are only meaningful when correctness holds.

## 2. Why Multithreading Is Not Always Faster

Multithreading incurs fixed coordination costs:
- Thread creation and `ExecutorService` scheduling overhead.
- `ArrayBlockingQueue` lock acquisition (`ReentrantLock` inside `ArrayBlockingQueue`) on every enqueue/dequeue pair.
- CPU cache-line bouncing and memory barrier costs.

When per-record work is trivial (`--work-iterations 0`) or dataset size is tiny (e.g. 500 events), `SequentialPipeline` can match or outperform `BoundedLogPipeline` because single-threaded execution avoids queue contention and stays hot in L1 CPU cache. As per-record CPU work (`--work-iterations`) or dataset size increases across multiple physical cores, concurrent execution amortizes queue overhead and achieves speedup up to core saturation (bounded by Amdahl's Law and single-producer ingestion rate).

## 3. Indicative Benchmark vs. JMH Microbenchmark

`BenchmarkRunner` measures end-to-end wall-clock duration via `System.nanoTime()` inside a single JVM process. While warm-up runs and repeated trials improve stability, this is an **indicative application-level benchmark**, not a JMH-isolated microbenchmark (it does not fork separate JVMs per trial or control CPU governor/OS noise).
