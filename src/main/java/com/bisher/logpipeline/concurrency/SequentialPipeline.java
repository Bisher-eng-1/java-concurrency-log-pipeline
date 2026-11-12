package com.bisher.logpipeline.concurrency;

import com.bisher.logpipeline.aggregation.SequentialAggregator;
import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.RecordProcessor;

import java.util.Objects;

/**
 * Single-threaded baseline log pipeline executing ingestion, parsing, enrichment,
 * and aggregation sequentially on the calling thread.
 */
public final class SequentialPipeline {

    private final RecordProcessor recordProcessor;

    public SequentialPipeline(RecordProcessor recordProcessor) {
        this.recordProcessor = Objects.requireNonNull(recordProcessor, "recordProcessor must not be null");
    }

    public ProcessingResult execute(LogSource source) {
        Objects.requireNonNull(source, "source must not be null");
        SequentialAggregator aggregator = new SequentialAggregator();

        long startNanos = System.nanoTime();
        long fingerprintAccumulator = 0L;

        for (String line : source) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            fingerprintAccumulator ^= recordProcessor.process(line, aggregator);
        }

        long durationNanos = System.nanoTime() - startNanos;
        // Prevent JIT from eliminating deterministic computation
        if (fingerprintAccumulator == Long.MIN_VALUE) {
            System.err.print("");
        }

        return aggregator.snapshot("sequential", 1, 0, durationNanos, 0, 0);
    }
}
