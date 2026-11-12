package com.bisher.logpipeline.aggregation;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.LogLevel;
import com.bisher.logpipeline.model.ProcessingResult;

import java.util.HashMap;
import java.util.Map;

/**
 * Single-threaded aggregator using plain primitive counters and {@link HashMap}
 * with zero synchronization overhead. Not thread-safe.
 */
public final class SequentialAggregator implements ResultAggregator {

    private long totalRecords = 0;
    private long validRecords = 0;
    private long invalidRecords = 0;
    private long processingFailures = 0;
    private long infoCount = 0;
    private long warnCount = 0;
    private long errorCount = 0;
    private long debugCount = 0;
    private final Map<String, Long> errorsByService = new HashMap<>();

    @Override
    public void recordValid(LogEvent event) {
        totalRecords++;
        validRecords++;
        switch (event.level()) {
            case INFO -> infoCount++;
            case WARN -> warnCount++;
            case ERROR -> {
                errorCount++;
                errorsByService.merge(event.serviceName(), 1L, Long::sum);
            }
            case DEBUG -> debugCount++;
        }
    }

    @Override
    public void recordInvalid() {
        totalRecords++;
        invalidRecords++;
    }

    @Override
    public void recordProcessingFailure() {
        totalRecords++;
        processingFailures++;
    }

    @Override
    public ProcessingResult snapshot(
            String mode,
            int workerCount,
            int queueCapacity,
            long durationNanos,
            long maxObservedQueueDepth,
            long blockedProducerEvents
    ) {
        return new ProcessingResult(
                mode,
                workerCount,
                queueCapacity,
                totalRecords,
                validRecords,
                invalidRecords,
                processingFailures,
                infoCount,
                warnCount,
                errorCount,
                debugCount,
                errorsByService,
                durationNanos,
                maxObservedQueueDepth,
                blockedProducerEvents
        );
    }
}
