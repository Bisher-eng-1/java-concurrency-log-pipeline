package com.bisher.logpipeline.model;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public record ProcessingResult(
        String mode,
        int workerCount,
        int queueCapacity,
        long totalRecords,
        long validRecords,
        long invalidRecords,
        long processingFailures,
        long infoCount,
        long warnCount,
        long errorCount,
        long debugCount,
        Map<String, Long> errorsByService,
        long durationNanos,
        long maxObservedQueueDepth,
        long blockedProducerEvents
) {
    public ProcessingResult {
        errorsByService = Collections.unmodifiableMap(new TreeMap<>(Objects.requireNonNull(errorsByService)));
    }

    public double durationMillis() {
        return durationNanos / 1_000_000.0;
    }

    public double durationSeconds() {
        return durationNanos / 1_000_000_000.0;
    }

    public double throughputRecordsPerSec() {
        if (durationNanos <= 0) {
            return 0.0;
        }
        return totalRecords / durationSeconds();
    }

    public boolean isLogicallyEquivalent(ProcessingResult other) {
        if (other == null) {
            return false;
        }
        return this.totalRecords == other.totalRecords
                && this.validRecords == other.validRecords
                && this.invalidRecords == other.invalidRecords
                && this.processingFailures == other.processingFailures
                && this.infoCount == other.infoCount
                && this.warnCount == other.warnCount
                && this.errorCount == other.errorCount
                && this.debugCount == other.debugCount
                && Objects.equals(this.errorsByService, other.errorsByService);
    }
}
