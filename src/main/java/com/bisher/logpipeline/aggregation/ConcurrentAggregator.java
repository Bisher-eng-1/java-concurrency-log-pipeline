package com.bisher.logpipeline.aggregation;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.ProcessingResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public final class ConcurrentAggregator implements ResultAggregator {

    private final LongAdder totalRecords = new LongAdder();
    private final LongAdder validRecords = new LongAdder();
    private final AtomicInteger invalidRecords = new AtomicInteger(0);
    private final AtomicInteger processingFailures = new AtomicInteger(0);

    private final LongAdder infoCount = new LongAdder();
    private final LongAdder warnCount = new LongAdder();
    private final LongAdder errorCount = new LongAdder();
    private final LongAdder debugCount = new LongAdder();

    private final ConcurrentHashMap<String, LongAdder> errorsByService = new ConcurrentHashMap<>();

    @Override
    public void recordValid(LogEvent event) {
        totalRecords.increment();
        validRecords.increment();
        switch (event.level()) {
            case INFO -> infoCount.increment();
            case WARN -> warnCount.increment();
            case ERROR -> {
                errorCount.increment();
                errorsByService
                        .computeIfAbsent(event.serviceName(), ignored -> new LongAdder())
                        .increment();
            }
            case DEBUG -> debugCount.increment();
        }
    }

    @Override
    public void recordInvalid() {
        totalRecords.increment();
        invalidRecords.incrementAndGet();
    }

    @Override
    public void recordProcessingFailure() {
        totalRecords.increment();
        processingFailures.incrementAndGet();
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
        Map<String, Long> serviceErrors = errorsByService.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().sum()
                ));

        return new ProcessingResult(
                mode,
                workerCount,
                queueCapacity,
                totalRecords.sum(),
                validRecords.sum(),
                invalidRecords.get(),
                processingFailures.get(),
                infoCount.sum(),
                warnCount.sum(),
                errorCount.sum(),
                debugCount.sum(),
                serviceErrors,
                durationNanos,
                maxObservedQueueDepth,
                blockedProducerEvents
        );
    }
}
