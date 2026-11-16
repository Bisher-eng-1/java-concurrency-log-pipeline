package com.bisher.logpipeline.aggregation;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.ProcessingResult;

public interface ResultAggregator {

    void recordValid(LogEvent event);

    void recordInvalid();

    void recordProcessingFailure();

    ProcessingResult snapshot(
            String mode,
            int workerCount,
            int queueCapacity,
            long durationNanos,
            long maxObservedQueueDepth,
            long blockedProducerEvents
    );
}
