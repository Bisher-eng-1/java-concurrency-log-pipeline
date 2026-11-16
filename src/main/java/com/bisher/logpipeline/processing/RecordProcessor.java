package com.bisher.logpipeline.processing;

import com.bisher.logpipeline.aggregation.ResultAggregator;
import com.bisher.logpipeline.model.LogEvent;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;

public final class RecordProcessor {

    private final LogParser parser;
    private final int workIterations;
    private final Consumer<LogEvent> postParseHook;

    public RecordProcessor(LogParser parser, int workIterations) {
        this(parser, workIterations, null);
    }

    public RecordProcessor(LogParser parser, int workIterations, Consumer<LogEvent> postParseHook) {
        this.parser = Objects.requireNonNull(parser, "parser must not be null");
        if (workIterations < 0) {
            throw new IllegalArgumentException("workIterations must be >= 0");
        }
        this.workIterations = workIterations;
        this.postParseHook = postParseHook;
    }

    public long process(String rawLine, ResultAggregator aggregator) {
        Objects.requireNonNull(aggregator, "aggregator must not be null");
        try {
            LogEvent event = parser.parse(rawLine);
            if (postParseHook != null) {
                postParseHook.accept(event);
            }
            long fingerprint = computeDeterministicFingerprint(event);
            aggregator.recordValid(event);
            return fingerprint;
        } catch (MalformedLogException ex) {
            aggregator.recordInvalid();
            return 0L;
        } catch (RuntimeException ex) {
            // Unexpected per-record processing failure tracked separately from schema validation errors
            aggregator.recordProcessingFailure();
            return -1L;
        }
    }

    private long computeDeterministicFingerprint(LogEvent event) {
        if (workIterations == 0) {
            return event.eventId().hashCode();
        }
        byte[] bytes = event.message().getBytes(StandardCharsets.UTF_8);
        long acc = 0x811c9dc5L ^ event.level().ordinal();
        for (int iter = 0; iter < workIterations; iter++) {
            for (byte b : bytes) {
                acc ^= (b & 0xffL);
                acc *= 0x01000193L;
            }
        }
        return acc;
    }
}
