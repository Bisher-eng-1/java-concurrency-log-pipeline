package com.bisher.logpipeline.model;

import java.time.Instant;
import java.util.Objects;

public record LogEvent(
        String eventId,
        Instant timestamp,
        LogLevel level,
        String serviceName,
        String message
) {
    public LogEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        Objects.requireNonNull(level, "level must not be null");
        Objects.requireNonNull(serviceName, "serviceName must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
