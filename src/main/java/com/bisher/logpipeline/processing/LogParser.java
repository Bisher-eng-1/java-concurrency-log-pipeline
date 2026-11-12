package com.bisher.logpipeline.processing;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.LogLevel;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Stateless, thread-safe parser for pipe-delimited log lines:
 * {@code <ISO-8601-timestamp>|<LEVEL>|<service>|<eventId>|<message>}
 */
public final class LogParser {

    private static final int EXPECTED_FIELD_COUNT = 5;

    /**
     * Parses and validates a single raw log line.
     *
     * @param rawLine raw text line
     * @return validated {@link LogEvent}
     * @throws MalformedLogException if the line is null, blank, has wrong field count,
     *                               invalid timestamp, invalid severity, or blank metadata
     */
    public LogEvent parse(String rawLine) throws MalformedLogException {
        if (rawLine == null || rawLine.isBlank()) {
            throw new MalformedLogException("Blank or null log record");
        }

        String[] parts = rawLine.split("\\|", EXPECTED_FIELD_COUNT);
        if (parts.length != EXPECTED_FIELD_COUNT) {
            throw new MalformedLogException(
                    "Expected " + EXPECTED_FIELD_COUNT + " pipe-delimited fields, found " + parts.length);
        }

        Instant timestamp;
        try {
            timestamp = Instant.parse(parts[0].trim());
        } catch (DateTimeParseException ex) {
            throw new MalformedLogException("Invalid ISO-8601 timestamp: " + parts[0], ex);
        }

        LogLevel level;
        try {
            level = LogLevel.parseStrict(parts[1]);
        } catch (IllegalArgumentException ex) {
            throw new MalformedLogException(ex.getMessage(), ex);
        }

        String serviceName = parts[2].trim();
        if (serviceName.isEmpty()) {
            throw new MalformedLogException("Missing service name");
        }

        String eventId = parts[3].trim();
        if (eventId.isEmpty()) {
            throw new MalformedLogException("Missing event identifier");
        }

        String message = parts[4];
        if (message.isBlank()) {
            throw new MalformedLogException("Blank log message");
        }

        return new LogEvent(eventId, timestamp, level, serviceName, message);
    }
}
