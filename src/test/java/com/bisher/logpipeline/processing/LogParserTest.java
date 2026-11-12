package com.bisher.logpipeline.processing;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.LogLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LogParserTest {

    private final LogParser parser = new LogParser();

    @Test
    void parsesValidStructuredLogLine() throws MalformedLogException {
        String raw = "2026-01-15T08:00:01.102Z|ERROR|payment-worker|evt-0001|Settlement timeout after 2000ms";
        LogEvent event = parser.parse(raw);

        assertEquals(Instant.parse("2026-01-15T08:00:01.102Z"), event.timestamp());
        assertEquals(LogLevel.ERROR, event.level());
        assertEquals("payment-worker", event.serviceName());
        assertEquals("evt-0001", event.eventId());
        assertEquals("Settlement timeout after 2000ms", event.message());
    }

    @Test
    void preservesPipeCharactersInsideMessagePayload() throws MalformedLogException {
        String raw = "2026-01-15T08:00:01.102Z|INFO|api-gateway|evt-0002|status=200|path=/orders";
        LogEvent event = parser.parse(raw);

        assertEquals("status=200|path=/orders", event.message());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "   ",
            "MALFORMED_WITHOUT_DELIMITERS",
            "2026-01-15T08:00:01Z|INFO|api-gateway",
            "not-a-timestamp|INFO|api-gateway|evt-1|message",
            "2026-01-15T08:00:01Z|FATAL|api-gateway|evt-1|unknown level",
            "2026-01-15T08:00:01Z|INFO||evt-1|missing service",
            "2026-01-15T08:00:01Z|INFO|api-gateway||missing event id",
            "2026-01-15T08:00:01Z|INFO|api-gateway|evt-1|   "
    })
    void rejectsMalformedLogRecords(String invalidLine) {
        assertThrows(MalformedLogException.class, () -> parser.parse(invalidLine));
    }

    @Test
    void rejectsNullRecord() {
        assertThrows(MalformedLogException.class, () -> parser.parse(null));
    }
}
