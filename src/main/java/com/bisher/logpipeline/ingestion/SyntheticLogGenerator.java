package com.bisher.logpipeline.ingestion;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Deterministic streaming generator of synthetic log events.
 * <p>
 * Note: All records produced by this class are synthetic test fixtures generated
 * from a fixed PRNG seed for reproducible benchmarking and fault-handling tests.
 * They do not represent real production traffic.
 */
public class SyntheticLogGenerator implements LogSource {

    private static final List<String> SERVICES = List.of(
            "api-gateway",
            "auth-service",
            "payment-worker",
            "order-processor",
            "cache-node",
            "notification-dispatcher"
    );

    private static final List<String> INFO_MESSAGES = List.of(
            "Request routed successfully in 12ms",
            "User session validated cleanly",
            "Batch checkpoint committed",
            "Cache lookup hit on primary key",
            "Order state transitioned to CONFIRMED"
    );

    private static final List<String> DEBUG_MESSAGES = List.of(
            "Inspecting downstream connection pool state",
            "Token claims decoded in 1.2ms",
            "Serialization buffer allocated 4096 bytes",
            "Evaluating retry backoff window"
    );

    private static final List<String> WARN_MESSAGES = List.of(
            "Downstream latency exceeded 350ms threshold",
            "Cache eviction rate elevated above baseline",
            "Retry attempt 1 scheduled for transient socket reset",
            "Thread pool queue utilization passed 75%"
    );

    private static final List<String> ERROR_MESSAGES = List.of(
            "Timeout waiting for downstream settlement response after 2000ms",
            "Connection refused by upstream shard",
            "Checksum verification failed for incoming payload",
            "Circuit breaker transitioned to OPEN state"
    );

    private static final Instant BASE_TIMESTAMP = Instant.parse("2026-01-15T08:00:00Z");

    private final int eventCount;
    private final long seed;
    private final double malformedRate;

    public SyntheticLogGenerator(int eventCount, long seed, double malformedRate) {
        if (eventCount < 0) {
            throw new IllegalArgumentException("eventCount must be >= 0");
        }
        if (malformedRate < 0.0 || malformedRate > 1.0) {
            throw new IllegalArgumentException("malformedRate must be in [0.0, 1.0]");
        }
        this.eventCount = eventCount;
        this.seed = seed;
        this.malformedRate = malformedRate;
    }

    @Override
    public String description() {
        return String.format("Synthetic stream (events=%d, seed=%d, malformedRate=%.2f%%)",
                eventCount, seed, malformedRate * 100.0);
    }

    @Override
    public Iterator<String> iterator() {
        return new SyntheticIterator(eventCount, seed, malformedRate);
    }

    private static final class SyntheticIterator implements Iterator<String> {
        private final int total;
        private final double malformedRate;
        private final Random random;
        private int index = 0;

        SyntheticIterator(int total, long seed, double malformedRate) {
            this.total = total;
            this.malformedRate = malformedRate;
            this.random = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            return index < total;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Synthetic log stream exhausted");
            }
            int current = index++;
            return formatEvent(current);
        }

        private String formatEvent(int seq) {
            // Advance deterministic PRNG identically for each position
            double malformedRoll = random.nextDouble();
            double severityRoll = random.nextDouble();
            int serviceIdx = random.nextInt(SERVICES.size());
            int msgRoll = random.nextInt(100);
            int jitterMillis = random.nextInt(1000);

            if (malformedRoll < malformedRate) {
                return switch (seq % 4) {
                    case 0 -> "CORRUPTED_RECORD_NO_DELIMITERS_" + seq;
                    case 1 -> "not-an-iso-timestamp|INFO|api-gateway|evt-" + seq + "|Bad timestamp";
                    case 2 -> BASE_TIMESTAMP.plusMillis(seq).toString() + "|CRITICAL|auth-service|evt-" + seq + "|Unknown severity";
                    default -> BASE_TIMESTAMP.plusMillis(seq).toString() + "|ERROR||evt-" + seq + "|Missing service name";
                };
            }

            // Realistic distribution: 60% INFO, 20% DEBUG, 13% WARN, 7% ERROR
            String level;
            String message;
            if (severityRoll < 0.60) {
                level = "INFO";
                message = INFO_MESSAGES.get(msgRoll % INFO_MESSAGES.size());
            } else if (severityRoll < 0.80) {
                level = "DEBUG";
                message = DEBUG_MESSAGES.get(msgRoll % DEBUG_MESSAGES.size());
            } else if (severityRoll < 0.93) {
                level = "WARN";
                message = WARN_MESSAGES.get(msgRoll % WARN_MESSAGES.size());
            } else {
                level = "ERROR";
                message = ERROR_MESSAGES.get(msgRoll % ERROR_MESSAGES.size());
            }

            Instant ts = BASE_TIMESTAMP.plusMillis((long) seq * 5L + jitterMillis);
            String service = SERVICES.get(serviceIdx);
            String eventId = String.format("evt-%07d", seq);

            return ts + "|" + level + "|" + service + "|" + eventId + "|" + message;
        }
    }
}
