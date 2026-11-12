package com.bisher.logpipeline.aggregation;

import com.bisher.logpipeline.model.LogEvent;
import com.bisher.logpipeline.model.LogLevel;
import com.bisher.logpipeline.model.ProcessingResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencyAndAggregationTest {

    @Test
    void preventsLostUpdatesUnderHighThreadContention() throws Exception {
        int threads = 8;
        int incrementsPerThread = 5_000;
        ConcurrentAggregator aggregator = new ConcurrentAggregator();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final String service = "svc-" + (t % 3);
            futures.add(pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < incrementsPerThread; i++) {
                        LogLevel level = switch (i % 4) {
                            case 0 -> LogLevel.INFO;
                            case 1 -> LogLevel.WARN;
                            case 2 -> LogLevel.ERROR;
                            default -> LogLevel.DEBUG;
                        };
                        aggregator.recordValid(new LogEvent(
                                "id-" + i,
                                Instant.EPOCH,
                                level,
                                service,
                                "synthetic payload"
                        ));
                        if (i % 100 == 0) {
                            aggregator.recordInvalid();
                        }
                        if (i % 500 == 0) {
                            aggregator.recordProcessingFailure();
                        }
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        startGate.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        ProcessingResult snapshot = aggregator.snapshot("test", threads, 100, 1L, 0, 0);

        long expectedValid = (long) threads * incrementsPerThread;
        long expectedInvalid = (long) threads * (incrementsPerThread / 100);
        long expectedFailures = (long) threads * (incrementsPerThread / 500);

        assertEquals(expectedValid, snapshot.validRecords());
        assertEquals(expectedInvalid, snapshot.invalidRecords());
        assertEquals(expectedFailures, snapshot.processingFailures());
        assertEquals(expectedValid + expectedInvalid + expectedFailures, snapshot.totalRecords());

        long perLevelCount = expectedValid / 4;
        assertEquals(perLevelCount, snapshot.infoCount());
        assertEquals(perLevelCount, snapshot.warnCount());
        assertEquals(perLevelCount, snapshot.errorCount());
        assertEquals(perLevelCount, snapshot.debugCount());

        // Verify ConcurrentHashMap per-service error sum equals total errorCount
        long serviceErrorSum = snapshot.errorsByService().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(snapshot.errorCount(), serviceErrorSum);
        assertEquals(3, snapshot.errorsByService().size());
    }
}
