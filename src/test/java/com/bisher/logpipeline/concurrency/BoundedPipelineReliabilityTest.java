package com.bisher.logpipeline.concurrency;

import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.ingestion.SyntheticLogGenerator;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.LogParser;
import com.bisher.logpipeline.processing.RecordProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedPipelineReliabilityTest {

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void enforcesBoundedQueueBackpressureWhenWorkersAreSaturated() throws Exception {
        int queueCapacity = 4;
        int workers = 1;
        int totalEvents = 20;

        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);

        RecordProcessor blockingFirstRecordProcessor = new RecordProcessor(new LogParser(), 0, event -> {
            if ("evt-0000000".equals(event.eventId())) {
                workerEntered.countDown();
                try {
                    if (!releaseWorker.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Test latch timed out");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        BoundedLogPipeline pipeline = new BoundedLogPipeline(blockingFirstRecordProcessor, workers, queueCapacity);
        AtomicReference<ProcessingResult> resultRef = new AtomicReference<>();

        Thread coordinator = new Thread(() -> {
            try {
                resultRef.set(pipeline.execute(new SyntheticLogGenerator(totalEvents, 7L, 0.0)));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, "test-coordinator");

        coordinator.start();
        assertTrue(workerEntered.await(5, TimeUnit.SECONDS));

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (coordinator.getState() != Thread.State.WAITING && System.nanoTime() < deadlineNanos) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.WAITING, coordinator.getState());

        releaseWorker.countDown();
        coordinator.join(5000);
        assertFalse(coordinator.isAlive());

        ProcessingResult result = resultRef.get();
        assertEquals(totalEvents, result.totalRecords());
        assertTrue(result.maxObservedQueueDepth() <= queueCapacity);
        assertTrue(result.blockedProducerEvents() > 0);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void shutsDownAllWorkerThreadsCleanlyAfterCompletion() throws Exception {
        RecordProcessor processor = new RecordProcessor(new LogParser(), 0);
        BoundedLogPipeline pipeline = new BoundedLogPipeline(processor, 4, 64);

        ProcessingResult result = pipeline.execute(new SyntheticLogGenerator(1000, 11L, 0.01));
        assertEquals(1000, result.totalRecords());

        long lingeringWorkers = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("log-worker-") && t.isAlive())
                .count();
        assertEquals(0L, lingeringWorkers);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void isolatesUnexpectedProcessingExceptionsWithoutCorruptingValidRecords() throws Exception {
        AtomicInteger seen = new AtomicInteger(0);
        RecordProcessor faultyProcessor = new RecordProcessor(new LogParser(), 0, event -> {
            if (seen.incrementAndGet() % 5 == 0) {
                throw new IllegalStateException("Injected downstream processing fault");
            }
        });

        LogSource source = new SyntheticLogGenerator(100, 55L, 0.0);
        ProcessingResult seq = new SequentialPipeline(faultyProcessor).execute(source);
        seen.set(0);
        ProcessingResult conc = new BoundedLogPipeline(faultyProcessor, 2, 16).execute(source);

        assertEquals(100, seq.totalRecords());
        assertEquals(20, seq.processingFailures());
        assertEquals(80, seq.validRecords());
        assertEquals(100, conc.totalRecords());
        assertEquals(20, conc.processingFailures());
        assertEquals(80, conc.validRecords());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void abortsProducerWithoutDeadlockWhenAllWorkersCrash() {
        RecordProcessor crashingProcessor = new RecordProcessor(new LogParser(), 0, event -> {
            throw new AssertionError("Simulated fatal worker error");
        });

        BoundedLogPipeline pipeline = new BoundedLogPipeline(crashingProcessor, 1, 2);
        LogSource source = new SyntheticLogGenerator(500, 3L, 0.0);

        assertThrows(IllegalStateException.class, () -> pipeline.execute(source));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void handlesCoordinatorInterruptionGracefullyWithoutHangingWorkers() throws Exception {
        CountDownLatch firstConsumed = new CountDownLatch(1);
        RecordProcessor slowProcessor = new RecordProcessor(new LogParser(), 0, event -> firstConsumed.countDown());

        LogSource infiniteSource = new LogSource() {
            @Override
            public String description() {
                return "infinite";
            }

            @Override
            public Iterator<String> iterator() {
                return new Iterator<>() {
                    private long i = 0;

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public String next() {
                        return "2026-01-15T08:00:00Z|INFO|api-gateway|evt-" + (i++) + "|stream";
                    }
                };
            }
        };

        BoundedLogPipeline pipeline = new BoundedLogPipeline(slowProcessor, 2, 8);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread runner = new Thread(() -> {
            try {
                pipeline.execute(infiniteSource);
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "interrupt-test-runner");

        runner.start();
        assertTrue(firstConsumed.await(5, TimeUnit.SECONDS));

        runner.interrupt();
        runner.join(5000);

        assertFalse(runner.isAlive());
        assertInstanceOf(InterruptedException.class, thrown.get());
    }
}
