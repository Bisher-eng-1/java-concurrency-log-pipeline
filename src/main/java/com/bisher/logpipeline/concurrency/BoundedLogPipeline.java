package com.bisher.logpipeline.concurrency;

import com.bisher.logpipeline.aggregation.ConcurrentAggregator;
import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.RecordProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producer-consumer concurrent log pipeline backed by a bounded {@link BlockingQueue}
 * and a fixed-size {@link ExecutorService} worker pool.
 * <p>
 * Backpressure and lifecycle guarantees:
 * <ul>
 *     <li>Uses {@link ArrayBlockingQueue} with fixed capacity so fast producers block via
 *         {@code queue.put(...)} when workers saturate rather than exhausting heap memory.</li>
 *     <li>Uses an explicit {@code POISON_PILL} sentinel placed once per worker after
 *         the producer finishes streaming so workers terminate cleanly without hanging.</li>
 *     <li>Restores interrupt status and performs bounded graceful executor shutdown
 *         ({@code shutdown()} &rarr; {@code awaitTermination()} &rarr; {@code shutdownNow()}).</li>
 * </ul>
 */
public final class BoundedLogPipeline {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10L;
    private static final QueueEnvelope POISON_PILL = new QueueEnvelope(null, true);

    private final RecordProcessor recordProcessor;
    private final int workers;
    private final int queueCapacity;

    public BoundedLogPipeline(RecordProcessor recordProcessor, int workers, int queueCapacity) {
        this.recordProcessor = Objects.requireNonNull(recordProcessor, "recordProcessor must not be null");
        if (workers < 1) {
            throw new IllegalArgumentException("workers must be >= 1");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1");
        }
        this.workers = workers;
        this.queueCapacity = queueCapacity;
    }

    /**
     * Executes the concurrent pipeline over the given streaming {@link LogSource}.
     *
     * @throws InterruptedException if the coordinating thread is interrupted while running
     */
    public ProcessingResult execute(LogSource source) throws InterruptedException {
        Objects.requireNonNull(source, "source must not be null");

        BlockingQueue<QueueEnvelope> queue = new ArrayBlockingQueue<>(queueCapacity);
        ConcurrentAggregator aggregator = new ConcurrentAggregator();
        AtomicInteger maxObservedQueueDepth = new AtomicInteger(0);
        AtomicLong blockedProducerEvents = new AtomicLong(0);
        AtomicInteger activeWorkers = new AtomicInteger(0);
        AtomicLong fingerprintSink = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(workers, new NamedWorkerThreadFactory());
        List<Future<?>> workerFutures = new ArrayList<>(workers);

        long startNanos = System.nanoTime();
        boolean interrupted = false;

        try {
            // Start fixed-size consumer pool
            for (int i = 0; i < workers; i++) {
                Future<?> future = executor.submit(() -> {
                    activeWorkers.incrementAndGet();
                    long localFingerprint = 0L;
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            QueueEnvelope item = queue.take();
                            if (item.isPoisonPill()) {
                                break;
                            }
                            localFingerprint ^= recordProcessor.process(item.rawLine(), aggregator);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        fingerprintSink.addAndGet(localFingerprint);
                        activeWorkers.decrementAndGet();
                    }
                });
                workerFutures.add(future);
            }

            // Produce records onto the bounded queue (applying backpressure on saturation)
            for (String line : source) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Pipeline coordinator interrupted during ingestion");
                }
                QueueEnvelope envelope = new QueueEnvelope(line, false);
                if (!queue.offer(envelope)) {
                    blockedProducerEvents.incrementAndGet();
                    queue.put(envelope);
                }
                int currentDepth = queue.size();
                maxObservedQueueDepth.accumulateAndGet(currentDepth, Math::max);
            }

            // Enqueue one sentinel poison pill per worker for deterministic shutdown
            for (int i = 0; i < workers; i++) {
                queue.put(POISON_PILL);
            }

            // Wait for all workers to finish draining remaining items
            for (Future<?> future : workerFutures) {
                try {
                    future.get();
                } catch (ExecutionException ex) {
                    throw new IllegalStateException("Unexpected worker execution failure", ex.getCause());
                }
            }
        } catch (InterruptedException ex) {
            interrupted = true;
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw ex;
        } finally {
            if (!interrupted) {
                shutdownGracefully(executor, SHUTDOWN_TIMEOUT_SECONDS);
            }
        }

        long durationNanos = System.nanoTime() - startNanos;
        if (fingerprintSink.get() == Long.MIN_VALUE) {
            System.err.print("");
        }

        return aggregator.snapshot(
                "concurrent",
                workers,
                queueCapacity,
                durationNanos,
                maxObservedQueueDepth.get(),
                blockedProducerEvents.get()
        );
    }

    /**
     * Performs standard two-phase graceful shutdown of an {@link ExecutorService}.
     */
    public static void shutdownGracefully(ExecutorService executor, long timeoutSeconds) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                    System.err.println("Warning: Worker executor did not terminate within timeout.");
                }
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private record QueueEnvelope(String rawLine, boolean isPoisonPill) {
    }

    private static final class NamedWorkerThreadFactory implements ThreadFactory {
        private final AtomicInteger threadId = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "log-worker-" + threadId.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}
