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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class BoundedLogPipeline {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10L;
    private static final QueueEnvelope POISON_PILL = new QueueEnvelope(null, true);

    private final RecordProcessor recordProcessor;
    private final int workers;
    private final int queueCapacity;
    private volatile long lastChecksum;

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

    public long lastChecksum() {
        return lastChecksum;
    }

    public ProcessingResult execute(LogSource source) throws InterruptedException {
        Objects.requireNonNull(source, "source must not be null");

        BlockingQueue<QueueEnvelope> queue = new ArrayBlockingQueue<>(queueCapacity);
        ConcurrentAggregator aggregator = new ConcurrentAggregator();
        AtomicInteger maxObservedQueueDepth = new AtomicInteger(0);
        AtomicLong blockedProducerEvents = new AtomicLong(0);
        AtomicInteger activeWorkers = new AtomicInteger(workers);
        AtomicLong fingerprintSink = new AtomicLong(0L);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicBoolean coordinatorAborted = new AtomicBoolean(false);
        Thread coordinatorThread = Thread.currentThread();

        ExecutorService executor = Executors.newFixedThreadPool(workers, new NamedWorkerThreadFactory());
        List<Future<?>> workerFutures = new ArrayList<>(workers);

        long startNanos = System.nanoTime();
        boolean interrupted = false;

        try {
            for (int i = 0; i < workers; i++) {
                Future<?> future = executor.submit(() -> {
                    long localFingerprint = 0L;
                    boolean receivedPoison = false;
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            QueueEnvelope item = queue.take();
                            if (item.isPoisonPill()) {
                                receivedPoison = true;
                                break;
                            }
                            localFingerprint ^= recordProcessor.process(item.rawLine(), aggregator);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable failure) {
                        workerFailure.compareAndSet(null, failure);
                        if (failure instanceof Error err) {
                            throw err;
                        }
                        if (failure instanceof RuntimeException rt) {
                            throw rt;
                        }
                        throw new RuntimeException(failure);
                    } finally {
                        fingerprintSink.accumulateAndGet(localFingerprint, (left, right) -> left ^ right);
                        int remaining = activeWorkers.decrementAndGet();
                        if (!receivedPoison && !coordinatorAborted.get() && workerFailure.get() != null && remaining == 0) {
                            coordinatorThread.interrupt();
                        }
                    }
                });
                workerFutures.add(future);
            }

            for (String line : source) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Pipeline coordinator interrupted during ingestion");
                }
                if (workerFailure.get() != null) {
                    throw new IllegalStateException("Worker thread aborted execution", workerFailure.get());
                }
                QueueEnvelope envelope = new QueueEnvelope(line, false);
                if (!queue.offer(envelope)) {
                    blockedProducerEvents.incrementAndGet();
                    queue.put(envelope);
                }
                int currentDepth = queue.size();
                maxObservedQueueDepth.accumulateAndGet(currentDepth, Math::max);
            }

            for (int i = 0; i < workers; i++) {
                if (workerFailure.get() != null) {
                    throw new IllegalStateException("Worker thread aborted execution", workerFailure.get());
                }
                queue.put(POISON_PILL);
            }

            for (Future<?> future : workerFutures) {
                try {
                    future.get();
                } catch (ExecutionException ex) {
                    throw new IllegalStateException("Unexpected worker execution failure", ex.getCause());
                }
            }
        } catch (InterruptedException ex) {
            if (workerFailure.get() != null) {
                coordinatorAborted.set(true);
                executor.shutdownNow();
                throw new IllegalStateException("Worker thread aborted execution", workerFailure.get());
            }
            interrupted = true;
            coordinatorAborted.set(true);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw ex;
        } catch (RuntimeException ex) {
            coordinatorAborted.set(true);
            executor.shutdownNow();
            throw ex;
        } finally {
            if (!interrupted) {
                shutdownGracefully(executor, SHUTDOWN_TIMEOUT_SECONDS);
            }
        }

        long durationNanos = System.nanoTime() - startNanos;
        this.lastChecksum = fingerprintSink.get();

        return aggregator.snapshot(
                "concurrent",
                workers,
                queueCapacity,
                durationNanos,
                maxObservedQueueDepth.get(),
                blockedProducerEvents.get()
        );
    }

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
