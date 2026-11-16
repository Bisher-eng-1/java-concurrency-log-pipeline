package com.bisher.logpipeline.reporting;

import com.bisher.logpipeline.benchmark.BenchmarkRunner.ComparisonReport;
import com.bisher.logpipeline.model.ProcessingResult;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Map;

public final class ConsoleReporter {

    private final PrintStream out;

    public ConsoleReporter(PrintStream out) {
        this.out = out;
    }

    public void printSingleResult(ProcessingResult result, String datasetDescription) {
        out.println("============================================================");
        out.println("JAVA CONCURRENCY LOG PIPELINE");
        out.println("============================================================");
        out.printf("Dataset:             %s%n", datasetDescription);
        out.printf("Execution Mode:      %s%n", result.mode().toUpperCase(Locale.ROOT));
        out.printf("Workers:             %d%n", result.workerCount());
        if (result.queueCapacity() > 0) {
            out.printf("Queue Capacity:      %d%n", result.queueCapacity());
            out.printf("Max Queue Depth:     %d%n", result.maxObservedQueueDepth());
            out.printf("Backpressure Events: %d%n", result.blockedProducerEvents());
        }
        out.println("------------------------------------------------------------");
        out.printf(Locale.ROOT, "Execution Time:      %.2f ms%n", result.durationMillis());
        out.printf(Locale.ROOT, "Throughput:          %,.0f records/sec%n", result.throughputRecordsPerSec());
        printAggregationDetails(result);
        out.println("============================================================");
    }

    public void printComparisonReport(ComparisonReport report) {
        ProcessingResult seq = report.representativeSequential();
        ProcessingResult conc = report.representativeConcurrent();

        out.println("============================================================");
        out.println("JAVA CONCURRENCY LOG PIPELINE");
        out.println("============================================================");
        out.printf("Dataset: %,d events (%s)%n", seq.totalRecords(), report.datasetDescription());
        out.printf("Warm-up Runs: %d | Measured Repeats: %d | Available CPU Cores: %d%n",
                report.warmupRuns(), report.measuredRepeats(), Runtime.getRuntime().availableProcessors());
        out.println();

        out.println("Sequential Processing:");
        out.printf(Locale.ROOT, "Execution Time: %.2f ms (avg over %d runs)%n",
                report.avgSequentialMillis(), report.measuredRepeats());
        out.printf(Locale.ROOT, "Throughput:     %,.0f records/sec%n", report.avgSequentialThroughput());
        out.println();

        out.println("Concurrent Processing:");
        out.printf("Workers:         %d (Queue Capacity: %d)%n", report.workers(), report.queueCapacity());
        out.printf(Locale.ROOT, "Execution Time:  %.2f ms (avg over %d runs)%n",
                report.avgConcurrentMillis(), report.measuredRepeats());
        out.printf(Locale.ROOT, "Throughput:      %,.0f records/sec%n", report.avgConcurrentThroughput());
        out.printf("Max Queue Depth: %d | Producer Backpressure Blocks: %d%n",
                conc.maxObservedQueueDepth(), conc.blockedProducerEvents());
        out.println();

        out.println("Result Verification:");
        out.printf("Sequential and concurrent outputs: %s%n", report.outputsMatch() ? "MATCH" : "MISMATCH");
        out.println();

        out.println("Performance Comparison:");
        double speedup = report.speedupFactor();
        if (speedup >= 1.0) {
            out.printf(Locale.ROOT, "Concurrent mode achieved %.2fx speedup over sequential mode.%n", speedup);
        } else {
            out.printf(Locale.ROOT,
                    "Sequential mode was %.2fx faster (concurrency coordination overhead exceeded compute gain).%n",
                    1.0 / Math.max(speedup, 0.0001));
        }

        printAggregationDetails(conc);
        out.println("============================================================");
    }

    private void printAggregationDetails(ProcessingResult result) {
        out.println("------------------------------------------------------------");
        out.println("Aggregation Summary:");
        out.printf("  Total Records:       %,d%n", result.totalRecords());
        out.printf("  Valid Records:       %,d%n", result.validRecords());
        out.printf("  Invalid (Malformed): %,d%n", result.invalidRecords());
        out.printf("  Processing Failures: %,d%n", result.processingFailures());
        out.printf("  INFO: %,d | DEBUG: %,d | WARN: %,d | ERROR: %,d%n",
                result.infoCount(), result.debugCount(), result.warnCount(), result.errorCount());

        if (!result.errorsByService().isEmpty()) {
            out.println("  Errors by Service:");
            for (Map.Entry<String, Long> entry : result.errorsByService().entrySet()) {
                out.printf("    - %-24s %,d%n", entry.getKey() + ":", entry.getValue());
            }
        }
    }
}
