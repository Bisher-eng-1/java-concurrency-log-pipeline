package com.bisher.logpipeline.benchmark;

import com.bisher.logpipeline.concurrency.BoundedLogPipeline;
import com.bisher.logpipeline.concurrency.SequentialPipeline;
import com.bisher.logpipeline.ingestion.FileLogSource;
import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.ingestion.SyntheticLogGenerator;
import com.bisher.logpipeline.model.PipelineConfig;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.LogParser;
import com.bisher.logpipeline.processing.RecordProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Executes indicative Sequential vs. Concurrent benchmarks over identical datasets.
 * <p>
 * Note on methodology: This runner executes in-process wall-clock timing after optional
 * warm-up runs to let C2 JIT compilation stabilize. It provides an indicative application-level
 * comparison rather than a JMH-isolated microbenchmark.
 */
public final class BenchmarkRunner {

    public record ComparisonReport(
            String datasetDescription,
            int workers,
            int queueCapacity,
            int warmupRuns,
            int measuredRepeats,
            ProcessingResult representativeSequential,
            ProcessingResult representativeConcurrent,
            double avgSequentialMillis,
            double avgConcurrentMillis,
            double avgSequentialThroughput,
            double avgConcurrentThroughput,
            boolean outputsMatch
    ) {
        public double speedupFactor() {
            if (avgConcurrentMillis <= 0.0) {
                return 0.0;
            }
            return avgSequentialMillis / avgConcurrentMillis;
        }
    }

    public ComparisonReport runComparison(PipelineConfig config) throws IOException, InterruptedException {
        Objects.requireNonNull(config, "config must not be null");
        RecordProcessor processor = new RecordProcessor(new LogParser(), config.workIterations());
        SequentialPipeline sequential = new SequentialPipeline(processor);
        BoundedLogPipeline concurrent = new BoundedLogPipeline(processor, config.workers(), config.queueCapacity());

        String description;
        try (LogSource probe = openSource(config)) {
            description = probe.description();
        }

        // Warm-up phase (discarded from measurements to mitigate cold JIT interpretation)
        for (int w = 0; w < config.warmupRuns(); w++) {
            try (LogSource seqSrc = openSource(config);
                 LogSource concSrc = openSource(config)) {
                sequential.execute(seqSrc);
                concurrent.execute(concSrc);
            }
        }

        List<ProcessingResult> seqResults = new ArrayList<>(config.repeats());
        List<ProcessingResult> concResults = new ArrayList<>(config.repeats());
        boolean allEquivalent = true;

        for (int r = 0; r < config.repeats(); r++) {
            ProcessingResult seqRes;
            ProcessingResult concRes;

            // Alternate execution order across repeats to reduce thermal/GC bias
            if (r % 2 == 0) {
                try (LogSource s1 = openSource(config); LogSource s2 = openSource(config)) {
                    seqRes = sequential.execute(s1);
                    concRes = concurrent.execute(s2);
                }
            } else {
                try (LogSource s1 = openSource(config); LogSource s2 = openSource(config)) {
                    concRes = concurrent.execute(s1);
                    seqRes = sequential.execute(s2);
                }
            }

            if (!seqRes.isLogicallyEquivalent(concRes)) {
                allEquivalent = false;
            }
            seqResults.add(seqRes);
            concResults.add(concRes);
        }

        double avgSeqMs = seqResults.stream().mapToDouble(ProcessingResult::durationMillis).average().orElse(0.0);
        double avgConcMs = concResults.stream().mapToDouble(ProcessingResult::durationMillis).average().orElse(0.0);
        double avgSeqTput = seqResults.stream().mapToDouble(ProcessingResult::throughputRecordsPerSec).average().orElse(0.0);
        double avgConcTput = concResults.stream().mapToDouble(ProcessingResult::throughputRecordsPerSec).average().orElse(0.0);

        return new ComparisonReport(
                description,
                config.workers(),
                config.queueCapacity(),
                config.warmupRuns(),
                config.repeats(),
                seqResults.get(0),
                concResults.get(0),
                avgSeqMs,
                avgConcMs,
                avgSeqTput,
                avgConcTput,
                allEquivalent
        );
    }

    public static LogSource openSource(PipelineConfig config) throws IOException {
        if (config.inputFile().isPresent()) {
            return new FileLogSource(config.inputFile().get());
        }
        return new SyntheticLogGenerator(config.events(), config.seed(), config.malformedRate());
    }
}
