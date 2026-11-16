package com.bisher.logpipeline;

import com.bisher.logpipeline.benchmark.BenchmarkRunner;
import com.bisher.logpipeline.benchmark.BenchmarkRunner.ComparisonReport;
import com.bisher.logpipeline.concurrency.BoundedLogPipeline;
import com.bisher.logpipeline.concurrency.SequentialPipeline;
import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.model.PipelineConfig;
import com.bisher.logpipeline.model.PipelineConfig.ExecutionMode;
import com.bisher.logpipeline.model.PipelineConfig.HelpRequestedException;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.LogParser;
import com.bisher.logpipeline.processing.RecordProcessor;
import com.bisher.logpipeline.reporting.ConsoleReporter;

import java.io.IOException;

public final class LogProcessor {

    private LogProcessor() {
    }

    public static void main(String[] args) {
        int exitCode = run(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int run(String[] args) {
        PipelineConfig config;
        try {
            config = PipelineConfig.fromArgs(args);
        } catch (HelpRequestedException ex) {
            printUsage();
            return 0;
        } catch (IllegalArgumentException ex) {
            System.err.println("Configuration error: " + ex.getMessage());
            printUsage();
            return 1;
        }

        ConsoleReporter reporter = new ConsoleReporter(System.out);
        RecordProcessor recordProcessor = new RecordProcessor(new LogParser(), config.workIterations());

        try {
            switch (config.mode()) {
                case SEQUENTIAL -> {
                    SequentialPipeline pipeline = new SequentialPipeline(recordProcessor);
                    try (LogSource source = BenchmarkRunner.openSource(config)) {
                        ProcessingResult result = pipeline.execute(source);
                        reporter.printSingleResult(result, source.description());
                    }
                }
                case CONCURRENT -> {
                    BoundedLogPipeline pipeline = new BoundedLogPipeline(
                            recordProcessor, config.workers(), config.queueCapacity());
                    try (LogSource source = BenchmarkRunner.openSource(config)) {
                        ProcessingResult result = pipeline.execute(source);
                        reporter.printSingleResult(result, source.description());
                    }
                }
                case COMPARE -> {
                    BenchmarkRunner runner = new BenchmarkRunner();
                    ComparisonReport report = runner.runComparison(config);
                    reporter.printComparisonReport(report);
                    if (!report.outputsMatch()) {
                        System.err.println("ERROR: Sequential and concurrent outputs diverged!");
                        return 2;
                    }
                }
            }
            return 0;
        } catch (IOException ex) {
            System.err.println("I/O error while reading log source: " + ex.getMessage());
            return 1;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            System.err.println("Execution interrupted before completion.");
            return 1;
        } catch (RuntimeException ex) {
            System.err.println("Fatal pipeline error: " + ex.getMessage());
            return 1;
        }
    }

    private static void printUsage() {
        System.out.println("""
                Usage: java -jar target/java-concurrency-log-pipeline.jar [OPTIONS]

                Options:
                  --mode <sequential|concurrent|compare>  Execution mode (default: compare)
                  --workers <int>                         Worker thread pool size (default: 4)
                  --queue-capacity <int>                  Bounded BlockingQueue capacity (default: 1000)
                  --events <int>                          Synthetic events to generate (default: 50000)
                  --seed <long>                           Fixed PRNG seed for reproducibility (default: 42)
                  --malformed-rate <double>               Fraction of malformed lines [0.0-1.0] (default: 0.01)
                  --repeats <int>                         Measured runs in compare mode (default: 3)
                  --warmup <int>                          Warm-up runs discarded in compare mode (default: 1)
                  --work-iterations <int>                 Per-record deterministic compute loops (default: 50)
                  --input <path>                          Optional input log file instead of synthetic generator
                  --help, -h                              Show this usage guide
                """);
    }
}
