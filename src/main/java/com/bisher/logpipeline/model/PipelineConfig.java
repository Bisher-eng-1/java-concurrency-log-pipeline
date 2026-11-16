package com.bisher.logpipeline.model;

import java.nio.file.Path;
import java.util.Optional;

public record PipelineConfig(
        ExecutionMode mode,
        int workers,
        int queueCapacity,
        int events,
        long seed,
        double malformedRate,
        int repeats,
        int warmupRuns,
        int workIterations,
        Optional<Path> inputFile
) {
    public enum ExecutionMode {
        SEQUENTIAL,
        CONCURRENT,
        COMPARE;

        public static ExecutionMode parse(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Execution mode must not be null");
            }
            return switch (value.trim().toLowerCase()) {
                case "sequential" -> SEQUENTIAL;
                case "concurrent" -> CONCURRENT;
                case "compare" -> COMPARE;
                default -> throw new IllegalArgumentException(
                        "Unsupported --mode '" + value + "'. Allowed: sequential, concurrent, compare");
            };
        }
    }

    public static final int DEFAULT_WORKERS = Math.max(2, Runtime.getRuntime().availableProcessors());
    public static final int DEFAULT_QUEUE_CAPACITY = 1000;
    public static final int DEFAULT_EVENTS = 50_000;
    public static final long DEFAULT_SEED = 42L;
    public static final double DEFAULT_MALFORMED_RATE = 0.01;
    public static final int DEFAULT_REPEATS = 3;
    public static final int DEFAULT_WARMUP_RUNS = 1;
    public static final int DEFAULT_WORK_ITERATIONS = 50;

    public PipelineConfig {
        if (workers < 1) {
            throw new IllegalArgumentException("--workers must be >= 1");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("--queue-capacity must be >= 1");
        }
        if (events < 0) {
            throw new IllegalArgumentException("--events must be >= 0");
        }
        if (malformedRate < 0.0 || malformedRate > 1.0) {
            throw new IllegalArgumentException("--malformed-rate must be between 0.0 and 1.0");
        }
        if (repeats < 1) {
            throw new IllegalArgumentException("--repeats must be >= 1");
        }
        if (warmupRuns < 0) {
            throw new IllegalArgumentException("--warmup must be >= 0");
        }
        if (workIterations < 0) {
            throw new IllegalArgumentException("--work-iterations must be >= 0");
        }
    }

    public static PipelineConfig defaults() {
        return new PipelineConfig(
                ExecutionMode.COMPARE,
                4,
                DEFAULT_QUEUE_CAPACITY,
                DEFAULT_EVENTS,
                DEFAULT_SEED,
                DEFAULT_MALFORMED_RATE,
                DEFAULT_REPEATS,
                DEFAULT_WARMUP_RUNS,
                DEFAULT_WORK_ITERATIONS,
                Optional.empty()
        );
    }

    public static PipelineConfig fromArgs(String[] args) {
        ExecutionMode mode = ExecutionMode.COMPARE;
        int workers = 4;
        int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        int events = DEFAULT_EVENTS;
        long seed = DEFAULT_SEED;
        double malformedRate = DEFAULT_MALFORMED_RATE;
        int repeats = DEFAULT_REPEATS;
        int warmupRuns = DEFAULT_WARMUP_RUNS;
        int workIterations = DEFAULT_WORK_ITERATIONS;
        Path inputFile = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--mode" -> mode = ExecutionMode.parse(requireValue(args, ++i, "--mode"));
                case "--workers" -> workers = parseInt(requireValue(args, ++i, "--workers"), "--workers");
                case "--queue-capacity" -> queueCapacity = parseInt(requireValue(args, ++i, "--queue-capacity"), "--queue-capacity");
                case "--events" -> events = parseInt(requireValue(args, ++i, "--events"), "--events");
                case "--seed" -> seed = parseLong(requireValue(args, ++i, "--seed"), "--seed");
                case "--malformed-rate" -> malformedRate = parseDouble(requireValue(args, ++i, "--malformed-rate"), "--malformed-rate");
                case "--repeats" -> repeats = parseInt(requireValue(args, ++i, "--repeats"), "--repeats");
                case "--warmup" -> warmupRuns = parseInt(requireValue(args, ++i, "--warmup"), "--warmup");
                case "--work-iterations" -> workIterations = parseInt(requireValue(args, ++i, "--work-iterations"), "--work-iterations");
                case "--input" -> inputFile = Path.of(requireValue(args, ++i, "--input"));
                case "--help", "-h" -> throw new HelpRequestedException();
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        return new PipelineConfig(
                mode,
                workers,
                queueCapacity,
                events,
                seed,
                malformedRate,
                repeats,
                warmupRuns,
                workIterations,
                Optional.ofNullable(inputFile)
        );
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing value for flag: " + flag);
        }
        return args[index];
    }

    private static int parseInt(String raw, String flag) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid integer for " + flag + ": " + raw, ex);
        }
    }

    private static long parseLong(String raw, String flag) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid long for " + flag + ": " + raw, ex);
        }
    }

    private static double parseDouble(String raw, String flag) {
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid decimal for " + flag + ": " + raw, ex);
        }
    }

    public static final class HelpRequestedException extends RuntimeException {
    }
}
