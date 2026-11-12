package com.bisher.logpipeline.ingestion;

import java.io.IOException;
import java.util.Iterator;

/**
 * Streaming source of raw log records.
 * Implementations yield records lazily so large datasets do not need to reside fully in heap memory.
 */
public interface LogSource extends Iterable<String>, AutoCloseable {

    /**
     * Human-readable description of the log source for reporting.
     */
    String description();

    @Override
    Iterator<String> iterator();

    @Override
    default void close() throws IOException {
        // Default no-op for in-memory streaming generators
    }
}
