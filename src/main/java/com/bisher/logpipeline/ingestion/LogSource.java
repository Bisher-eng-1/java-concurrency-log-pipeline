package com.bisher.logpipeline.ingestion;

import java.io.IOException;
import java.util.Iterator;

public interface LogSource extends Iterable<String>, AutoCloseable {

    String description();

    @Override
    Iterator<String> iterator();

    @Override
    default void close() throws IOException {
        // Default no-op for in-memory streaming generators
    }
}
