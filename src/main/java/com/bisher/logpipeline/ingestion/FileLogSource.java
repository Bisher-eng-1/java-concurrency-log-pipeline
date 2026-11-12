package com.bisher.logpipeline.ingestion;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Streaming file-backed {@link LogSource} that reads line by line using a buffered reader.
 */
public class FileLogSource implements LogSource {

    private final Path filePath;
    private BufferedReader activeReader;

    public FileLogSource(Path filePath) throws IOException {
        this.filePath = Objects.requireNonNull(filePath, "filePath must not be null");
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException("Log input file not found: " + filePath.toAbsolutePath());
        }
        if (!Files.isRegularFile(filePath)) {
            throw new IOException("Log input path is not a regular file: " + filePath.toAbsolutePath());
        }
    }

    @Override
    public String description() {
        return "File stream (" + filePath.toAbsolutePath() + ")";
    }

    @Override
    public Iterator<String> iterator() {
        try {
            close();
            this.activeReader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
            return new BufferedLineIterator(activeReader);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to open log file: " + filePath, ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (activeReader != null) {
            activeReader.close();
            activeReader = null;
        }
    }

    private static final class BufferedLineIterator implements Iterator<String> {
        private final BufferedReader reader;
        private String nextLine;
        private boolean finished = false;

        BufferedLineIterator(BufferedReader reader) {
            this.reader = reader;
            advance();
        }

        private void advance() {
            try {
                nextLine = reader.readLine();
                if (nextLine == null) {
                    finished = true;
                }
            } catch (IOException ex) {
                throw new UncheckedIOException("Error reading from log file", ex);
            }
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public String next() {
            if (finished) {
                throw new NoSuchElementException("End of file stream reached");
            }
            String current = nextLine;
            advance();
            return current;
        }
    }
}
