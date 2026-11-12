package com.bisher.logpipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogProcessorCliTest {

    @Test
    void processesValidInputFileSuccessfully(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("test.log");
        Files.writeString(logFile, """
                2026-01-15T08:00:01Z|INFO|api-gateway|evt-1|ok
                2026-01-15T08:00:02Z|ERROR|payment-worker|evt-2|failed
                BAD_LINE
                """);

        int exitCode = LogProcessor.run(new String[]{
                "--mode", "compare",
                "--workers", "2",
                "--queue-capacity", "16",
                "--repeats", "1",
                "--warmup", "0",
                "--input", logFile.toString()
        });
        assertEquals(0, exitCode);
    }

    @Test
    void returnsNonZeroExitCodeForMissingInputFile() {
        int exitCode = LogProcessor.run(new String[]{
                "--mode", "sequential",
                "--input", "/nonexistent/path/missing-logs.txt"
        });
        assertEquals(1, exitCode);
    }

    @Test
    void returnsNonZeroExitCodeForInvalidCliArguments() {
        int exitCode = LogProcessor.run(new String[]{
                "--mode", "invalid-mode"
        });
        assertEquals(1, exitCode);
    }
}
