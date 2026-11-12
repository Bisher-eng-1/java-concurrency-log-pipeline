package com.bisher.logpipeline.concurrency;

import com.bisher.logpipeline.ingestion.LogSource;
import com.bisher.logpipeline.ingestion.SyntheticLogGenerator;
import com.bisher.logpipeline.model.ProcessingResult;
import com.bisher.logpipeline.processing.LogParser;
import com.bisher.logpipeline.processing.RecordProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineExecutionTest {

    @Test
    void handlesEmptyInputInBothModes() throws Exception {
        LogSource emptySource = new LogSource() {
            @Override
            public String description() {
                return "empty";
            }

            @Override
            public Iterator<String> iterator() {
                return Collections.emptyIterator();
            }
        };

        RecordProcessor processor = new RecordProcessor(new LogParser(), 0);
        ProcessingResult seq = new SequentialPipeline(processor).execute(emptySource);
        ProcessingResult conc = new BoundedLogPipeline(processor, 4, 32).execute(emptySource);

        assertEquals(0, seq.totalRecords());
        assertEquals(0, conc.totalRecords());
        assertTrue(seq.isLogicallyEquivalent(conc));
    }

    @Test
    void processesFiftyThousandSyntheticEventsWithIdenticalAggregation() throws Exception {
        int events = 50_000;
        long seed = 1337L;
        double malformedRate = 0.02;

        RecordProcessor processor = new RecordProcessor(new LogParser(), 10);
        LogSource seqSource = new SyntheticLogGenerator(events, seed, malformedRate);
        LogSource concSource = new SyntheticLogGenerator(events, seed, malformedRate);

        ProcessingResult seq = new SequentialPipeline(processor).execute(seqSource);
        ProcessingResult conc = new BoundedLogPipeline(processor, 4, 1000).execute(concSource);

        assertEquals(events, seq.totalRecords());
        assertTrue(seq.invalidRecords() > 0, "Expected malformed synthetic records");
        assertTrue(seq.errorCount() > 0, "Expected ERROR severity records");
        assertTrue(seq.isLogicallyEquivalent(conc),
                "Sequential and concurrent aggregations must be strictly equivalent");
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1",
            "2, 16",
            "4, 256",
            "8, 1000"
    })
    void supportsVariedWorkerCountsAndQueueCapacities(int workers, int queueCapacity) throws Exception {
        int events = 5_000;
        RecordProcessor processor = new RecordProcessor(new LogParser(), 5);

        ProcessingResult seq = new SequentialPipeline(processor)
                .execute(new SyntheticLogGenerator(events, 99L, 0.03));
        ProcessingResult conc = new BoundedLogPipeline(processor, workers, queueCapacity)
                .execute(new SyntheticLogGenerator(events, 99L, 0.03));

        assertEquals(events, conc.totalRecords());
        assertTrue(seq.isLogicallyEquivalent(conc));
    }

    @Test
    void handlesMixedValidAndMalformedLinesDeterministically() throws Exception {
        List<String> lines = List.of(
                "2026-01-15T08:00:01Z|INFO|api-gateway|e1|ok",
                "BROKEN_LINE",
                "2026-01-15T08:00:02Z|ERROR|auth-service|e2|denied",
                "2026-01-15T08:00:03Z|WARN|cache-node|e3|slow",
                "2026-01-15T08:00:04Z|DEBUG|auth-service|e4|trace",
                "2026-01-15T08:00:05Z|ERROR|auth-service|e5|timeout"
        );
        LogSource fixedSource = new LogSource() {
            @Override
            public String description() {
                return "fixed";
            }

            @Override
            public Iterator<String> iterator() {
                return lines.iterator();
            }
        };

        RecordProcessor processor = new RecordProcessor(new LogParser(), 0);
        ProcessingResult seq = new SequentialPipeline(processor).execute(fixedSource);
        ProcessingResult conc = new BoundedLogPipeline(processor, 3, 2).execute(fixedSource);

        assertEquals(6, seq.totalRecords());
        assertEquals(5, seq.validRecords());
        assertEquals(1, seq.invalidRecords());
        assertEquals(2L, seq.errorsByService().get("auth-service"));
        assertTrue(seq.isLogicallyEquivalent(conc));
    }
}
