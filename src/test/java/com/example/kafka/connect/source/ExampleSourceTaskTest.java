package com.example.kafka.connect.source;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExampleSourceTaskTest {

    private ExampleSourceTask task;
    private Map<String, String> config;

    @BeforeEach
    void setUp() {
        task = new ExampleSourceTask();
        config = new HashMap<>();
        config.put("topic", "test-topic");
        config.put("poll.interval.ms", "5000");
        config.put("batch.size", "10");

        // Mock the context and offset storage
        SourceTaskContext context = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = Mockito.mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(Collections.singletonMap("source", "example")))
                .thenReturn(Collections.singletonMap("offset", 0));

        task.initialize(context);
        task.start(config);
    }

    @Test
    void testPollReturnsRecords() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertEquals(10, records.size()); // batch.size = 10
        assertEquals(Long.valueOf(0), records.get(0).key());
        assertNotNull(records.get(0).value());
    }

    @Test
    void testVersion() {
        assertEquals("0.0.2", task.version());
    }

    @Test
    void testStop() {
        assertDoesNotThrow(() -> task.stop());
    }
}