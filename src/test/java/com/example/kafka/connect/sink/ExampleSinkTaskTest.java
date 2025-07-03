package com.example.kafka.connect.sink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExampleSinkTaskTest {

    private ExampleSinkTask task;
    private Map<String, String> config;
    private final String TEST_OUTPUT_FILE = "test-sink-output.txt";

    @BeforeEach
    void setUp() {
        task = new ExampleSinkTask();
        config = new HashMap<>();
        config.put("output.file", TEST_OUTPUT_FILE);
        config.put("topics", "test-topic");
        config.put("flush.size", "2"); // Small batch for testing
        task.start(config);
    }

    @Test
    void testFlush() {
        assertDoesNotThrow(() -> task.flush(null)); // No offsets to commit
    }

    @Test
    void testVersion() {
        assertEquals("1.0.0", task.version());
    }
}