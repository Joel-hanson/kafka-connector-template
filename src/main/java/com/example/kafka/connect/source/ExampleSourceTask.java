package com.example.kafka.connect.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.utils.ExampleDataRecord;

/**
 * Example implementation of a Kafka Connect Source Task. This task is
 * responsible for: 1. Connecting to the external data source 2. Reading data
 * from the source system 3. Creating SourceRecords and returning them to the
 * Kafka Connect framework
 */
public class ExampleSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(ExampleSourceTask.class);
    private static final String POSITION_FIELD = "position";

    private ExampleSourceConnectorConfig config;
    private String topic;
    private int batchSize;
    private long pollIntervalMs;

    // Source offset tracking
    private AtomicLong currentPosition = new AtomicLong();
    private Map<String, Object> offset;

    // Schema for generated data
    private Schema valueSchema;

    @Override
    public String version() {
        return "0.0.2";
    }

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting Example Source Task");
        config = new ExampleSourceConnectorConfig(props);

        // Get configuration values
        topic = config.getTopic();
        batchSize = config.getBatchSize();
        pollIntervalMs = config.getPollIntervalMs();

        // Initialize connection to external system
        initializeConnection();

        // Define schema for the records
        valueSchema = SchemaBuilder.struct().name("exampleRecord").field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA).field("value", Schema.FLOAT64_SCHEMA)
                .field("timestamp", Schema.INT64_SCHEMA).build();

        // Get last saved offset, if it exists
        offset = context.offsetStorageReader().offset(sourcePartition());
        if (offset != null && offset.containsKey(POSITION_FIELD)) {
            currentPosition.set((Long) offset.get(POSITION_FIELD));
            log.info("Loaded offset: {}", currentPosition.get());
        } else {
            // No stored offset, start from the beginning
            currentPosition.set(0L);
            log.info("No stored offset, starting from 0");
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // If pollIntervalMs > 0, sleep to avoid tight loops
        if (pollIntervalMs > 0) {
            Thread.sleep(pollIntervalMs);
        }

        // Check if there's data to fetch
        if (!hasNextBatch()) {
            return Collections.emptyList();
        }

        final List<SourceRecord> records = new ArrayList<>(batchSize);

        try {
            // Fetch a batch of data from the external system
            for (int i = 0; i < batchSize && hasNext(); i++) {
                // Get the next record from the external system
                final ExampleDataRecord data = getNextRecord();

                if (data != null) {
                    // Create a structured record
                    final Struct valueStruct = new Struct(valueSchema).put("id", data.getId())
                            .put("name", data.getName()).put("value", data.getValue())
                            .put("timestamp", data.getTimestamp());

                    // Create a SourceRecord
                    final SourceRecord record = new SourceRecord(sourcePartition(), // Source partition - uniquely
                                                                                    // identifies the source stream
                            sourceOffset(), // Source offset - position in the source stream
                            topic, // Destination Kafka topic
                            null, // Optional: partition to write to in the Kafka topic
                            null, // Optional: key schema
                            data.getId(), // Optional: record key
                            valueSchema, // Value schema
                            valueStruct); // Record value

                    records.add(record);
                    currentPosition.incrementAndGet();
                }
            }
        } catch (final Exception e) {
            log.error("Error fetching data from source system", e);
        }

        if (!records.isEmpty()) {
            log.debug("Returning {} records", records.size());
        }

        return records;
    }

    @Override
    public void stop() {
        log.info("Stopping Example Source Task");
        // Close any resources or connections
        closeConnection();
    }

    /**
     * Returns a map that defines the partition of the source this task is handling.
     * This should uniquely identify the "stream" or "partition" of data from the
     * source system.
     */
    private Map<String, String> sourcePartition() {
        final Map<String, String> partition = new HashMap<>();
        partition.put("source", "example-source");
        // Add any source-specific partition identifiers
        return partition;
    }

    /**
     * Returns a map representing the position/offset in the source partition. This
     * is used to track the position in the source so that the connector can resume
     * from the correct position after a restart.
     */
    private Map<String, Object> sourceOffset() {
        final Map<String, Object> offset = new HashMap<>();
        offset.put(POSITION_FIELD, currentPosition.get());
        return offset;
    }

    // Example method to check if there's more data to fetch
    private boolean hasNext() {
        // Replace with actual logic to check if there's more data in the source
        return true; // For demonstration, always has more data
    }

    // Example method to check if there's a batch available
    private boolean hasNextBatch() {
        // Replace with actual logic to check if there's a batch available
        return true; // For demonstration, always has a batch
    }

    // Example method to fetch the next record from the external system
    private ExampleDataRecord getNextRecord() {
        // Replace with actual logic to fetch data from the source
        final long position = currentPosition.get();

        // This is a mock implementation - replace with actual data retrieval
        return new ExampleDataRecord(position, "record-" + position, Math.random() * 100, System.currentTimeMillis());
    }

    // Initialize connection to the external system
    private void initializeConnection() {
        // Example: Initialize database connection or API client
        log.info("Initializing connection to externla system");
        // Implementation details depend on the specific source system
    }

    // Close connection to the external system
    private void closeConnection() {
        // Example: Close database connection or API client
        log.info("Closing connection");
        // Implementation details depend on the specific source system
    }
}
