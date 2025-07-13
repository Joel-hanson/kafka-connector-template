package com.example.kafka.connect.sink;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example implementation of a Kafka Connect Sink Task. This task is responsible
 * for: 1. Connecting to the target system 2. Processing batches of records from
 * Kafka topics 3. Writing those records to the target system
 */
public class ExampleSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(ExampleSinkTask.class);

    private ExampleSinkConnectorConfig config;
    private int remainingRetries;
    private long retryBackoffMs;

    @Override
    public String version() {
        return "0.0.2";
    }

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting Example Sink Task");
        config = new ExampleSinkConnectorConfig(props);

        // Get configuration values
        remainingRetries = config.getMaxRetries();
        retryBackoffMs = config.getRetryBackoffMs();

        // Initialize connection to target system
        initializeConnection();

        log.info("Example Sink Task started");
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        final int recordsCount = records.size();
        log.debug("Received {} records", recordsCount);

        try {
            // Process and write records to the target system
            for (final SinkRecord record : records) {
                processRecord(record);
            }

            // If we got here, we succeeded, so we can reset the retry counter
            remainingRetries = config.getMaxRetries();

        } catch (final RetriableException e) {
            // If it's a retriable exception, we'll retry
            if (remainingRetries > 0) {
                remainingRetries--;
                log.warn("Retriable exception occurred, retrying ({} attempts remaining): {}", remainingRetries,
                        e.getMessage());
                context.timeout(retryBackoffMs);
                throw e; // Re-throwing the exception to trigger the retry
            } else {
                log.error("Retriable exception occurred but no retries remaining", e);
                throw new ConnectException("No more retries left for retriable exception", e);
            }
        } catch (final Exception e) {
            // For other exceptions, we fail the task
            log.error("Error processing records", e);
            throw new ConnectException("Error processing records", e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Example Sink Task");
        // Close any resources or connections
        closeConnection();
        log.info("Example Sink Task stopped");
    }

    // Optional: Override flush() if you need to perform any batch operations
    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("Flushing offsets: {}", offsets);
        // Perform any necessary flush operations
    }

    // Process a single record
    private void processRecord(final SinkRecord record) {
        // Extract data from the record based on its schema
        final Map<String, Object> values = new HashMap<>();

        if (record.value() instanceof Struct) {
            // Handle structured data with schema
            final Struct struct = (Struct) record.value();
            final Schema schema = struct.schema();

            for (final Field field : schema.fields()) {
                final String fieldName = field.name();
                final Object fieldValue = struct.get(field);
                values.put(fieldName, fieldValue);
            }
        } else if (record.value() instanceof Map) {
            // Handle schemaless data (Map)
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>) record.value();
            values.putAll(map);
        } else if (record.value() != null) {
            // Handle primitive types
            values.put("value", record.value());
        }

        // Write the data to the target system
        writeToTargetSystem(record.topic(), record.kafkaPartition(), record.kafkaOffset(), values);
    }

    // Example method to write data to the target system
    private void writeToTargetSystem(final String topic, final Integer partition, final Long offset,
            final Map<String, Object> values) {
        log.debug("Writing record from topic {}, partition {}, offset {} to target system", topic, partition, offset);

        // Implement the logic to write data to your target system
        // This depends on the specific target system (database, file, API, etc.)

        // Example implementation (just logging)
        log.info("Writing data: {}", values);
    }

    // Initialize connection to the target system
    private void initializeConnection() {
        try {
            log.info("Initializing connection to external system");
            // Implement connection initialization logic
            // This depends on the specific target system

        } catch (final Exception e) {
            log.error("Failed to initialize connection", e);
            throw new ConnectException("Failed to initialize connection", e);
        }
    }

    // Close connection to the target system
    private void closeConnection() {
        try {
            log.info("Closing connection");
            // Implement connection closing logic

        } catch (final Exception e) {
            log.warn("Error closing connection", e);
        }
    }
}
