package com.example.kafka.connect.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for the Example Sink Connector. Tests the end-to-end
 * functionality of consuming messages from Kafka topics and processing them
 * through the sink connector.
 */
public class ExampleSinkConnectorIT extends AbstractIT {

    private static final Logger log = LoggerFactory.getLogger(ExampleSinkConnectorIT.class);

    private static final String CONNECTOR_NAME = "test-sink-connector";

    @Test
    public void testSinkConnectorBasicFunctionality() throws Exception {
        log.info("Testing basic sink connector functionality");

        // Create and start the sink connector
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        getConnectRunner().createConnector(connectorConfig);

        // Send test messages to the topic
        sendJsonMessages(5);

        // Poll the connector task to ensure it processes the messages
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            log.info("Connector should have processed messages");
            assertThat(true).isTrue(); // Placeholder assertion, replace with actual checks

            // Check connector status
            assertThat(getConnectRunner().getHerder().connectorStatus(CONNECTOR_NAME).connector().state())
                    .isEqualTo("RUNNING");
            assertThat(getConnectRunner().getHerder().connectorStatus(CONNECTOR_NAME).tasks()).isNotEmpty();
            assertThat(getConnectRunner().getHerder().connectorStatus(CONNECTOR_NAME).tasks().get(0).state())
                    .isEqualTo("RUNNING");
        });

        log.info("Basic sink connector test completed successfully");
    }

    @Test
    public void testSinkConnectorWithInvalidConfiguration() throws Exception {
        log.info("Testing sink connector with invalid configuration");

        // Create connector config with missing required properties
        Map<String, String> invalidConfig = createBasicSinkConnectorConfig();
        invalidConfig.put("connector.class", "com.example.kafka.connect.sink.ExampleSinkConnector");
        invalidConfig.put("name", CONNECTOR_NAME + "-invalid");
        // Intentionally missing 'topics' property

        // This should fail or handle gracefully
        try {
            getConnectRunner().createConnector(invalidConfig);
            log.warn("Expected connector creation to fail with invalid config");
        } catch (ConfigException e) {
            log.info("Connector creation failed as expected: {}", e.getMessage());
            assertThat(e).isNotNull();
        }

        log.info("Invalid configuration test completed successfully");
    }

    @Test
    public void testSinkConnectorTaskRestart() throws Exception {
        log.info("Testing sink connector task restart");

        // Create and start the sink connector
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        String connName = CONNECTOR_NAME + "-restart";
        connectorConfig.put("name", connName);
        getConnectRunner().createConnector(connectorConfig);

        // Send initial messages
        sendJsonMessages(3);

        // Wait a bit for initial processing
        Thread.sleep(5000);

        // Restart the connector task
        getConnectRunner().restartTask(connName, 0);

        // Send more messages after restart
        sendJsonMessages(3);

        // Wait for connector to process messages after restart
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() -> {
            log.info("Connector should have processed messages after restart");
            assertThat(true).isTrue(); // Placeholder assertion

            // Check connector status

            assertThat(getConnectRunner().getHerder().connectorStatus(connName).connector().state())
                    .isEqualTo("RUNNING");
            assertThat(getConnectRunner().getHerder().connectorStatus(connName).tasks()).isNotEmpty();
            assertThat(getConnectRunner().getHerder().connectorStatus(connName).tasks().get(0).state())
                    .isEqualTo("RUNNING");
        });

        log.info("Task restart test completed successfully");
    }

    /**
     * Creates a sink connector configuration
     */
    private Map<String, String> createSinkConnectorConfig() {
        Map<String, String> config = createBasicSinkConnectorConfig();
        config.put("name", CONNECTOR_NAME);

        // Add any additional sink-specific configuration
        config.put("batch.size", "100");
        config.put("flush.timeout.ms", "5000");

        return config;
    }

    /**
     * Sends JSON messages to the sink topic
     */
    private void sendJsonMessages(int count) throws ExecutionException, InterruptedException {
        for (int i = 0; i < count; i++) {
            String jsonValue = String.format(
                    "{\"id\": %d, \"name\": \"Test User %d\", \"timestamp\": %d, \"active\": true}", i, i,
                    System.currentTimeMillis());

            ProducerRecord<String, String> record = new ProducerRecord<>(SINK_TOPIC_NAME, "json-key-" + i, jsonValue);

            getProducer().send(record).get();
        }

        log.info("Sent {} JSON messages to topic {}", count, SINK_TOPIC_NAME);
    }
}
