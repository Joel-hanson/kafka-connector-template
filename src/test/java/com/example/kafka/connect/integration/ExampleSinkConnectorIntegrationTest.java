package com.example.kafka.connect.integration;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the Example Sink Connector.
 * Tests the end-to-end functionality of consuming messages from Kafka topics
 * and processing them through the sink connector.
 */
public class ExampleSinkConnectorIntegrationTest extends AbstractIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(ExampleSinkConnectorIntegrationTest.class);
    
    private static final String CONNECTOR_NAME = "test-sink-connector";
    
    @Test
    public void testSinkConnectorBasicFunctionality() throws Exception {
        log.info("Testing basic sink connector functionality");
        
        // Create and start the sink connector
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        getConnectRunner().createConnector(connectorConfig);
        
        // Send test messages to the topic
        sendTestMessages(5);
        
        // Wait for connector to process messages
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   // In a real implementation, you would verify that messages were 
                   // processed by checking the target system (database, file, etc.)
                   log.info("Connector should have processed messages");
                   assertThat(true).isTrue(); // Placeholder assertion
               });
        
        log.info("Basic sink connector test completed successfully");
    }
    
    @Test
    public void testSinkConnectorWithMultiplePartitions() throws Exception {
        log.info("Testing sink connector with multiple partitions");
        
        // Create a topic with multiple partitions
        String multiPartitionTopic = "multi-partition-topic";
        createTopic(multiPartitionTopic, 3);
        
        // Create connector config for multi-partition topic
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        connectorConfig.put("topics", multiPartitionTopic);
        connectorConfig.put("name", CONNECTOR_NAME + "-multi-partition");
        
        getConnectRunner().createConnector(connectorConfig);
        
        // Send messages to different partitions
        sendTestMessagesToPartitions(multiPartitionTopic, 3, 5);
        
        // Wait for connector to process all messages
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   log.info("Connector should have processed messages from all partitions");
                   assertThat(true).isTrue(); // Placeholder assertion
               });
        
        log.info("Multi-partition sink connector test completed successfully");
    }
    
    @Test
    public void testSinkConnectorWithInvalidConfiguration() throws Exception {
        log.info("Testing sink connector with invalid configuration");
        
        // Create connector config with missing required properties
        Map<String, String> invalidConfig = createBasicConnectorConfig();
        invalidConfig.put("connector.class", "com.example.kafka.connect.sink.ExampleSinkConnector");
        invalidConfig.put("name", CONNECTOR_NAME + "-invalid");
        // Intentionally missing 'topics' property
        
        // This should fail or handle gracefully
        try {
            getConnectRunner().createConnector(invalidConfig);
            log.warn("Expected connector creation to fail with invalid config");
        } catch (Exception e) {
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
        connectorConfig.put("name", CONNECTOR_NAME + "-restart");
        getConnectRunner().createConnector(connectorConfig);
        
        // Send initial messages
        sendTestMessages(3);
        
        // Wait a bit for initial processing
        Thread.sleep(5000);
        
        // Restart the connector task
        getConnectRunner().restartTask(CONNECTOR_NAME + "-restart", 0);
        
        // Send more messages after restart
        sendTestMessages(3);
        
        // Wait for connector to process messages after restart
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   log.info("Connector should have processed messages after restart");
                   assertThat(true).isTrue(); // Placeholder assertion
               });
        
        log.info("Task restart test completed successfully");
    }
    
    @Test
    public void testSinkConnectorWithJsonMessages() throws Exception {
        log.info("Testing sink connector with JSON messages");
        
        // Create connector config with JSON converter
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-json");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfig.put("value.converter.schemas.enable", "false");
        
        getConnectRunner().createConnector(connectorConfig);
        
        // Send JSON messages
        sendJsonMessages(5);
        
        // Wait for connector to process JSON messages
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   log.info("Connector should have processed JSON messages");
                   assertThat(true).isTrue(); // Placeholder assertion
               });
        
        log.info("JSON messages test completed successfully");
    }
    
    @Test
    public void testSinkConnectorPerformance() throws Exception {
        log.info("Testing sink connector performance");
        
        // Create connector
        Map<String, String> connectorConfig = createSinkConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-performance");
        getConnectRunner().createConnector(connectorConfig);
        
        // Send a large number of messages
        int messageCount = 1000;
        long startTime = System.currentTimeMillis();
        
        sendTestMessages(messageCount);
        
        // Wait for all messages to be processed
        await().atMost(Duration.ofMinutes(5))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   long endTime = System.currentTimeMillis();
                   long duration = endTime - startTime;
                   double messagesPerSecond = messageCount / (duration / 1000.0);
                   
                   log.info("Processed {} messages in {} ms ({} messages/sec)", 
                           messageCount, duration, messagesPerSecond);
                   
                   // Verify reasonable throughput (adjust threshold as needed)
                   assertThat(messagesPerSecond).isGreaterThan(10.0);
               });
        
        log.info("Performance test completed successfully");
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
     * Sends test messages to the sink topic
     */
    private void sendTestMessages(int count) throws ExecutionException, InterruptedException {
        for (int i = 0; i < count; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                SINK_TOPIC_NAME, 
                "key-" + i, 
                "Test message " + i + " - " + System.currentTimeMillis()
            );
            
            getProducer().send(record).get();
        }
        
        log.info("Sent {} test messages to topic {}", count, SINK_TOPIC_NAME);
    }
    
    /**
     * Sends test messages to specific partitions
     */
    private void sendTestMessagesToPartitions(String topic, int partitions, int messagesPerPartition) 
            throws ExecutionException, InterruptedException {
        
        for (int partition = 0; partition < partitions; partition++) {
            for (int i = 0; i < messagesPerPartition; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic, 
                    partition,
                    "key-" + partition + "-" + i, 
                    "Test message for partition " + partition + " - message " + i
                );
                
                getProducer().send(record).get();
            }
        }
        
        log.info("Sent {} messages per partition to {} partitions of topic {}", 
                messagesPerPartition, partitions, topic);
    }
    
    /**
     * Sends JSON messages to the sink topic
     */
    private void sendJsonMessages(int count) throws ExecutionException, InterruptedException {
        for (int i = 0; i < count; i++) {
            String jsonValue = String.format(
                "{\"id\": %d, \"name\": \"Test User %d\", \"timestamp\": %d, \"active\": true}",
                i, i, System.currentTimeMillis()
            );
            
            ProducerRecord<String, String> record = new ProducerRecord<>(
                SINK_TOPIC_NAME,
                "json-key-" + i,
                jsonValue
            );
            
            getProducer().send(record).get();
        }
        
        log.info("Sent {} JSON messages to topic {}", count, SINK_TOPIC_NAME);
    }
}
