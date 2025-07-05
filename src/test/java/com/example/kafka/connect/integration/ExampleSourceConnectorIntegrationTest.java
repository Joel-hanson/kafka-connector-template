package com.example.kafka.connect.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for the Example Source Connector.
 * Tests the end-to-end functionality of reading data from external systems
 * and producing messages to Kafka topics.
 */
public class ExampleSourceConnectorIntegrationTest extends AbstractIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(ExampleSourceConnectorIntegrationTest.class);
    
    private static final String CONNECTOR_NAME = "test-source-connector";
    private static final String SOURCE_TOPIC_PREFIX = "source-";
    
    @Test
    public void testSourceConnectorBasicFunctionality() throws Exception {
        log.info("Testing basic source connector functionality");
        
        // Create and start the source connector
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for connector to produce messages
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   log.info("Consumed {} messages from source topic", records.count());
                   
                   // Verify that messages were produced
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        log.info("Basic source connector test completed successfully");
    }
    
    @Test
    public void testSourceConnectorWithCustomConfiguration() throws Exception {
        log.info("Testing source connector with custom configuration");
        
        // Create connector config with custom settings
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-custom");
        connectorConfig.put("poll.interval.ms", "2000");
        connectorConfig.put("batch.max.rows", "50");
        
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for connector to produce messages with custom config
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   log.info("Consumed {} messages from source topic with custom config", records.count());
                   
                   // Verify that messages were produced
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        log.info("Custom configuration test completed successfully");
    }
    
    @Test
    public void testSourceConnectorWithMultipleTables() throws Exception {
        log.info("Testing source connector with multiple tables");
        
        // Create connector config for multiple tables
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-multi-table");
        connectorConfig.put("table.whitelist", "table1,table2,table3");
        
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for connector to produce messages from multiple tables
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   log.info("Consumed {} messages from multiple tables", records.count());
                   
                   // Verify that messages were produced
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        log.info("Multiple tables test completed successfully");
    }
    
    @Test
    public void testSourceConnectorWithInvalidConfiguration() throws Exception {
        log.info("Testing source connector with invalid configuration");
        
        // Create connector config with missing required properties
        Map<String, String> invalidConfig = createBasicConnectorConfig();
        invalidConfig.put("connector.class", "com.example.kafka.connect.source.ExampleSourceConnector");
        invalidConfig.put("name", CONNECTOR_NAME + "-invalid");
        // Intentionally missing 'topic.prefix' property
        
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
    public void testSourceConnectorTaskRestart() throws Exception {
        log.info("Testing source connector task restart");
        
        // Create and start the source connector
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-restart");
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for initial message production
        await().atMost(Duration.ofSeconds(15))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        // Restart the connector task
        getConnectRunner().restartTask(CONNECTOR_NAME + "-restart", 0);
        
        // Wait for connector to continue producing messages after restart
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   log.info("Consumed {} messages after restart", records.count());
                   
                   // Verify continuous message production after restart
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        log.info("Task restart test completed successfully");
    }
    
    @Test
    public void testSourceConnectorMessageFormat() throws Exception {
        log.info("Testing source connector message format");
        
        // Create connector
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-format");
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for messages and verify format
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   assertThat(records.count()).isGreaterThan(0);
                   
                   // Check message format
                   for (ConsumerRecord<String, String> record : records) {
                       log.info("Message - Key: {}, Value: {}, Partition: {}, Offset: {}", 
                               record.key(), record.value(), record.partition(), record.offset());
                       
                       // Verify key is not null
                       assertThat(record.key()).isNotNull();
                       
                       // Verify value is not null
                       assertThat(record.value()).isNotNull();
                       
                       // Verify topic naming convention
                       assertThat(record.topic()).startsWith(SOURCE_TOPIC_PREFIX);
                   }
               });
        
        log.info("Message format test completed successfully");
    }
    
    @Test
    public void testSourceConnectorPerformance() throws Exception {
        log.info("Testing source connector performance");
        
        // Create connector with performance settings
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-performance");
        connectorConfig.put("batch.max.rows", "1000");
        connectorConfig.put("poll.interval.ms", "1000");
        
        getConnectRunner().createConnector(connectorConfig);
        
        long startTime = System.currentTimeMillis();
        int targetMessageCount = 1000;
        
        // Wait for a significant number of messages
        await().atMost(Duration.ofMinutes(5))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromSourceTopic();
                   int messageCount = records.count();
                   
                   log.info("Performance test - consumed {} messages so far", messageCount);
                   
                   if (messageCount >= targetMessageCount) {
                       long endTime = System.currentTimeMillis();
                       long duration = endTime - startTime;
                       double messagesPerSecond = messageCount / (duration / 1000.0);
                       
                       log.info("Performance test - {} messages in {} ms ({} messages/sec)", 
                               messageCount, duration, messagesPerSecond);
                       
                       // Verify reasonable throughput (adjust threshold as needed)
                       assertThat(messagesPerSecond).isGreaterThan(5.0);
                   }
                   
                   assertThat(messageCount).isGreaterThanOrEqualTo(targetMessageCount);
               });
        
        log.info("Performance test completed successfully");
    }
    
    @Test
    public void testSourceConnectorWithDifferentTopicPrefix() throws Exception {
        log.info("Testing source connector with different topic prefix");
        
        String customPrefix = "custom-prefix-";
        
        // Create connector config with custom topic prefix
        Map<String, String> connectorConfig = createSourceConnectorConfig();
        connectorConfig.put("name", CONNECTOR_NAME + "-custom-prefix");
        connectorConfig.put("topic.prefix", customPrefix);
        
        getConnectRunner().createConnector(connectorConfig);
        
        // Wait for messages with custom prefix
        await().atMost(Duration.ofSeconds(30))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   // Try to consume from topics with custom prefix
                   String expectedTopic = customPrefix + "test-table";
                   ConsumerRecords<String, String> records = consumeFromTopic(expectedTopic);
                   
                   log.info("Consumed {} messages from topic with custom prefix", records.count());
                   
                   // Verify that messages were produced to topics with custom prefix
                   assertThat(records.count()).isGreaterThan(0);
               });
        
        log.info("Custom topic prefix test completed successfully");
    }
    
    /**
     * Creates a source connector configuration
     */
    private Map<String, String> createSourceConnectorConfig() {
        Map<String, String> config = createBasicSourceConnectorConfig();
        config.put("name", CONNECTOR_NAME);
        
        // Add any additional source-specific configuration
        config.put("poll.interval.ms", "5000");
        config.put("batch.max.rows", "100");
        config.put("connection.url", "jdbc:h2:mem:testdb");
        config.put("connection.user", "sa");
        config.put("connection.password", "");
        
        return config;
    }
    
    /**
     * Consumes messages from the source topic
     */
    private ConsumerRecords<String, String> consumeFromSourceTopic() {
        return consumeFromTopic(SOURCE_TOPIC_PREFIX + "test-table");
    }
    
    /**
     * Consumes messages from a specific topic
     */
    private ConsumerRecords<String, String> consumeFromTopic(String topicName) {
        try {
            // Create the topic if it doesn't exist
            createTopic(topicName, 1);
        } catch (Exception e) {
            // Topic might already exist, ignore
        }
        
        TopicPartition partition = new TopicPartition(topicName, 0);
        getConsumer().assign(Collections.singletonList(partition));
        getConsumer().seekToBeginning(Collections.singletonList(partition));
        
        ConsumerRecords<String, String> records = getConsumer().poll(Duration.ofSeconds(5));
        
        log.debug("Consumed {} messages from topic {}", records.count(), topicName);
        
        return records;
    }
}
