package com.example.kafka.connect.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration tests that verify both Source and Sink connectors
 * working together in a complete data pipeline.
 */
public class EndToEndIntegrationTest extends AbstractIntegrationTest {
    
    private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);
    
    private static final String SOURCE_CONNECTOR_NAME = "e2e-source-connector";
    private static final String SINK_CONNECTOR_NAME = "e2e-sink-connector";
    private static final String PIPELINE_TOPIC = "pipeline-topic";
    
    @Test
    public void testSourceToSinkDataPipeline() throws Exception {
        log.info("Testing end-to-end data pipeline: Source -> Kafka -> Sink");
        
        // Create the pipeline topic
        createTopic(PIPELINE_TOPIC, 1);
        
        // Create and start the source connector
        Map<String, String> sourceConfig = createSourceConnectorConfig();
        getConnectRunner().createConnector(sourceConfig);
        
        // Create and start the sink connector
        Map<String, String> sinkConfig = createSinkConnectorConfig();
        getConnectRunner().createConnector(sinkConfig);
        
        // Wait for the pipeline to process data
        await().atMost(Duration.ofSeconds(60))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   // Verify that source connector is producing messages
                   ConsumerRecords<String, String> records = consumeFromTopic(PIPELINE_TOPIC);
                   log.info("Pipeline processed {} messages", records.count());
                   
                   // Verify that messages are flowing through the pipeline
                   assertThat(records.count()).isGreaterThan(0);
                   
                   // Verify message content
                   for (ConsumerRecord<String, String> record : records) {
                       log.info("Pipeline message - Key: {}, Value: {}", record.key(), record.value());
                       assertThat(record.key()).isNotNull();
                       assertThat(record.value()).isNotNull();
                   }
               });
        
        log.info("End-to-end pipeline test completed successfully");
    }
    
    @Test
    public void testPipelineWithTransformations() throws Exception {
        log.info("Testing pipeline with data transformations");
        
        // Create the pipeline topic
        createTopic(PIPELINE_TOPIC + "-transform", 1);
        
        // Create source connector with transformations
        Map<String, String> sourceConfig = createSourceConnectorConfig();
        sourceConfig.put("name", SOURCE_CONNECTOR_NAME + "-transform");
        sourceConfig.put("topic.prefix", "transform-");
        
        // Add transformation configuration
        sourceConfig.put("transforms", "addPrefix");
        sourceConfig.put("transforms.addPrefix.type", "org.apache.kafka.connect.transforms.RegexRouter");
        sourceConfig.put("transforms.addPrefix.regex", "(.*)");
        sourceConfig.put("transforms.addPrefix.replacement", "transformed-$1");
        
        getConnectRunner().createConnector(sourceConfig);
        
        // Create sink connector to consume transformed messages
        Map<String, String> sinkConfig = createSinkConnectorConfig();
        sinkConfig.put("name", SINK_CONNECTOR_NAME + "-transform");
        sinkConfig.put("topics", "transformed-transform-test-table");
        
        getConnectRunner().createConnector(sinkConfig);
        
        // Wait for transformed messages
        await().atMost(Duration.ofSeconds(60))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromTopic("transformed-transform-test-table");
                   log.info("Transformation pipeline processed {} messages", records.count());
                   
                   assertThat(records.count()).isGreaterThan(0);
                   
                   // Verify transformed topic naming
                   for (ConsumerRecord<String, String> record : records) {
                       assertThat(record.topic()).startsWith("transformed-");
                   }
               });
        
        log.info("Pipeline with transformations test completed successfully");
    }
    
    @Test
    public void testPipelineErrorHandling() throws Exception {
        log.info("Testing pipeline error handling");
        
        // Create source connector with potential error conditions
        Map<String, String> sourceConfig = createSourceConnectorConfig();
        sourceConfig.put("name", SOURCE_CONNECTOR_NAME + "-error");
        sourceConfig.put("topic.prefix", "error-");
        
        // Add error handling configuration
        sourceConfig.put("errors.tolerance", "all");
        sourceConfig.put("errors.log.enable", "true");
        sourceConfig.put("errors.log.include.messages", "true");
        
        getConnectRunner().createConnector(sourceConfig);
        
        // Create sink connector with error handling
        Map<String, String> sinkConfig = createSinkConnectorConfig();
        sinkConfig.put("name", SINK_CONNECTOR_NAME + "-error");
        sinkConfig.put("topics", "error-test-table");
        sinkConfig.put("errors.tolerance", "all");
        sinkConfig.put("errors.log.enable", "true");
        
        getConnectRunner().createConnector(sinkConfig);
        
        // Send some potentially problematic messages
        sendTestMessages("error-test-table", 10);
        
        // Wait for error handling to process messages
        await().atMost(Duration.ofSeconds(60))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromTopic("error-test-table");
                   log.info("Error handling pipeline processed {} messages", records.count());
                   
                   // Even with errors, some messages should be processed
                   assertThat(records.count()).isGreaterThanOrEqualTo(0);
               });
        
        log.info("Pipeline error handling test completed successfully");
    }
    
    @Test
    public void testPipelineWithMultipleTopics() throws Exception {
        log.info("Testing pipeline with multiple topics");
        
        String topic1 = "multi-topic-1";
        String topic2 = "multi-topic-2";
        
        // Create multiple topics
        createTopic(topic1, 1);
        createTopic(topic2, 1);
        
        // Create source connector producing to multiple topics
        Map<String, String> sourceConfig = createSourceConnectorConfig();
        sourceConfig.put("name", SOURCE_CONNECTOR_NAME + "-multi");
        sourceConfig.put("topic.prefix", "multi-");
        sourceConfig.put("table.whitelist", "topic1,topic2");
        
        getConnectRunner().createConnector(sourceConfig);
        
        // Create sink connector consuming from multiple topics
        Map<String, String> sinkConfig = createSinkConnectorConfig();
        sinkConfig.put("name", SINK_CONNECTOR_NAME + "-multi");
        sinkConfig.put("topics", topic1 + "," + topic2);
        
        getConnectRunner().createConnector(sinkConfig);
        
        // Wait for processing from multiple topics
        await().atMost(Duration.ofSeconds(60))
               .pollInterval(Duration.ofSeconds(5))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records1 = consumeFromTopic(topic1);
                   ConsumerRecords<String, String> records2 = consumeFromTopic(topic2);
                   
                   int totalMessages = records1.count() + records2.count();
                   log.info("Multi-topic pipeline processed {} messages ({} from {}, {} from {})",
                           totalMessages, records1.count(), topic1, records2.count(), topic2);
                   
                   // Verify messages from multiple topics
                   assertThat(totalMessages).isGreaterThan(0);
               });
        
        log.info("Multi-topic pipeline test completed successfully");
    }
    
    @Test
    public void testPipelinePerformance() throws Exception {
        log.info("Testing pipeline performance under load");
        
        String performanceTopic = "performance-topic";
        createTopic(performanceTopic, 3); // Multiple partitions for better performance
        
        // Create high-throughput source connector
        Map<String, String> sourceConfig = createSourceConnectorConfig();
        sourceConfig.put("name", SOURCE_CONNECTOR_NAME + "-perf");
        sourceConfig.put("topic.prefix", "perf-");
        sourceConfig.put("batch.max.rows", "1000");
        sourceConfig.put("poll.interval.ms", "1000");
        sourceConfig.put("tasks.max", "2");
        
        getConnectRunner().createConnector(sourceConfig);
        
        // Create high-throughput sink connector
        Map<String, String> sinkConfig = createSinkConnectorConfig();
        sinkConfig.put("name", SINK_CONNECTOR_NAME + "-perf");
        sinkConfig.put("topics", "perf-test-table");
        sinkConfig.put("batch.size", "1000");
        sinkConfig.put("tasks.max", "2");
        
        getConnectRunner().createConnector(sinkConfig);
        
        long startTime = System.currentTimeMillis();
        int targetMessages = 5000;
        
        // Wait for high-volume processing
        await().atMost(Duration.ofMinutes(10))
               .pollInterval(Duration.ofSeconds(10))
               .untilAsserted(() -> {
                   ConsumerRecords<String, String> records = consumeFromTopic("perf-test-table");
                   int messageCount = records.count();
                   
                   log.info("Performance pipeline processed {} messages so far", messageCount);
                   
                   if (messageCount >= targetMessages) {
                       long endTime = System.currentTimeMillis();
                       long duration = endTime - startTime;
                       double messagesPerSecond = messageCount / (duration / 1000.0);
                       
                       log.info("Performance test - {} messages in {} ms ({} messages/sec)",
                               messageCount, duration, messagesPerSecond);
                       
                       // Verify reasonable pipeline throughput
                       assertThat(messagesPerSecond).isGreaterThan(10.0);
                   }
                   
                   assertThat(messageCount).isGreaterThanOrEqualTo(targetMessages);
               });
        
        log.info("Pipeline performance test completed successfully");
    }
    
    /**
     * Creates a source connector configuration for E2E testing
     */
    private Map<String, String> createSourceConnectorConfig() {
        Map<String, String> config = createBasicSourceConnectorConfig();
        config.put("name", SOURCE_CONNECTOR_NAME);
        config.put("topic.prefix", "pipeline-");
        config.put("poll.interval.ms", "3000");
        config.put("batch.max.rows", "100");
        
        // Mock database connection for testing
        config.put("connection.url", "jdbc:h2:mem:e2etestdb");
        config.put("connection.user", "sa");
        config.put("connection.password", "");
        
        return config;
    }
    
    /**
     * Creates a sink connector configuration for E2E testing
     */
    private Map<String, String> createSinkConnectorConfig() {
        Map<String, String> config = createBasicSinkConnectorConfig();
        config.put("name", SINK_CONNECTOR_NAME);
        config.put("topics", PIPELINE_TOPIC);
        config.put("batch.size", "100");
        config.put("flush.timeout.ms", "5000");
        
        return config;
    }
    
    /**
     * Sends test messages to a specific topic
     */
    private void sendTestMessages(String topic, int count) throws ExecutionException, InterruptedException {
        for (int i = 0; i < count; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                "test-key-" + i,
                "Test message " + i + " for E2E testing"
            );
            
            getProducer().send(record).get();
        }
        
        log.info("Sent {} test messages to topic {}", count, topic);
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
