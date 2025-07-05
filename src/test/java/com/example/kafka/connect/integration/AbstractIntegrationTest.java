package com.example.kafka.connect.integration;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.output.Slf4jLogConsumer;

/**
 * Abstract base class for integration tests providing common Kafka infrastructure
 * using TestContainers. This class sets up a Kafka cluster with Connect functionality
 * and provides utility methods for creating producers, consumers, and topics.
 */
@Testcontainers
public abstract class AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    protected static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(5);
    protected static final String TEST_TOPIC_NAME = "test-topic";
    protected static final String SINK_TOPIC_NAME = "sink-topic";
    protected static final String SOURCE_TOPIC_NAME = "source-topic";

    private static final String DEFAULT_KAFKA_TAG = "3.7.1";
    private static final DockerImageName KAFKA_IMAGE_NAME =
        DockerImageName.parse("apache/kafka").withTag(DEFAULT_KAFKA_TAG);

    @Container
    protected static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE_NAME)
        .withNetwork(Network.newNetwork())
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
        .withExposedPorts(9092, 8083);


    protected static KafkaProducer<String, String> producer;
    protected static KafkaConsumer<String, String> consumer;
    protected static AdminClient adminClient;
    protected static ConnectRunner connectRunner;

    @BeforeEach
    void setUp() throws Exception {
        log.info("Setting up integration test infrastructure");
        kafkaContainer.followOutput(new Slf4jLogConsumer(log));
        // Initialize Kafka clients
        setupKafkaClients();

        // Set up Kafka Connect
        setupKafkaConnect();

        // Create test topics
        setupTopics();

        log.info("Integration test setup completed");
    }

    @AfterEach
    void tearDown() {
        log.info("Tearing down integration test infrastructure");

        if (connectRunner != null) {
            connectRunner.stop();
        }

        if (producer != null) {
            producer.close();
        }

        if (consumer != null) {
            consumer.close();
        }

        if (adminClient != null) {
            adminClient.close();
        }

        if (connectRunner != null) {
            connectRunner.awaitStop();
        }

        log.info("Integration test teardown completed");
    }

    /**
     * Sets up Kafka clients (producer, consumer, admin client)
     */
    private void setupKafkaClients() {
        log.info("Setting up Kafka clients");

        // Create producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producer = new KafkaProducer<>(producerProps);

        // Create consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);

        // Create admin client
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);
    }

    /**
     * Sets up Kafka Connect runtime
     */
    private void setupKafkaConnect() {
        log.info("Setting up Kafka Connect");
        connectRunner = new ConnectRunner(kafkaContainer.getBootstrapServers());
        connectRunner.start();
    }

    /**
     * Creates test topics
     */
    private void setupTopics() throws ExecutionException, InterruptedException {
        log.info("Checking and creating missing topics");

        List<String> topicNames = List.of(TEST_TOPIC_NAME, SINK_TOPIC_NAME, SOURCE_TOPIC_NAME);
        Set<String> existingTopics = adminClient.listTopics().names().get();

        List<NewTopic> topicsToCreate = topicNames.stream()
            .filter(name -> !existingTopics.contains(name))
            .map(name -> new NewTopic(name, 1, (short) 1))
            .collect(Collectors.toList());

        if (!topicsToCreate.isEmpty()) {
            adminClient.createTopics(topicsToCreate).all().get();
            log.info("Created topics: {}", topicsToCreate.stream().map(NewTopic::name).collect(Collectors.toList()));
        } else {
            log.info("All topics already exist, no need to create");
        }
    }


    /**
     * Creates additional topics for tests
     */
    protected void createTopic(String topicName, int partitions) throws ExecutionException, InterruptedException {
        log.info("Creating topic: {} with {} partitions", topicName, partitions);
        NewTopic topic = new NewTopic(topicName, partitions, (short) 1);
        adminClient.createTopics(List.of(topic)).all().get();
    }

    /**
     * Creates a basic connector configuration with common settings
     */
    protected Map<String, String> createBasicConnectorConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("key.converter", StringConverter.class.getName());
        config.put("value.converter", JsonConverter.class.getName());
        config.put("value.converter.schemas.enable", "false");
        config.put("tasks.max", "1");
        return config;
    }

    /**
     * Creates a basic sink connector configuration
     */
    protected Map<String, String> createBasicSinkConnectorConfig() {
        Map<String, String> config = createBasicConnectorConfig();
        config.put("connector.class", "com.example.kafka.connect.sink.ExampleSinkConnector");
        config.put("topics", SINK_TOPIC_NAME);
        config.put("name", "test-sink-connector");
        return config;
    }

    /**
     * Creates a basic source connector configuration
     */
    protected Map<String, String> createBasicSourceConnectorConfig() {
        Map<String, String> config = createBasicConnectorConfig();
        config.put("connector.class", "com.example.kafka.connect.source.ExampleSourceConnector");
        config.put("topic.prefix", "source-");
        config.put("name", "test-source-connector");
        return config;
    }

    /**
     * Gets the Kafka bootstrap servers for external connections
     */
    protected String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    /**
     * Gets the connect runner for connector management
     */
    protected ConnectRunner getConnectRunner() {
        return connectRunner;
    }

    /**
     * Gets the Kafka producer
     */
    protected KafkaProducer<String, String> getProducer() {
        return producer;
    }

    /**
     * Gets the Kafka consumer
     */
    protected KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    /**
     * Gets the admin client
     */
    protected AdminClient getAdminClient() {
        return adminClient;
    }
}
