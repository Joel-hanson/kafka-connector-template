package com.example.kafka.connect.integration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Abstract base class for integration tests providing common Kafka
 * infrastructure using TestContainers. This class sets up a Kafka cluster with
 * Connect functionality and provides utility methods for creating producers,
 * consumers, and topics.
 */
@Testcontainers
public abstract class AbstractIT {

    private static final Logger log = LoggerFactory.getLogger(AbstractIT.class);

    protected static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofMinutes(5);
    protected static final String TEST_TOPIC_NAME = "test-topic";
    protected static final String SINK_TOPIC_NAME = "sink-topic";
    protected static final String SOURCE_TOPIC_NAME = "source-topic";

    private static final String DEFAULT_KAFKA_TAG = "3.9.1";
    private static final DockerImageName KAFKA_IMAGE_NAME = DockerImageName.parse("apache/kafka")
            .withTag(DEFAULT_KAFKA_TAG);

    @SuppressWarnings("resource")
    @Container
    protected static final KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_IMAGE_NAME)
            .withNetwork(Network.newNetwork()).withStartupTimeout(CONTAINER_STARTUP_TIMEOUT)
            .withExposedPorts(9092, 8083);

    protected static KafkaProducer<String, String> producer;
    protected static KafkaConsumer<String, String> consumer;
    protected static AdminClient adminClient;
    protected static ConnectRunner connectRunner;

    private static Path setupPluginDir() throws Exception {
        final Path testDir = Files.createTempDirectory("kafka-connector-template-");
        final String destFilePath = "./target/kafka-connector-template-1.0.0-SNAPSHOT-jar-with-dependencies.jar";
        final Path distFile = Paths.get(destFilePath);
        assert Files.exists(distFile);

        final var pluginDir = Paths.get(testDir.toString(), "plugins/kafka-connector-template/");
        Files.createDirectories(pluginDir);

        Files.copy(distFile, pluginDir.resolve(distFile.getFileName()));
        log.info("Plugin directory created at: {}", pluginDir.toAbsolutePath());
        return pluginDir;
    }

    /**
     * Creates additional topics for tests
     */
    protected void createTopic(final String topicName, final int partitions)
            throws ExecutionException, InterruptedException {
        log.info("Creating topic: {} with {} partitions", topicName, partitions);
        final NewTopic topic = new NewTopic(topicName, partitions, (short) 1);
        adminClient.createTopics(List.of(topic)).all().get();
    }

    /**
     * Creates a basic connector configuration with common settings
     */
    protected Map<String, String> createBasicConnectorConfig() {
        final Map<String, String> config = new HashMap<>();
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
        final Map<String, String> config = createBasicConnectorConfig();
        config.put("connector.class", "com.example.kafka.connect.sink.ExampleSinkConnector");
        config.put("topics", SINK_TOPIC_NAME);
        config.put("name", "test-sink-connector");
        return config;
    }

    /**
     * Creates a basic source connector configuration
     */
    protected Map<String, String> createBasicSourceConnectorConfig() {
        final Map<String, String> config = createBasicConnectorConfig();
        config.put("connector.class", "com.example.kafka.connect.source.ExampleSourceConnector");
        config.put("topic", SOURCE_TOPIC_NAME);
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

    @BeforeEach
    void setUp() throws Exception {
        log.info("Setting up integration test infrastructure");
        // kafkaContainer.followOutput(new Slf4jLogConsumer(log));

        // Initialize Kafka clients
        setupKafkaClients();

        // Create topics
        setupTopics();

        // Set up Kafka Connect
        setupKafkaConnect();

        log.info("Integration test setup completed");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        log.info("Tearing down integration test infrastructure");

        if (connectRunner != null) {
            connectRunner.stop();
            Thread.sleep(1000); // Allow time for Connect to stop
        }

        if (producer != null) {
            producer.close(Duration.ofSeconds(10));
        }

        if (consumer != null) {
            consumer.close(Duration.ofSeconds(10));
        }

        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(10));
        }

        if (connectRunner != null) {
            connectRunner.stop();
        }

        log.info("Integration test teardown completed");
    }

    /**
     * Sets up Kafka clients (producer, consumer, admin client)
     */
    private void setupKafkaClients() {
        log.info("Setting up Kafka clients");

        // Create producer
        final Properties producerProps = new Properties();
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
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);

        // Create admin client
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        adminClient = AdminClient.create(adminProps);
    }

    /**
     * Sets up Kafka Connect runtime
     */
    private void setupKafkaConnect() throws Exception {
        log.info("Setting up Kafka Connect");
        final Path pluginDir = setupPluginDir();
        if (!Files.exists(pluginDir)) {
            throw new IllegalStateException("Plugin directory does not exist: " + pluginDir);
        }
        log.info("Using plugin directory: {}", pluginDir.toAbsolutePath());
        log.info("Bootstrap servers for Connect: {}", kafkaContainer.getBootstrapServers());
        connectRunner = new ConnectRunner(kafkaContainer.getBootstrapServers(), pluginDir);
        log.info("Starting Connect runtime...");
        connectRunner.start();
        log.info("Kafka Connect setup completed successfully");
    }

    /**
     * Creates test topics
     */
    private void setupTopics() throws ExecutionException, InterruptedException {
        log.info("Checking and creating missing topics");

        // Define the topics to create
        final List<String> topicNames = List.of(TEST_TOPIC_NAME, SINK_TOPIC_NAME, SOURCE_TOPIC_NAME,
                ConnectRunner.OFFSET_TOPIC, ConnectRunner.CONFIG_TOPIC, ConnectRunner.STATUS_TOPIC);

        final Set<String> existingTopics = adminClient.listTopics().names().get();

        final List<NewTopic> topicsToCreate = topicNames.stream().filter(name -> !existingTopics.contains(name))
                .map(name -> {
                    NewTopic topic = new NewTopic(name, 1, (short) 1);
                    // Set cleanup policy to compact for Kafka Connect internal topics
                    if (name.equals(ConnectRunner.OFFSET_TOPIC) || name.equals(ConnectRunner.CONFIG_TOPIC)
                            || name.equals(ConnectRunner.STATUS_TOPIC)) {
                        Map<String, String> configs = new HashMap<>();
                        configs.put("cleanup.policy", "compact");
                        topic.configs(configs);
                    }
                    return topic;
                }).collect(Collectors.toList());

        if (!topicsToCreate.isEmpty()) {
            adminClient.createTopics(topicsToCreate).all().get();
            log.info("Created topics: {}", topicsToCreate.stream().map(NewTopic::name).collect(Collectors.toList()));
        } else {
            log.info("All topics already exist, no need to create");
        }
    }
}
