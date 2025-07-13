/*
 * Copyright 2022 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.kafka.connect.integration;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectRunner manages an embedded Kafka Connect runtime for integration
 * testing. This implementation provides a real embedded Connect worker for
 * comprehensive integration testing of Kafka Connect connectors.
 */
public final class ConnectRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectRunner.class);

    public static String OFFSET_TOPIC = "connect-offsets";
    public static String CONFIG_TOPIC = "connect-configs";
    public static String STATUS_TOPIC = "connect-status";
    private final String bootstrapServers;
    private final Path pluginDir;

    private Herder herder;
    private Worker worker;
    private ConnectRestServer restServer;
    private Connect<StandaloneHerder> connect;
    private boolean started = false;
    private final String KAFKA_CONNECT_GROUP_ID = "connect-cluster";

    public ConnectRunner(final String bootstrapServers, final Path pluginDir) {
        this.bootstrapServers = bootstrapServers;
        this.pluginDir = pluginDir;
    }

    /**
     * Creates a connector with the given configuration
     */
    public void createConnector(final Map<String, String> config) throws ExecutionException, InterruptedException {
        if (!started) {
            throw new IllegalStateException("Connect runtime not started");
        }

        final String connectorName = config.get(ConnectorConfig.NAME_CONFIG);
        if (connectorName == null) {
            throw new IllegalArgumentException("Connector name is required");
        }

        LOGGER.info("Creating connector: {}", connectorName);

        final FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
            if (error != null) {
                LOGGER.error("Failed to create connector: {}", connectorName, error);
            } else {
                LOGGER.info("Created connector {}", info.result().name());
            }
        });

        herder.putConnectorConfig(connectorName, config, false, cb);

        final Herder.Created<ConnectorInfo> connectorInfoCreated = cb.get();
        if (!connectorInfoCreated.created()) {
            throw new RuntimeException("Failed to create connector: " + connectorName);
        }
    }

    /**
     * Restarts a connector task
     */
    public void restartTask(final String connector, final int task) throws ExecutionException, InterruptedException {
        if (!started) {
            throw new IllegalStateException("Connect runtime not started");
        }

        LOGGER.info("Restarting task {}-{}", connector, task);

        final FutureCallback<Void> cb = new FutureCallback<>();

        herder.restartTask(new ConnectorTaskId(connector, task), cb);

        try {
            cb.get();
            LOGGER.info("Restarted task {}-{}", connector, task);
        } catch (Exception error) {
            LOGGER.error("Failed to restart task {}-{}", connector, task, error);
            throw new RuntimeException("Failed to restart task: " + connector + "-" + task, error);
        }
    }

    // get connector task to call the poll method
    public ConnectorTaskId getConnectorTaskId(final String connectorName, final int taskId) {
        if (!started) {
            throw new IllegalStateException("Connect runtime not started");
        }
        return new ConnectorTaskId(connectorName, taskId);
    }

    /**
     * Starts the embedded Kafka Connect runtime
     */
    void start() {
        LOGGER.info("Starting embedded Kafka Connect runtime with bootstrap servers: {}", bootstrapServers);

        final Map<String, String> workerProps = createWorkerConfig();
        LOGGER.debug("Worker configuration: {}", workerProps);

        final Time time = Time.SYSTEM;
        final String workerId = "test-worker";
        final String kafkaClusterId = "test-cluster";

        LOGGER.debug("Initializing plugins from path: {}", pluginDir);
        final Plugins plugins = new Plugins(workerProps);
        final DistributedConfig config = new DistributedConfig(workerProps);

        final ConnectorClientConfigOverridePolicy overridePolicy = new AllConnectorClientConfigOverridePolicy();

        // Initialize offset backing store
        LOGGER.debug("Creating Kafka-based offset backing store for topic: {}", OFFSET_TOPIC);
        final OffsetBackingStore offsetBackingStore = createOffsetBackingStore(config, plugins);

        LOGGER.debug("Creating worker with ID: {}", workerId);
        worker = new Worker(workerId, time, plugins, config, offsetBackingStore, overridePolicy);

        LOGGER.debug("Creating herder for cluster: {}", kafkaClusterId);
        herder = new StandaloneHerder(worker, kafkaClusterId, overridePolicy);

        final RestClient restClient = new RestClient(config);
        restServer = new ConnectRestServer(10, restClient, workerProps);
        restServer.initializeServer();
        restServer.initializeResources(herder);

        LOGGER.debug("Starting Connect runtime...");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Connect<StandaloneHerder> connect = new Connect(herder, restServer);
        connect.start();

        started = true;
        LOGGER.info("Embedded Kafka Connect runtime started successfully");
    }

    /**
     * Stops the Connect runtime
     */
    void stop() {
        LOGGER.info("Stopping embedded Kafka Connect runtime");

        if (worker != null) {
            try {
                worker.stop();
            } catch (final Exception e) {
                LOGGER.warn("Error stopping worker", e);
            }
        }

        if (restServer != null) {
            restServer.stop();
        }

        started = false;
        LOGGER.info("Embedded Kafka Connect runtime stopped");
    }

    /**
     * Waits for the Connect runtime to stop
     */
    void awaitStop() {
        if (connect != null) {
            connect.awaitStop();
        }

        if (restServer != null) {
            restServer.stop();
        }

        LOGGER.info("Embedded Kafka Connect runtime stopped completely");
    }

    private Map<String, String> createWorkerConfig() {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put(DistributedConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        workerProps.put(DistributedConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "5000");

        // Default converters - connectors can override these
        workerProps.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");

        // Enable logging for converters
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put("value.converter.schemas.enable", "false");

        // Plugin path
        workerProps.put(DistributedConfig.PLUGIN_PATH_CONFIG, pluginDir.toString());

        workerProps.put(DistributedConfig.GROUP_ID_CONFIG, KAFKA_CONNECT_GROUP_ID);
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, OFFSET_TOPIC);
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, CONFIG_TOPIC);
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, STATUS_TOPIC);

        // Enhanced logging configuration for debugging
        LOGGER.debug("Worker configuration: {}", workerProps);

        return workerProps;
    }

    private OffsetBackingStore createOffsetBackingStore(final DistributedConfig config, final Plugins plugins) {
        // Use Kafka-based offset store for proper distributed operation
        // This stores offsets in the connect-offsets topic

        // Create a proper TopicAdmin for managing offset topic
        final Supplier<TopicAdmin> topicAdminSupplier = () -> {
            final Map<String, Object> adminConfig = new HashMap<>();
            adminConfig.put("bootstrap.servers", bootstrapServers);
            adminConfig.put("client.id", "connect-worker-offset-admin");
            return new TopicAdmin(adminConfig);
        };

        // Create a supplier for the offset topic name
        final Supplier<String> offsetTopicSupplier = () -> OFFSET_TOPIC;

        // Create key converter for offset storage
        final Converter keyConverter = plugins.newConverter(config, "key.converter",
                ClassLoaderUsage.CURRENT_CLASSLOADER);

        // Configure the key converter
        final Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        keyConverter.configure(converterConfig, true);

        // Create KafkaOffsetBackingStore with required parameters
        final KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(topicAdminSupplier,
                offsetTopicSupplier, keyConverter);
        offsetBackingStore.configure(config);
        return offsetBackingStore;
    }

    public Herder getHerder() {
        if (!started) {
            throw new IllegalStateException("Connect runtime not started");
        }
        if (herder == null) {
            throw new IllegalStateException("Herder is not initialized");
        }
        return herder;
    }
}
