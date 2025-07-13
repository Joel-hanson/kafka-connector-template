package com.example.kafka.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example implementation of a Kafka Connect Sink Connector. This connector is
 * responsible for: 1. Defining configuration properties 2. Instantiating and
 * configuring tasks to send data to the target system 3. Distributing work
 * across multiple tasks
 */
public class ExampleSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(ExampleSinkConnector.class);
    private Map<String, String> configProps;
    private ExampleSinkConnectorConfig config;

    @Override
    public final String version() {
        return "0.0.2";
    }

    @Override
    public final void start(final Map<String, String> props) {
        log.info("Starting Example Sink Connector");
        configProps = props;
        config = new ExampleSinkConnectorConfig(props);
        // Initialize any resources or connections needed by the connector
        log.info("Example Sink Connector started");
    }

    @Override
    public final Class<? extends Task> taskClass() {
        return ExampleSinkTask.class;
    }

    @Override
    public final List<Map<String, String>> taskConfigs(final int maxTasks) {
        // Define how to split the work across multiple tasks
        final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        // Simple example: create identical configuration for each task
        // For more complex scenarios, you might distribute topics/partitions across
        // tasks
        for (int i = 0; i < maxTasks; i++) {
            final Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }

    @Override
    public final void stop() {
        log.info("Stopping Example Sink Connector");
        // Clean up any resources that were created in start()
        log.info("Example Sink Connector stopped");
    }

    @Override
    public final ConfigDef config() {
        return ExampleSinkConnectorConfig.config();
    }
}
