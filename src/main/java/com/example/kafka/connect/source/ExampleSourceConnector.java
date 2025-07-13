package com.example.kafka.connect.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example implementation of a Kafka Connect Source Connector. This connector is
 * responsible for: 1. Defining configuration properties 2. Instantiating and
 * configuring tasks to pull data from external systems 3. Partitioning the work
 * across multiple tasks
 */
public class ExampleSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(ExampleSourceConnector.class);
    private Map<String, String> configProps;
    private ExampleSourceConnectorConfig config;

    @Override
    public String version() {
        return "0.0.2";
    }

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting Example Source Connector");
        configProps = props;
        config = new ExampleSourceConnectorConfig(props);
        // Initialize any resources or connections needed by the connector
        log.info("Example Source Connector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ExampleSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        // Define how to split the work across multiple tasks
        // For example, you might assign different partitions or query ranges to each
        // task
        final List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

        // Simple example: create identical configuration for each task
        // In a real connector, you might partition your data source
        for (int i = 0; i < maxTasks; i++) {
            final Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            taskConfigs.add(taskConfig);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping Example Source Connector");
        // Clean up any resources that were created in start()
        log.info("Example Source Connector stopped");
    }

    @Override
    public ConfigDef config() {
        return ExampleSourceConnectorConfig.config();
    }
}
