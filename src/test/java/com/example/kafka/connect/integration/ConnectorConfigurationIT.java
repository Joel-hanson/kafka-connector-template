package com.example.kafka.connect.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.connect.sink.ExampleSinkConnector;
import com.example.kafka.connect.source.ExampleSourceConnector;

/**
 * Integration tests for connector configuration validation. Tests configuration
 * parsing, validation, and error handling.
 */
public class ConnectorConfigurationIT extends AbstractIT {

    private static final Logger log = LoggerFactory.getLogger(ConnectorConfigurationIT.class);

    @Test
    public void testSinkConnectorValidConfiguration() {
        log.info("Testing sink connector with valid configuration");

        ExampleSinkConnector connector = new ExampleSinkConnector();
        Map<String, String> config = createValidSinkConfig();

        // Validate configuration
        Config validatedConfig = connector.validate(config);

        // Check that there are no errors
        List<ConfigValue> configValues = validatedConfig.configValues();
        for (ConfigValue configValue : configValues) {
            log.info("Config: {} = {}, Errors: {}", configValue.name(), configValue.value(),
                    configValue.errorMessages());

            // Assert no configuration errors
            assertThat(configValue.errorMessages()).isEmpty();
        }

        log.info("Sink connector configuration validation passed");
    }

    @Test
    public void testSourceConnectorValidConfiguration() {
        log.info("Testing source connector with valid configuration");

        ExampleSourceConnector connector = new ExampleSourceConnector();
        Map<String, String> config = createValidSourceConfig();

        // Validate configuration
        Config validatedConfig = connector.validate(config);

        // Check that there are no errors
        List<ConfigValue> configValues = validatedConfig.configValues();
        for (ConfigValue configValue : configValues) {
            log.info("Config: {} = {}, Errors: {}", configValue.name(), configValue.value(),
                    configValue.errorMessages());

            // Assert no configuration errors
            assertThat(configValue.errorMessages()).isEmpty();
        }

        log.info("Source connector configuration validation passed");
    }

    @Test
    public void testSinkConnectorInvalidConfiguration() {
        log.info("Testing sink connector with invalid configuration");

        ExampleSinkConnector connector = new ExampleSinkConnector();
        Map<String, String> config = createInvalidSinkConfig();

        // Validate configuration
        Config validatedConfig = connector.validate(config);

        // Check that there are validation errors
        List<ConfigValue> configValues = validatedConfig.configValues();
        boolean hasErrors = false;

        for (ConfigValue configValue : configValues) {
            if (!configValue.errorMessages().isEmpty()) {
                hasErrors = true;
                log.info("Expected error for config {}: {}", configValue.name(), configValue.errorMessages());
            }
        }

        // We expect some configuration errors
        assertThat(hasErrors).isTrue();

        log.info("Sink connector invalid configuration test passed");
    }

    @Test
    public void testSourceConnectorInvalidConfiguration() {
        log.info("Testing source connector with invalid configuration");

        ExampleSourceConnector connector = new ExampleSourceConnector();
        Map<String, String> config = createInvalidSourceConfig();

        // Validate configuration
        Config validatedConfig = connector.validate(config);

        // Check that there are validation errors
        List<ConfigValue> configValues = validatedConfig.configValues();
        boolean hasErrors = false;

        for (ConfigValue configValue : configValues) {
            if (!configValue.errorMessages().isEmpty()) {
                hasErrors = true;
                log.info("Expected error for config {}: {}", configValue.name(), configValue.errorMessages());
            }
        }

        // We expect some configuration errors
        assertThat(hasErrors).isTrue();

        log.info("Source connector invalid configuration test passed");
    }

    @Test
    public void testConnectorConfigDefaults() {
        log.info("Testing connector configuration defaults");

        // Test sink connector defaults
        ExampleSinkConnector sinkConnector = new ExampleSinkConnector();
        Map<String, String> minimalSinkConfig = new HashMap<>();
        minimalSinkConfig.put("name", "test-sink");
        minimalSinkConfig.put("connector.class", ExampleSinkConnector.class.getName());
        minimalSinkConfig.put("topics", "test-topic");

        Config sinkConfig = sinkConnector.validate(minimalSinkConfig);
        validateDefaultValues(sinkConfig, "sink");

        // Test source connector defaults
        ExampleSourceConnector sourceConnector = new ExampleSourceConnector();
        Map<String, String> minimalSourceConfig = new HashMap<>();
        minimalSourceConfig.put("name", "test-source");
        minimalSourceConfig.put("connector.class", ExampleSourceConnector.class.getName());
        minimalSourceConfig.put("topic.prefix", "test-");

        Config sourceConfig = sourceConnector.validate(minimalSourceConfig);
        validateDefaultValues(sourceConfig, "source");

        log.info("Connector configuration defaults test passed");
    }

    @Test
    public void testConnectorTaskConfigs() throws Exception {
        log.info("Testing connector task configuration generation");

        // Test sink connector task configs
        ExampleSinkConnector sinkConnector = new ExampleSinkConnector();
        Map<String, String> sinkConfig = createValidSinkConfig();
        sinkConnector.start(sinkConfig);

        List<Map<String, String>> sinkTaskConfigs = sinkConnector.taskConfigs(1);
        assertThat(sinkTaskConfigs).hasSize(1);
        assertThat(sinkTaskConfigs.get(0)).containsKey("topics");

        sinkConnector.stop();

        // Test source connector task configs
        ExampleSourceConnector sourceConnector = new ExampleSourceConnector();
        Map<String, String> sourceConfig = createValidSourceConfig();
        sourceConnector.start(sourceConfig);

        List<Map<String, String>> sourceTaskConfigs = sourceConnector.taskConfigs(1);
        assertThat(sourceTaskConfigs).hasSize(1);
        assertThat(sourceTaskConfigs.get(0)).containsKey("topic");

        sourceConnector.stop();

        log.info("Connector task configuration test passed");
    }

    @Test
    public void testConnectorVersions() {
        log.info("Testing connector versions");

        ExampleSinkConnector sinkConnector = new ExampleSinkConnector();
        ExampleSourceConnector sourceConnector = new ExampleSourceConnector();

        String sinkVersion = sinkConnector.version();
        String sourceVersion = sourceConnector.version();

        log.info("Sink connector version: {}", sinkVersion);
        log.info("Source connector version: {}", sourceVersion);

        // Verify versions are not null or empty
        assertThat(sinkVersion).isNotNull().isNotEmpty();
        assertThat(sourceVersion).isNotNull().isNotEmpty();

        // Verify versions follow semantic versioning pattern
        assertThat(sinkVersion).matches("\\d+\\.\\d+\\.\\d+.*");
        assertThat(sourceVersion).matches("\\d+\\.\\d+\\.\\d+.*");

        log.info("Connector versions test passed");
    }

    @Test
    public void testConnectorTaskClasses() {
        log.info("Testing connector task classes");

        ExampleSinkConnector sinkConnector = new ExampleSinkConnector();
        ExampleSourceConnector sourceConnector = new ExampleSourceConnector();

        Class<?> sinkTaskClass = sinkConnector.taskClass();
        Class<?> sourceTaskClass = sourceConnector.taskClass();

        log.info("Sink task class: {}", sinkTaskClass.getName());
        log.info("Source task class: {}", sourceTaskClass.getName());

        // Verify task classes are not null
        assertThat(sinkTaskClass).isNotNull();
        assertThat(sourceTaskClass).isNotNull();

        // Verify task classes are correct types
        assertThat(sinkTaskClass.getName()).contains("SinkTask");
        assertThat(sourceTaskClass.getName()).contains("SourceTask");

        log.info("Connector task classes test passed");
    }

    /**
     * Creates a valid sink connector configuration
     */
    private Map<String, String> createValidSinkConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "test-sink-connector");
        config.put("connector.class", ExampleSinkConnector.class.getName());
        config.put("topics", "test-topic");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");
        config.put("tasks.max", "1");
        return config;
    }

    /**
     * Creates a valid source connector configuration
     */
    private Map<String, String> createValidSourceConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "test-source-connector");
        config.put("connector.class", ExampleSourceConnector.class.getName());
        config.put("topic", "test-topic");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");
        config.put("tasks.max", "1");
        return config;
    }

    /**
     * Creates an invalid sink connector configuration (missing required properties)
     */
    private Map<String, String> createInvalidSinkConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "invalid-sink-connector");
        config.put("connector.class", ExampleSinkConnector.class.getName());
        // Missing 'topics' property
        config.put("tasks.max", "invalid-number"); // Invalid value
        return config;
    }

    /**
     * Creates an invalid source connector configuration (missing required
     * properties)
     */
    private Map<String, String> createInvalidSourceConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("name", "invalid-source-connector");
        config.put("connector.class", ExampleSourceConnector.class.getName());
        // Missing 'topic.prefix' property
        config.put("tasks.max", "invalid-number"); // Invalid value
        return config;
    }

    /**
     * Validates default configuration values
     */
    private void validateDefaultValues(Config config, String connectorType) {
        List<ConfigValue> configValues = config.configValues();

        for (ConfigValue configValue : configValues) {
            log.info("{} connector default - {}: {}", connectorType, configValue.name(), configValue.value());
        }

        // Verify that required configurations are present
        assertThat(configValues).isNotEmpty();
    }
}
