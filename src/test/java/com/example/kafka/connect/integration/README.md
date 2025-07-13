# Integration Tests for Kafka Connector

This directory contains comprehensive integration tests for the Example Kafka Connector project, inspired by the testing framework used in the [Aiven JDBC Connector for Apache Kafka](https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka).

## Overview

The integration tests verify the end-to-end functionality of both Source and Sink connectors using real Kafka infrastructure provided by TestContainers.

## Test Structure

### Base Infrastructure

- **AbstractIntegrationTest**: Base class providing Kafka infrastructure using TestContainers
- **ConnectRunner**: Manages Kafka Connect runtime for testing

### Test Classes

1. **ExampleSinkConnectorIntegrationTest**: Tests for Sink connector functionality
   - Basic message consumption and processing
   - Multi-partition topic handling
   - Configuration validation
   - Task restart scenarios
   - JSON message processing
   - Performance testing

2. **ExampleSourceConnectorIntegrationTest**: Tests for Source connector functionality
   - Message production to Kafka topics
   - Custom configuration handling
   - Multiple table/source processing
   - Task restart scenarios
   - Message format validation
   - Performance testing

4. **ConnectorConfigurationIntegrationTest**: Configuration validation tests
   - Valid/invalid configuration scenarios
   - Default value verification
   - Task configuration generation
   - Version and metadata validation

## Key Features

### TestContainers Integration

The tests use TestContainers to provide:

- Real Kafka cluster
- Isolated test environments
- Automatic cleanup
- Docker-based infrastructure

### Comprehensive Coverage

- **Functional Testing**: Core connector functionality
- **Performance Testing**: Throughput and latency verification
- **Error Handling**: Resilience and recovery scenarios
- **Configuration Testing**: Parameter validation and defaults
- **Integration Testing**: End-to-end data pipelines

### Realistic Scenarios

- Multiple partitions and topics
- Data transformations
- Error tolerance configuration
- High-volume message processing
- Connector lifecycle management

## Running the Tests

### Prerequisites

- Java 11 or higher
- Maven 3.6+
- Docker (for TestContainers)

### Execute Tests

```bash
# You have to build the project first
mvn clean install -DskipTests

# Run all tests including unit and integration
mvn verify

# Run only integration tests
mvn failsafe:integration-test

# Run specific test class
mvn test -Dtest=ExampleSinkConnectorIntegrationTest

# Run with specific profile
mvn integration-tests
```

### Maven Configuration

The integration tests are configured with:

- **maven-failsafe-plugin**: Executes integration tests
- **TestContainers**: Provides Kafka infrastructure
- **Awaitility**: Asynchronous testing utilities
- **AssertJ**: Enhanced assertions

## Test Configuration

### Connector Settings

Tests use realistic connector configurations:

```properties
# Source Connector
connector.class=com.example.kafka.connect.source.ExampleSourceConnector
topic.prefix=test-
poll.interval.ms=5000
batch.max.rows=100

# Sink Connector
connector.class=com.example.kafka.connect.sink.ExampleSinkConnector
topics=test-topic
batch.size=100
flush.timeout.ms=5000
```

### TestContainers Configuration

Kafka cluster setup:

- **Image**: apache/kafka:3.9.1
- **Auto-create topics**: Disabled
- **Network isolation**: Each test gets isolated network
- **Startup timeout**: 5 minutes

## Best Practices

### Test Design

1. **Isolation**: Each test is independent
2. **Cleanup**: Automatic resource cleanup
3. **Timeouts**: Reasonable wait times with Awaitility
4. **Logging**: Comprehensive test logging
5. **Assertions**: Clear and specific assertions

### Error Scenarios

- **Configuration Errors**: Invalid parameter handling
- **Network Failures**: Connection resilience
- **Data Corruption**: Malformed message handling
- **Resource Limits**: Memory and disk constraints

## Troubleshooting

### Common Issues

1. **Docker Not Available**

   ```shell
   Caused by: java.lang.IllegalStateException: Could not find a valid Docker environment
   ```

   - Ensure Docker is running
   - Check Docker socket permissions
   - If you are having arm64 and have issues using rancher desktop, try switching to colima or docker desktop. Also for rancher desktop, ensure you have configured docker.socket correctly.

2. **Port Conflicts**

   ```shell
   Caused by: org.testcontainers.containers.ContainerLaunchException: Container startup failed
   ```

   - TestContainers handles port allocation automatically
   - Check for conflicting services

3. **Timeout Issues**

   ```shell
   Condition was not fulfilled within 30 seconds
   ```

   - Increase timeout values
   - Check system resources
   - Review test logic

### Debug Mode

Enable debug logging:

```bash
mvn verify -Dlogback.configurationFile=logback-debug.xml
```

### Container Logs

Access container logs for debugging:

```bash
# Enable TestContainers debug
export TESTCONTAINERS_DEBUG=true
mvn verify
```

## Extending Tests

### Adding New Test Cases

1. **Create test method** in appropriate test class
2. **Use AbstractIntegrationTest** utilities
3. **Follow naming conventions**: `testFeatureName()`
4. **Add proper assertions** with meaningful messages
5. **Include cleanup** if needed

### Custom Test Infrastructure

```java
@Test
public void testCustomScenario() throws Exception {
    // Setup
    Map<String, String> config = createCustomConfig();
    getConnectRunner().createConnector(config);

    // Execute
    performTestActions();

    // Verify
    await().atMost(Duration.ofSeconds(30))
           .untilAsserted(() -> {
               // Your assertions here
               assertThat(result).isNotNull();
           });
}
```

## Continuous Integration

### GitHub Actions

Example CI configuration:

```yaml
name: Integration Tests
on: [push, pull_request]
jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up JDK 11
        uses: actions/setup-java@v3
        with:
          java-version: '11'
      - name: Run integration tests
        run: mvn verify
```

### Test Reports

Tests generate reports in:

- `target/failsafe-reports/`: Surefire reports
- `target/site/jacoco/`: Code coverage
- Container logs in TestContainers output

## Resources

- [Apache Kafka Connect Documentation](https://kafka.apache.org/documentation/#connect)
- [TestContainers Documentation](https://www.testcontainers.org/)
- [Aiven JDBC Connector](https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka)
- [Kafka Connect Testing Best Practices](https://docs.confluent.io/platform/current/connect/testing.html)
