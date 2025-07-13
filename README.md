# Kafka Connector Template

[![Build Status](https://github.com/joel-hanson/kafka-connector-template/workflows/Build/badge.svg)](https://github.com/joel-hanson/kafka-connector-template/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A comprehensive template for creating Apache Kafka Connect source and sink connectors, built with Maven. This template provides a foundation for developing production-ready connectors with proper configuration, testing, and deployment capabilities.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
  - [Building the Connector](#building-the-connector)
  - [Running with Docker](#running-with-docker)
  - [Manual Installation](#manual-installation)
- [Configuration](#configuration)
  - [Source Connector Configuration](#source-connector-configuration)
  - [Sink Connector Configuration](#sink-connector-configuration)
- [Development](#development)
  - [Java Version Support](#java-version-support)
  - [Adding New Functionality](#adding-new-functionality)
  - [Testing](#testing)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
    - [Docker Compose Testing](#docker-compose-testing)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

## Overview

This template provides a foundation for creating both source and sink Kafka connectors:

- **Source Connector**: Pulls data from an external system into Kafka topics
- **Sink Connector**: Exports data from Kafka topics to an external system

The template includes proper configuration classes, comprehensive tests, Docker setup for local development, and GitHub Actions workflows for CI/CD.

## Project Structure

```shell
├── src/main/java/             # Main source code
│   └── com/example/kafka/connect/
│       ├── sink/              # Sink connector implementation
│       └── source/            # Source connector implementation
├── src/test/                  # Test code
├── config/                    # Example configurations
└── .github/                   # GitHub templates and workflows
```

## Prerequisites

- **Java**: Java 11, 17, or 21 (LTS versions recommended)
- **Maven**: 3.6.3 or higher
- **Docker and Docker Compose**: For local development and testing
- **Apache Kafka**: 3.x (or preferred version)

### Java Version Compatibility

This project supports multiple Java versions through Maven profiles:

- **Java 11**: Minimum supported version (default)
- **Java 17**: Recommended for production use
- **Java 21**: Latest LTS version with enhanced performance

#### Testing with Different Java Versions

```bash
# Test with Java 11 (default)
mvn clean test

# Explicitly test with Java 17
mvn clean test -Pjava17

# Test with Java 21
mvn clean test -Pjava21

# Enable preview features (for testing newer Java features)
mvn clean test -Ppreview-features
```

#### Setting Java Version via Environment

You can also override the Java version using Maven properties:

```bash
# Compile for Java 17
mvn clean compile -Dmaven.compiler.source=17 -Dmaven.compiler.target=17

# Compile for Java 21
mvn clean compile -Dmaven.compiler.source=21 -Dmaven.compiler.target=21
```

## Getting Started

### Building the Connector

Clone the repository and build the project:

```bash
git clone https://github.com/joel-hanson/kafka-connector-template.git
cd kafka-connector-template
mvn clean package
```

This will create a JAR file in the `target/` directory with all dependencies included.

### Running with Docker

The easiest way to get started is using the provided Docker Compose setup:

```bash
cd docker
docker-compose up -d
```

This will start:

- Zookeeper
- Kafka broker
- Kafka Connect with the connector plugin pre-installed
- Schema Registry (optional)

You can then configure the connectors using the Kafka Connect REST API.

### Manual Installation

1. Build the connector JAR as described above
2. Copy the JAR file to the Kafka Connect plugins directory:

   ```bash
   cp target/kafka-connector-template-*.jar $KAFKA_CONNECT_PLUGINS_DIR/
   ```

3. Restart Kafka Connect

## Configuration

### Source Connector Configuration

Create a file named `source-connector.properties` with the following content:

```properties
name=example-source-connector
connector.class=com.example.kafka.connect.source.ExampleSourceConnector
tasks.max=1
topics=example-topic
# Custom connector configuration
example.source.batch.size=100
example.source.poll.interval.ms=1000
# Add other configuration properties as needed
```

To deploy the connector:

```bash
curl -X POST -H "Content-Type: application/json" --data @config/json/source-connector.json http://localhost:8083/connectors
```

### Sink Connector Configuration

Create a file named `sink-connector.properties` with the following content:

```properties
name=example-sink-connector
connector.class=com.example.kafka.connect.sink.ExampleSinkConnector
tasks.max=1
topics=example-topic
# Custom connector configuration
example.sink.batch.size=100
# Add other configuration properties as needed
```

To deploy the connector:

```bash
curl -X POST -H "Content-Type: application/json" --data @config/json/sink-connector.json http://localhost:8083/connectors
```

## Development

### Java Version Support

This project is configured to work with Java 11, 17, and 21. Maven profiles automatically detect and configure the appropriate Java version:

- **Automatic Detection**: Maven profiles activate based on the detected Java version
- **Cross-Platform**: Works on all major operating systems
- **CI/CD**: GitHub Actions test against all supported Java versions

### Available Maven Profiles

- `java11` - Activated when using Java 11
- `java17` - Activated when using Java 17  
- `java21` - Activated when using Java 21

### Adding New Functionality

1. Create or modify the connector configuration class to add new configuration options
2. Implement the functionality in the connector or task classes
3. Add appropriate tests

### Testing

The project includes comprehensive unit and integration tests.

#### Unit Tests

Run unit tests with:

```bash
mvn test
```

#### Integration Tests

The project includes extensive integration tests using TestContainers that verify end-to-end functionality with real Kafka infrastructure. These tests are based on the testing framework from the [Aiven JDBC Connector for Apache Kafka](https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka).

**Features:**

- Real Kafka cluster using TestContainers
- Source and Sink connector integration tests
- Configuration validation tests
- Error handling verification

**Run integration tests:**

```bash
# Run all integration tests
mvn verify
```

**Test Categories:**

1. **Sink Connector Tests** (`ExampleSinkConnectorIntegrationTest`)
   - Basic message consumption
   - Multi-partition handling
   - JSON message processing
   - Performance testing
   - Error scenarios

2. **Source Connector Tests** (`ExampleSourceConnectorIntegrationTest`)
   - Message production
   - Multiple table handling
   - Custom configurations
   - Message format validation

3. **Configuration Tests** (`ConnectorConfigurationIntegrationTest`)
   - Configuration validation
   - Default value verification
   - Error handling

For more details, see the [Integration Tests README](src/test/java/com/example/kafka/connect/integration/README.md).

#### Docker Compose Testing

You can also test with Docker Compose for manual verification:

```bash
# Start the required services
docker-compose -f docker-compose.yml up -d

# Run the tests
mvn verify

# Stop the services
docker-compose -f docker-compose.yml down
```

Verify the working of connector:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic example-source-topic --from-beginning --property print.key=true --property print.offset=true
```

You will see the events from the topic.

## Deployment

The connector can be deployed in several ways:

1. **Manual deployment**: Copy the JAR to the Kafka Connect plugins directory
2. **Confluent Hub**: Package the connector for distribution via Confluent Hub
3. **Docker**: Use the provided Dockerfile to create a custom Connect image with the connector

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
