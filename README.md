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

- Java 11 or higher
- Maven 3.6.3 or higher
- Docker and Docker Compose (for local development)
- Apache Kafka 3.x (or preferred version)

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
- End-to-end pipeline testing
- Configuration validation tests
- Performance testing
- Error handling verification

**Run integration tests:**

```bash
# Run all integration tests
mvn verify

# Use the convenient test runner script
./run-integration-tests.sh all

# Run specific test categories
./run-integration-tests.sh sink     # Sink connector tests
./run-integration-tests.sh source   # Source connector tests
./run-integration-tests.sh e2e      # End-to-end tests
./run-integration-tests.sh config   # Configuration tests
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

3. **End-to-End Tests** (`EndToEndIntegrationTest`)
   - Complete data pipelines
   - Data transformations
   - Multi-topic processing
   - Performance under load

4. **Configuration Tests** (`ConnectorConfigurationIntegrationTest`)
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
