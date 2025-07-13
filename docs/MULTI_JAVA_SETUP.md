# Multi-Java Version Testing Setup

This document explains how the project is configured to support and test with Java 11, 17, and 21.

## Maven Configuration

### Properties

The `pom.xml` includes version-specific properties and plugin configurations:

```xml
<properties>
    <!-- Default to Java 11 for maximum compatibility -->
    <java.version>11</java.version>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
    <maven.compiler.release>11</maven.compiler.release>
    
    <!-- Plugin versions ensuring compatibility -->
    <maven.compiler.plugin.version>3.12.1</maven.compiler.plugin.version>
    <maven.surefire.plugin.version>3.2.5</maven.surefire.plugin.version>
    <maven.failsafe.plugin.version>3.2.5</maven.failsafe.plugin.version>
</properties>
```

### Profiles

The project uses Maven profiles to automatically detect and configure for different Java versions:

#### Java 11 Profile (Default)

```xml
<profile>
    <id>java11</id>
    <activation>
        <jdk>[11,17)</jdk>
    </activation>
    <properties>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
        <maven.compiler.release>11</maven.compiler.release>
    </properties>
</profile>
```

#### Java 17 Profile

```xml
<profile>
    <id>java17</id>
    <activation>
        <jdk>[17,21)</jdk>
    </activation>
    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <maven.compiler.release>17</maven.compiler.release>
    </properties>
</profile>
```

#### Java 21 Profile

```xml
<profile>
    <id>java21</id>
    <activation>
        <jdk>[21,)</jdk>
    </activation>
    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <maven.compiler.release>21</maven.compiler.release>
    </properties>
</profile>
```

## GitHub Actions Workflows

### 1. Basic CI (`ci.yml`)

- Quick validation with Java 11
- Build matrix testing with Java 11, 17, 21
- Fast feedback for pull requests

### 2. Comprehensive CI (`ci-multi-java.yml`)

- Multi-stage pipeline with proper dependencies
- Separate jobs for validation, unit tests, integration tests, and build verification
- Artifacts upload for test reports and built JARs
- Test result publishing

### 3. Integration Tests (`integration-tests.yml`)

- Focused on running the comprehensive test script
- Matrix strategy for all Java versions
- Detailed test reporting

## Key Features

### Automatic Profile Detection

Maven automatically activates the appropriate profile based on the detected Java version:

- Java 11.x.x → `java11` profile
- Java 17.x.x → `java17` profiles
- Java 21.x.x → `java21` profiles

### Manual Profile Selection

You can also manually specify profiles:

```bash
# Force Java 11 compilation settings
mvn clean compile -Pjava11

# Force Java 17 compilation settings
mvn clean compile -Pjava17

# Force Java 21 compilation settings
mvn clean compile -Pjava21
```

### Local Testing Script

The `scripts/test-java-versions.sh` script provides comprehensive local testing:

```bash
# Run all compatibility tests
./scripts/test-java-versions.sh

# The script automatically detects CI environment
# and adjusts behavior accordingly
```

## Dependency Compatibility

All dependencies are chosen to be compatible with Java 11+:

- **Kafka Connect**: 3.9.1 (LTS, supports Java 11+)
- **JUnit**: 5.11.4 (supports Java 8+)
- **Mockito**: 5.15.2 (supports Java 8+)
- **TestContainers**: 1.21.3 (supports Java 8+)
- **Log4j**: 2.24.3 (supports Java 8+)

## Testing Strategy

### Unit Tests

- Run on all Java versions (11, 17, 21)
- Use version-specific Maven profiles
- Include compatibility checks

### Integration Tests

- Full end-to-end testing with TestContainers
- Kafka Connect runtime testing
- Real connector functionality validation

### Build Verification

- JAR creation and validation
- Dependency verification
- Artifact integrity checks

## Best Practices

1. **Default to Java 11**: Ensures maximum compatibility
2. **Use `maven.compiler.release`**: Enforces API compatibility
3. **Profile-based configuration**: Automatic adaptation to runtime Java version
4. **Comprehensive CI matrix**: Test all supported Java versions
5. **Artifact preservation**: Save test reports and build artifacts
6. **Fail-fast validation**: Quick checks before expensive operations

## Running Tests Locally

```bash
# Test with current Java version
mvn clean test

# Test with specific profile
mvn clean test -Pjava17

# Run integration tests
mvn clean integration-test

# Run the comprehensive test script
./scripts/test-java-versions.sh

# Build for specific Java version
mvn clean package -Pjava21 -DskipTests
```

## Troubleshooting

### CI Cache Issues

GitHub Actions uses separate caches for each Java version to avoid conflicts:

```yaml
key: ${{ runner.os }}-m2-java${{ matrix.java }}-${{ hashFiles('**/pom.xml') }}
```

This ensures that dependencies compiled with different Java versions don't interfere with each other.
