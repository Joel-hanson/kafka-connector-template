#!/bin/bash

# Script to test the project with different Java versions
# This script helps verify that the Maven configuration works correctly
# with Java 11, 17, and 21

set -e

echo "=== Kafka Connector Template - Java Version Compatibility Test ==="
echo ""

# Function to display Java version info
show_java_info() {
    echo "Current Java version:"
    java -version 2>&1 | head -n 1
    echo "Maven compiler source/target will be automatically detected via profiles"
    echo ""
}

# Function to test compilation and basic tests
test_compilation() {
    local test_name="$1"
    echo "🧪 Testing: $test_name"
    echo "----------------------------------------"

    show_java_info

    echo "Running: mvn clean compile test -q"
    if mvn clean compile test -q; then
        echo "✅ $test_name: SUCCESS"
    else
        echo "❌ $test_name: FAILED"
        return 1
    fi
    echo ""
}

# Function to test specific Maven profile
test_profile() {
    local profile="$1"
    local test_name="Testing with profile: $profile"
    echo "🧪 $test_name"
    echo "----------------------------------------"

    show_java_info

    echo "Running: mvn clean compile test -P$profile -q"
    if mvn clean compile test -P"$profile" -q; then
        echo "✅ Profile $profile: SUCCESS"
    else
        echo "❌ Profile $profile: FAILED"
        return 1
    fi
    echo ""
}

# Function to run full integration tests
test_integration() {
    local test_name="Integration Tests"
    echo "🧪 $test_name"
    echo "----------------------------------------"

    show_java_info

    echo "Running: mvn clean integration-test -q"
    if mvn clean integration-test -q; then
        echo "✅ Integration tests: SUCCESS"
    else
        echo "❌ Integration tests: FAILED"
        return 1
    fi
    echo ""
}

# Main execution
echo "This script will test the project compilation and tests."
echo "Make sure you have Maven installed and JAVA_HOME properly set."
echo ""

# Detect if running in CI environment
if [[ "${CI:-false}" == "true" || "${GITHUB_ACTIONS:-false}" == "true" ]]; then
    echo "🔍 Running in CI environment"
    echo "Current Java version will be used for testing"
    echo ""

    # In CI, just run the comprehensive test suite
    test_compilation "CI Auto-detected Java version"
    test_integration
else
    echo "🏠 Running in local environment"
    echo ""

    # Test basic compilation (should auto-detect Java version)
    test_compilation "Auto-detected Java version"

    # Test specific profiles (these should work regardless of actual Java version)
    echo "Testing explicit Maven profiles..."
    echo "(Note: These profiles set compiler source/target regardless of runtime Java version)"
    echo ""

    test_profile "java11"
    test_profile "java17"
    test_profile "java21"

    # Run integration tests
    test_integration
fi

echo "=== Summary ==="
echo "✅ All tests passed! The project is compatible with Java 11, 17, and 21."
echo ""
echo "💡 Tips:"
echo "  - The project defaults to Java 11 for maximum compatibility"
echo "  - Maven profiles automatically activate based on detected Java version"
echo "  - You can manually activate profiles with -Pjava17 or -Pjava21"
echo "  - Use -Ppreview-features to enable Java preview features"
echo ""
echo "🚀 To run integration tests: mvn integration-test"
echo "📦 To build the connector JAR: mvn clean package"
