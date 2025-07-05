#!/bin/bash

# Integration Test Runner for Kafka Connector
# This script provides convenient commands to run different types of integration tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    # Check Java
    if ! command -v java &> /dev/null; then
        print_error "Java is not installed or not in PATH"
        exit 1
    fi
    
    # Check Maven
    if ! command -v mvn &> /dev/null; then
        print_error "Maven is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Function to run all integration tests
run_all_tests() {
    print_info "Running all integration tests..."
    mvn clean verify -DskipUnitTests
    if [ $? -eq 0 ]; then
        print_success "All integration tests passed!"
    else
        print_error "Some integration tests failed"
        exit 1
    fi
}

# Function to run specific test class
run_specific_test() {
    local test_class=$1
    print_info "Running integration test: $test_class"
    mvn test -Dtest="$test_class"
    if [ $? -eq 0 ]; then
        print_success "Test $test_class passed!"
    else
        print_error "Test $test_class failed"
        exit 1
    fi
}

# Function to run sink connector tests
run_sink_tests() {
    print_info "Running Sink Connector integration tests..."
    run_specific_test "ExampleSinkConnectorIntegrationTest"
}

# Function to run source connector tests
run_source_tests() {
    print_info "Running Source Connector integration tests..."
    run_specific_test "ExampleSourceConnectorIntegrationTest"
}

# Function to run end-to-end tests
run_e2e_tests() {
    print_info "Running End-to-End integration tests..."
    run_specific_test "EndToEndIntegrationTest"
}

# Function to run configuration tests
run_config_tests() {
    print_info "Running Configuration integration tests..."
    run_specific_test "ConnectorConfigurationIntegrationTest"
}

# Function to build project
build_project() {
    print_info "Building project..."
    mvn clean compile
    if [ $? -eq 0 ]; then
        print_success "Project built successfully!"
    else
        print_error "Project build failed"
        exit 1
    fi
}

# Function to show test reports
show_reports() {
    print_info "Test reports location:"
    echo "  - Failsafe reports: target/failsafe-reports/"
    echo "  - Surefire reports: target/surefire-reports/"
    
    if [ -d "target/failsafe-reports" ]; then
        echo ""
        print_info "Recent test results:"
        ls -la target/failsafe-reports/*.xml 2>/dev/null || echo "  No test results found"
    fi
}

# Function to clean up
cleanup() {
    print_info "Cleaning up test artifacts..."
    mvn clean
    docker system prune -f --filter "label=org.testcontainers=true" 2>/dev/null || true
    print_success "Cleanup completed"
}

# Function to show usage
show_usage() {
    echo "Integration Test Runner for Kafka Connector"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  all          Run all integration tests"
    echo "  sink         Run sink connector tests"
    echo "  source       Run source connector tests"
    echo "  e2e          Run end-to-end tests"
    echo "  config       Run configuration tests"
    echo "  build        Build the project"
    echo "  reports      Show test reports location"
    echo "  cleanup      Clean up test artifacts"
    echo "  check        Check prerequisites"
    echo "  help         Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all           # Run all integration tests"
    echo "  $0 sink          # Run only sink connector tests"
    echo "  $0 e2e           # Run end-to-end pipeline tests"
    echo ""
}

# Main script logic
main() {
    case "${1:-help}" in
        "all")
            check_prerequisites
            build_project
            run_all_tests
            show_reports
            ;;
        "sink")
            check_prerequisites
            build_project
            run_sink_tests
            ;;
        "source")
            check_prerequisites
            build_project
            run_source_tests
            ;;
        "e2e")
            check_prerequisites
            build_project
            run_e2e_tests
            ;;
        "config")
            check_prerequisites
            build_project
            run_config_tests
            ;;
        "build")
            check_prerequisites
            build_project
            ;;
        "reports")
            show_reports
            ;;
        "cleanup")
            cleanup
            ;;
        "check")
            check_prerequisites
            ;;
        "help"|*)
            show_usage
            ;;
    esac
}

# Run main function with all arguments
main "$@"
