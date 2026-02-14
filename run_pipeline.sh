#!/bin/bash

# CDC Pipeline Orchestrator
# Starts the complete CDC pipeline: databases -> simulator -> extractor -> loader

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
LOG_LEVEL=${LOG_LEVEL:-INFO}
MUTATION_INTERVAL=${MUTATION_INTERVAL_SECONDS:-5}
CDC_INTERVAL=${CDC_EXTRACTION_INTERVAL_SECONDS:-10}

# Function to print colored output
print_status() {
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

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Function to wait for database to be ready
wait_for_db() {
    local host=$1
    local port=$2
    local db_name=$3
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for $db_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker exec $db_name pg_isready -U postgres -h localhost -p $2 > /dev/null 2>&1; then
            print_success "$db_name is ready!"
            return 0
        fi
        
        print_status "Attempt $attempt/$max_attempts: $db_name not ready yet..."
        sleep 2
        ((attempt++))
    done
    
    print_error "$db_name failed to become ready after $max_attempts attempts"
    return 1
}

# Function to start databases
start_databases() {
    print_status "Starting PostgreSQL databases..."
    
    # Check if containers already exist
    if docker ps -a | grep -q "operational_db"; then
        print_warning "operational_db container already exists. Stopping and removing..."
        docker stop operational_db 2>/dev/null || true
        docker rm operational_db 2>/dev/null || true
    fi
    
    if docker ps -a | grep -q "warehouse_db"; then
        print_warning "warehouse_db container already exists. Stopping and removing..."
        docker stop warehouse_db 2>/dev/null || true
        docker rm warehouse_db 2>/dev/null || true
    fi
    
    # Start databases
    docker-compose up -d
    
    # Wait for databases to be ready
    wait_for_db "localhost" "5434" "operational_db"
    wait_for_db "localhost" "5433" "warehouse_db"
    
    print_success "Both databases are running and ready!"
}

# Function to check environment
check_environment() {
    print_status "Checking environment..."
    
    # Check if .env file exists
    if [ ! -f ".env" ]; then
        print_warning ".env file not found. Creating from .env.example..."
        cp .env.example .env
    fi
    
    # Check Python dependencies
    if ! python3 -c "import psycopg2, faker" 2>/dev/null; then
        print_status "Installing Python dependencies..."
        pip3 install -r requirements.txt
    fi
    
    # Create logs directory
    mkdir -p logs
    
    print_success "Environment check completed!"
}

# Function to start simulator
start_simulator() {
    print_status "Starting database mutator simulator..."
    
    # Run in background
    python3 src/simulators/db_mutator.py > logs/simulator.log 2>&1 &
    SIMULATOR_PID=$!
    
    # Save PID for cleanup
    echo $SIMULATOR_PID > .simulator.pid
    
    print_success "Simulator started (PID: $SIMULATOR_PID)"
}

# Function to start CDC extractor
start_extractor() {
    print_status "Starting CDC extractor..."
    
    # Run in background
    python3 src/cdc/log_extractor.py > logs/extractor.log 2>&1 &
    EXTRACTOR_PID=$!
    
    # Save PID for cleanup
    echo $EXTRACTOR_PID > .extractor.pid
    
    print_success "CDC extractor started (PID: $EXTRACTOR_PID)"
}

# Function to run SCD2 loader
run_loader() {
    print_status "Running SCD Type 2 loader..."
    
    python3 src/warehouse/scd2_loader.py
    
    print_success "SCD Type 2 loader completed!"
}

# Function to cleanup processes
cleanup() {
    print_status "Cleaning up processes..."
    
    # Kill simulator if running
    if [ -f ".simulator.pid" ]; then
        SIM_PID=$(cat .simulator.pid)
        if kill -0 $SIM_PID 2>/dev/null; then
            print_status "Stopping simulator (PID: $SIM_PID)..."
            kill -TERM $SIM_PID 2>/dev/null || true
            sleep 2
            kill -KILL $SIM_PID 2>/dev/null || true
        fi
        rm -f .simulator.pid
    fi
    
    # Kill extractor if running
    if [ -f ".extractor.pid" ]; then
        EXT_PID=$(cat .extractor.pid)
        if kill -0 $EXT_PID 2>/dev/null; then
            print_status "Stopping CDC extractor (PID: $EXT_PID)..."
            kill -TERM $EXT_PID 2>/dev/null || true
            sleep 2
            kill -KILL $EXT_PID 2>/dev/null || true
        fi
        rm -f .extractor.pid
    fi
    
    print_success "Cleanup completed!"
}

# Function to show status
show_status() {
    print_status "Pipeline Status:"
    echo "=================="
    
    # Database status
    if docker ps | grep -q "operational_db"; then
        echo "✓ operational_db: Running (port 5434)"
    else
        echo "✗ operational_db: Not running"
    fi
    
    if docker ps | grep -q "warehouse_db"; then
        echo "✓ warehouse_db: Running (port 5433)"
    else
        echo "✗ warehouse_db: Not running"
    fi
    
    # Process status
    if [ -f ".simulator.pid" ]; then
        SIM_PID=$(cat .simulator.pid)
        if kill -0 $SIM_PID 2>/dev/null; then
            echo "✓ Simulator: Running (PID: $SIM_PID)"
        else
            echo "✗ Simulator: Not running"
        fi
    else
        echo "✗ Simulator: Not started"
    fi
    
    if [ -f ".extractor.pid" ]; then
        EXT_PID=$(cat .extractor.pid)
        if kill -0 $EXT_PID 2>/dev/null; then
            echo "✓ CDC Extractor: Running (PID: $EXT_PID)"
        else
            echo "✗ CDC Extractor: Not running"
        fi
    else
        echo "✗ CDC Extractor: Not started"
    fi
    
    echo ""
    echo "Log files:"
    echo "- Simulator: logs/simulator.log"
    echo "- Extractor: logs/extractor.log"
    echo "- SCD2 Loader: logs/src_warehouse_scd2_loader.log"
}

# Function to show help
show_help() {
    echo "CDC Pipeline Orchestrator"
    echo "========================="
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     Start the complete pipeline (default)"
    echo "  status    Show pipeline status"
    echo "  stop      Stop all pipeline components"
    echo "  restart   Restart the pipeline"
    echo "  loader    Run only the SCD2 loader"
    echo "  help      Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  LOG_LEVEL                    Logging level (DEBUG, INFO, WARNING, ERROR)"
    echo "  MUTATION_INTERVAL_SECONDS    Simulator interval in seconds (default: 5)"
    echo "  CDC_EXTRACTION_INTERVAL_SECONDS CDC extractor interval in seconds (default: 10)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Start complete pipeline"
    echo "  $0 status           # Show status"
    echo "  $0 stop             # Stop all components"
    echo "  LOG_LEVEL=DEBUG $0  # Start with debug logging"
}

# Set up signal handlers for graceful shutdown
trap cleanup EXIT INT TERM

# Main script logic
case "${1:-start}" in
    "start")
        print_status "Starting CDC Pipeline..."
        echo ""
        
        check_docker
        check_environment
        start_databases
        
        # Give databases a moment to fully initialize
        sleep 3
        
        start_simulator
        start_extractor
        
        print_status ""
        print_success "CDC Pipeline started successfully!"
        echo ""
        print_status "Components running:"
        echo "- operational_db: localhost:5434"
        echo "- warehouse_db: localhost:5433"
        echo "- Simulator: Running in background"
        echo "- CDC Extractor: Running in background"
        echo ""
        print_status "To run the SCD2 loader manually:"
        echo "  $0 loader"
        echo ""
        print_status "To check status:"
        echo "  $0 status"
        echo ""
        print_status "To stop the pipeline:"
        echo "  $0 stop"
        echo ""
        print_status "Log files available in logs/ directory"
        
        # Keep script running to maintain background processes
        print_status "Press Ctrl+C to stop the pipeline..."
        while true; do
            sleep 10
        done
        ;;
        
    "status")
        show_status
        ;;
        
    "stop")
        cleanup
        print_success "Pipeline stopped!"
        ;;
        
    "restart")
        print_status "Restarting pipeline..."
        cleanup
        sleep 2
        exec "$0" start
        ;;
        
    "loader")
        check_environment
        run_loader
        ;;
        
    "help"|"-h"|"--help")
        show_help
        ;;
        
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
