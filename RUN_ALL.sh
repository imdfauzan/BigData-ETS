#!/bin/bash

################################################################################
#                                                                              #
#  HargaPangan Monitor - MASTER START SCRIPT                                 #
#  Menjalankan semua komponen sistem dari satu file                          #
#                                                                              #
#  Usage: ./RUN_ALL.sh [start|stop|status|demo]                              #
#                                                                              #
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="$PROJECT_DIR/venv"
LOGS_DIR="$PROJECT_DIR/logs"
DATA_DIR="$PROJECT_DIR/dashboard/data"

# PIDs file
PIDS_FILE="$PROJECT_DIR/.running_pids"

# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

print_header() {
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# ─────────────────────────────────────────────────────────────────────────────
# PREREQUISITE CHECKS
# ─────────────────────────────────────────────────────────────────────────────

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check venv
    if [ ! -d "$VENV_PATH" ]; then
        print_error "Virtual environment not found at $VENV_PATH"
        echo "Creating virtual environment..."
        python3 -m venv "$VENV_PATH"
        print_success "Virtual environment created"
    fi
    
    # Activate venv
    source "$VENV_PATH/bin/activate"
    print_success "Virtual environment activated"
    
    # Check Python packages
    print_info "Checking Python dependencies..."
    if ! python -c "import kafka" 2>/dev/null; then
        print_warning "kafka-python not found, installing..."
        pip install kafka-python==2.0.2 six --quiet
        print_success "kafka-python installed"
    fi
    
    if ! python -c "import pyspark" 2>/dev/null; then
        print_warning "pyspark not found, installing..."
        pip install pyspark pandas --quiet
        print_success "pyspark installed"
    fi
    
    if ! python -c "import flask" 2>/dev/null; then
        print_warning "flask not found, installing..."
        pip install flask --quiet
        print_success "flask installed"
    fi
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker."
        exit 1
    fi
    print_success "Docker found"
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose not found. Please install Docker Compose."
        exit 1
    fi
    print_success "Docker Compose found"
    
    # Create logs directory
    mkdir -p "$LOGS_DIR"
    mkdir -p "$DATA_DIR"
    
    print_success "All prerequisites OK"
}

# ─────────────────────────────────────────────────────────────────────────────
# DOCKER MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

start_docker_services() {
    print_header "Starting Docker Services"
    
    cd "$PROJECT_DIR"
    
    # Start Kafka & ZooKeeper
    print_info "Starting Kafka & ZooKeeper..."
    docker-compose -f docker-compose-kafka.yml up -d
    
    # Start Hadoop HDFS
    print_info "Starting Hadoop HDFS..."
    docker-compose -f docker-compose-hadoop.yml up -d
    
    sleep 10
    
    # Verify containers
    if docker ps | grep -q kafka-broker; then
        print_success "Kafka broker is running"
    else
        print_error "Kafka broker failed to start"
        return 1
    fi
    
    if docker ps | grep -q namenode; then
        print_success "Hadoop namenode is running"
    else
        print_error "Hadoop namenode failed to start"
        return 1
    fi
    
    print_success "Docker services started"
}

stop_docker_services() {
    print_header "Stopping Docker Services"
    
    cd "$PROJECT_DIR"
    
    docker-compose -f docker-compose-kafka.yml down
    docker-compose -f docker-compose-hadoop.yml down
    
    print_success "Docker services stopped"
}

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

start_producer_api() {
    print_info "Starting producer_api.py..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    python kafka/producer_api.py > "$LOGS_DIR/producer_api.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 2
    if kill -0 $PID 2>/dev/null; then
        print_success "producer_api.py started (PID: $PID)"
    else
        print_error "producer_api.py failed to start"
        return 1
    fi
}

start_producer_rss() {
    print_info "Starting producer_rss.py..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    python kafka/producer_rss.py > "$LOGS_DIR/producer_rss.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 2
    if kill -0 $PID 2>/dev/null; then
        print_success "producer_rss.py started (PID: $PID)"
    else
        print_error "producer_rss.py failed to start"
        return 1
    fi
}

start_consumer() {
    print_info "Starting consumer_to_hdfs.py..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    python kafka/consumer_to_hdfs.py > "$LOGS_DIR/consumer.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 2
    if kill -0 $PID 2>/dev/null; then
        print_success "consumer_to_hdfs.py started (PID: $PID)"
    else
        print_error "consumer_to_hdfs.py failed to start"
        return 1
    fi
}

start_spark_analysis() {
    print_info "Starting spark/analysis.py..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    python spark/analysis.py > "$LOGS_DIR/spark_analysis.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 5
    if kill -0 $PID 2>/dev/null; then
        print_success "spark/analysis.py started (PID: $PID)"
    else
        print_warning "spark/analysis.py may still be initializing..."
    fi
}

start_dashboard() {
    print_info "Starting dashboard/app.py..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    python dashboard/app.py > "$LOGS_DIR/dashboard.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 3
    if kill -0 $PID 2>/dev/null; then
        print_success "dashboard/app.py started (PID: $PID)"
    else
        print_error "dashboard/app.py failed to start"
        return 1
    fi
}

start_bronze_lakehouse() {
    print_info "Starting Bronze Layer (lakehouse/01_bronze.py)..."
    source "$VENV_PATH/bin/activate"
    cd "$PROJECT_DIR"
    
    # Check if delta-spark is installed
    if ! python -c "import delta" 2>/dev/null; then
        print_warning "delta-spark not found, installing..."
        pip install delta-spark==3.1.0 --quiet
        print_success "delta-spark installed"
    fi
    
    python lakehouse/01_bronze.py > "$LOGS_DIR/bronze.log" 2>&1 &
    PID=$!
    echo $PID >> "$PIDS_FILE"
    
    sleep 5
    if kill -0 $PID 2>/dev/null; then
        print_success "Bronze Layer started (PID: $PID)"
    else
        print_warning "Bronze Layer may still be initializing..."
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

cmd_start() {
    print_header "STARTING HARGAPANGAN MONITOR SYSTEM"
    
    # Clear old PIDs
    rm -f "$PIDS_FILE"
    
    # Kill any existing processes
    pkill -f "python kafka\|python spark\|python dashboard\|python lakehouse" 2>/dev/null || true
    sleep 2
    
    # Check prerequisites
    check_prerequisites
    
    # Start services
    start_docker_services || exit 1
    
    sleep 10
    
    # Start producers & consumer
    start_producer_api || exit 1
    start_producer_rss || exit 1
    start_consumer || exit 1
    
    sleep 5
    
    # Start Bronze Layer (reads from HDFS)
    start_bronze_lakehouse || true
    
    sleep 3
    
    # Start analysis
    start_spark_analysis || true
    
    sleep 5
    
    # Start dashboard
    start_dashboard || exit 1
    
    print_header "✓ SYSTEM STARTED SUCCESSFULLY"
    echo ""
    echo -e "${GREEN}All components are running!${NC}"
    echo ""
    echo "📊 Dashboard: ${BLUE}http://localhost:5000${NC}"
    echo "📡 API: ${BLUE}http://localhost:5000/api/data${NC}"
    echo "📝 Logs: ${BLUE}$LOGS_DIR/${NC}"
    echo ""
    echo "Running processes:"
    ps aux | grep -E "producer_api|producer_rss|consumer|spark|app.py" | grep -v grep | awk '{printf "  PID %6s: %s\n", $2, $11}'
    echo ""
}

cmd_stop() {
    print_header "STOPPING HARGAPANGAN MONITOR SYSTEM"
    
    # Stop Python processes
    print_info "Stopping Python processes..."
    pkill -f "python kafka\|python spark\|python dashboard\|python lakehouse" 2>/dev/null || true
    
    sleep 2
    
    # Stop Docker services
    stop_docker_services
    
    # Clear PIDs file
    rm -f "$PIDS_FILE"
    
    print_success "System stopped"
}

cmd_status() {
    print_header "SYSTEM STATUS"
    
    echo ""
    echo "🐳 Docker Containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "kafka|zookeeper|namenode|datanode" || echo "  None running"
    
    echo ""
    echo "🔄 Python Processes:"
    if ps aux | grep -q "producer_api.py"; then
        echo "  ✓ producer_api.py $(ps aux | grep 'producer_api.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ producer_api.py"
    fi
    
    if ps aux | grep -q "producer_rss.py"; then
        echo "  ✓ producer_rss.py $(ps aux | grep 'producer_rss.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ producer_rss.py"
    fi
    
    if ps aux | grep -q "consumer_to_hdfs.py"; then
        echo "  ✓ consumer_to_hdfs.py $(ps aux | grep 'consumer_to_hdfs.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ consumer_to_hdfs.py"
    fi
    
    if ps aux | grep -q "spark/analysis.py"; then
        echo "  ✓ spark/analysis.py $(ps aux | grep 'spark/analysis.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ spark/analysis.py"
    fi
    
    if ps aux | grep -q "dashboard/app.py"; then
        echo "  ✓ dashboard/app.py $(ps aux | grep 'dashboard/app.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ dashboard/app.py"
    fi
    
    if ps aux | grep -q "01_bronze.py"; then
        echo "  ✓ lakehouse/01_bronze.py $(ps aux | grep '01_bronze.py' | grep -v grep | awk '{print "PID", $2}')"
    else
        echo "  ✗ lakehouse/01_bronze.py"
    fi
    
    echo ""
    echo "📊 Dashboard:"
    if curl -s http://localhost:5000 > /dev/null 2>&1; then
        echo "  ✓ Running on http://localhost:5000"
    else
        echo "  ✗ Not accessible"
    fi
    
    echo ""
    echo "📁 Data Files:"
    [ -f "$DATA_DIR/live_api.json" ] && echo "  ✓ live_api.json ($(wc -l < "$DATA_DIR/live_api.json") lines)" || echo "  ✗ live_api.json"
    [ -f "$DATA_DIR/live_rss.json" ] && echo "  ✓ live_rss.json ($(wc -l < "$DATA_DIR/live_rss.json") lines)" || echo "  ✗ live_rss.json"
    [ -f "$DATA_DIR/spark_results.json" ] && echo "  ✓ spark_results.json" || echo "  ✗ spark_results.json"
    
    echo ""
    echo "🏠 Lakehouse (Bronze Layer):"
    if [ -d "$PROJECT_DIR/lakehouse_data/bronze" ]; then
        PARQUET_COUNT=$(find "$PROJECT_DIR/lakehouse_data/bronze" -type f -name "*.parquet" 2>/dev/null | wc -l)
        echo "  ✓ Bronze layer exists ($(du -sh "$PROJECT_DIR/lakehouse_data/bronze" 2>/dev/null | cut -f1))"
        echo "    • API data: $([ -d "$PROJECT_DIR/lakehouse_data/bronze/pangan_api" ] && echo "✓" || echo "✗")"
        echo "    • RSS data: $([ -d "$PROJECT_DIR/lakehouse_data/bronze/pangan_rss" ] && echo "✓" || echo "✗")"
    else
        echo "  ✗ Bronze layer not created yet"
    fi
    
    echo ""
}

cmd_demo() {
    print_header "LIVE DEMO MONITORING"
    
    echo ""
    echo "Monitoring data flow for 60 seconds..."
    echo ""
    
    for i in {1..12}; do
        clear
        echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
        echo -e "${BLUE}  HargaPangan Monitor - Live Data Flow${NC}"
        echo -e "${BLUE}  Cycle $i/12 (5 seconds each)${NC}"
        echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
        echo ""
        
        echo "📊 Latest Data Status:"
        curl -s http://localhost:5000/api/data 2>/dev/null | python3 << 'EOF' || echo "  API not responding..."
import sys, json
from datetime import datetime
try:
    d = json.load(sys.stdin)
    print(f"  Timestamp: {d.get('server_timestamp', 'N/A')}")
    print(f"  Live Harga Records: {len(d.get('live_harga', []))}")
    print(f"  Berita Articles: {len(d.get('berita', []))}")
    
    spark = d.get('spark', {})
    if spark:
        vol = spark.get('analisis_1_volatilitas', [])
        print(f"  Spark Analyses: {len(vol)} commodities")
        if vol:
            print(f"    • {vol[0]['label']}: {vol[0]['indeks_volatilitas']}% volatility")
    
    print("")
    print("📡 Latest API Message:")
    harga = d.get('live_harga', [])
    if harga:
        latest = harga[-1]
        print(f"  Komoditas: {latest.get('label', 'N/A')}")
        print(f"  Harga: Rp {latest.get('harga', 0):,.0f}/{latest.get('satuan', 'kg')}")
    
    print("")
    print("📰 Latest News:")
    berita = d.get('berita', [])
    if berita:
        latest_news = berita[0]
        title = latest_news.get('title', 'N/A')[:60]
        print(f"  {title}...")
except:
    print("  (API not ready yet)")
EOF
        
        echo ""
        echo "📁 File Sizes:"
        [ -f "$DATA_DIR/live_api.json" ] && ls -lh "$DATA_DIR/live_api.json" | awk '{print "  live_api.json: " $5}'
        [ -f "$DATA_DIR/live_rss.json" ] && ls -lh "$DATA_DIR/live_rss.json" | awk '{print "  live_rss.json: " $5}'
        
        sleep 5
    done
    
    print_header "Demo Complete!"
}

cmd_logs() {
    print_header "VIEWING LOGS"
    
    tail_lines=${2:-50}
    
    case ${1:-all} in
        api)
            echo "📋 producer_api.log:"
            tail -n $tail_lines "$LOGS_DIR/producer_api.log"
            ;;
        rss)
            echo "📋 producer_rss.log:"
            tail -n $tail_lines "$LOGS_DIR/producer_rss.log"
            ;;
        consumer)
            echo "📋 consumer.log:"
            tail -n $tail_lines "$LOGS_DIR/consumer.log"
            ;;
        spark)
            echo "📋 spark_analysis.log:"
            tail -n $tail_lines "$LOGS_DIR/spark_analysis.log"
            ;;
        dashboard)
            echo "📋 dashboard.log:"
            tail -n $tail_lines "$LOGS_DIR/dashboard.log"
            ;;
        bronze)
            echo "📋 bronze.log:"
            tail -n $tail_lines "$LOGS_DIR/bronze.log"
            ;;
        all)
            echo "📋 All logs (latest 20 lines each):"
            for log in "$LOGS_DIR"/*.log; do
                echo ""
                echo "=== $(basename $log) ==="
                tail -n 20 "$log"
            done
            ;;
    esac
}

cmd_help() {
    cat << EOF

${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}
${BLUE}║  HargaPangan Monitor - Master Control Script                ║${NC}
${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}

${GREEN}USAGE:${NC}
  $0 <command> [options]

${GREEN}COMMANDS:${NC}
  start          Start all components (Docker, Kafka, Hadoop, producers, consumer, Bronze Layer, Spark, dashboard)
  stop           Stop all components gracefully
  status         Show status of all components
  demo           Watch live data flow for 60 seconds
  logs [type]    View logs (api|rss|consumer|spark|dashboard|bronze|all)
  help           Show this help message

${GREEN}EXAMPLES:${NC}
  # Start everything
  $0 start

  # Check system status
  $0 status

  # Watch live data flow
  $0 demo

  # View API producer logs
  $0 logs api

  # View bronze layer logs
  $0 logs bronze

  # View all logs
  $0 logs all

  # Stop everything
  $0 stop

${GREEN}DASHBOARD:${NC}
  http://localhost:5000

${GREEN}API ENDPOINT:${NC}
  http://localhost:5000/api/data

${GREEN}LAKEHOUSE (BRONZE LAYER):${NC}
  Location: ./lakehouse_data/bronze/
  API Data: ./lakehouse_data/bronze/pangan_api/
  RSS Data: ./lakehouse_data/bronze/pangan_rss/
  Format: Delta Lake (Parquet + metadata)

${YELLOW}NOTES:${NC}
  • First start may take 30-60 seconds to initialize Kafka/Hadoop
  • Bronze Layer runs automatically and reads data from HDFS
  • Logs are saved in: $LOGS_DIR/
  • Data files are saved in: $DATA_DIR/
  • Lakehouse data is saved in: ./lakehouse_data/

EOF
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

main() {
    command="${1:-help}"
    
    case "$command" in
        start)
            cmd_start
            ;;
        stop)
            cmd_stop
            ;;
        status)
            cmd_status
            ;;
        demo)
            cmd_demo
            ;;
        logs)
            cmd_logs "$2" "$3"
            ;;
        help|--help|-h)
            cmd_help
            ;;
        *)
            print_error "Unknown command: $command"
            echo ""
            cmd_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
