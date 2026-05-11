#!/usr/bin/env python3
"""
orchestrate.py — Quick Pipeline Orchestration Script
Start/stop semua components dengan satu command

Usage:
    python orchestrate.py start          # Start semua components
    python orchestrate.py stop           # Stop semua components
    python orchestrate.py status         # Check status
    python orchestrate.py logs           # Show last logs
"""

import os
import sys
import subprocess
import time
import signal
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent

COMPONENTS = {
    "producer_api": {
        "script": "kafka/producer_api.py",
        "description": "Producer API (Harga Komoditas)",
        "required": True,
    },
    "producer_rss": {
        "script": "kafka/producer_rss.py",
        "description": "Producer RSS (Google News)",
        "required": True,
    },
    "consumer_hdfs": {
        "script": "kafka/consumer_to_hdfs.py",
        "description": "Consumer → HDFS (Long-term Storage)",
        "required": True,
    },
    "consumer_splitter": {
        "script": "kafka/consumer_splitter.py",
        "description": "Consumer Splitter (Live Data)",
        "required": True,
    },
    "dashboard": {
        "script": "dashboard/app.py",
        "description": "Flask Dashboard (http://localhost:5000)",
        "required": False,
    },
}

PROCESSES = {}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def log(msg: str, level: str = "INFO"):
    """Print formatted log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    colors = {
        "INFO": "\033[94m",      # Blue
        "SUCCESS": "\033[92m",   # Green
        "WARNING": "\033[93m",   # Yellow
        "ERROR": "\033[91m",     # Red
        "RESET": "\033[0m",
    }
    color = colors.get(level, colors["INFO"])
    print(f"{color}[{timestamp}] [{level}]{colors['RESET']} {msg}")

def start_component(name: str, config: dict):
    """Start satu component."""
    script_path = BASE_DIR / config["script"]
    
    if not script_path.exists():
        log(f"Script tidak ditemukan: {script_path}", "ERROR")
        return False
    
    try:
        # Determine python executable
        python_exe = sys.executable
        
        # Start process
        process = subprocess.Popen(
            [python_exe, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(BASE_DIR),
        )
        
        PROCESSES[name] = process
        log(f"✓ Started {name} (PID: {process.pid})", "SUCCESS")
        return True
        
    except Exception as e:
        log(f"✗ Failed to start {name}: {e}", "ERROR")
        return False

def stop_component(name: str):
    """Stop satu component."""
    if name not in PROCESSES:
        log(f"Component {name} tidak running", "WARNING")
        return False
    
    process = PROCESSES[name]
    try:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        del PROCESSES[name]
        log(f"✓ Stopped {name}", "SUCCESS")
        return True
    except Exception as e:
        log(f"✗ Failed to stop {name}: {e}", "ERROR")
        return False

def start_all():
    """Start semua components."""
    log("╔════════════════════════════════════════════╗", "INFO")
    log("║  Starting HargaPangan Pipeline             ║", "INFO")
    log("╚════════════════════════════════════════════╝", "INFO")
    
    # Check dependencies
    log("📋 Checking dependencies...", "INFO")
    try:
        import kafka
        import feedparser
        import requests
        log("✓ All Python dependencies installed", "SUCCESS")
    except ImportError as e:
        log(f"✗ Missing dependency: {e}", "ERROR")
        log("  Run: pip install kafka-python feedparser requests", "ERROR")
        return False
    
    # Check Docker containers
    log("🐳 Checking Docker services...", "INFO")
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            log("✗ Docker tidak running", "ERROR")
            return False
        log("✓ Docker is running", "SUCCESS")
    except FileNotFoundError:
        log("✗ Docker not found in PATH", "ERROR")
        return False
    
    # Start components
    log("\n🚀 Starting components...", "INFO")
    started = []
    failed = []
    
    for name, config in COMPONENTS.items():
        log(f"\n► {config['description']}", "INFO")
        if start_component(name, config):
            started.append(name)
        else:
            if config["required"]:
                failed.append(name)
            else:
                log(f"  (Optional component, skipping)", "WARNING")
    
    # Summary
    log("\n╔════════════════════════════════════════════╗", "INFO")
    log("║  Pipeline Status                           ║", "INFO")
    log("╚════════════════════════════════════════════╝", "INFO")
    
    log(f"✓ Started: {len(started)} components", "SUCCESS")
    for name in started:
        log(f"  • {name} (PID: {PROCESSES[name].pid})", "INFO")
    
    if failed:
        log(f"✗ Failed: {len(failed)} required components", "ERROR")
        for name in failed:
            log(f"  • {name}", "ERROR")
        return False
    
    log("\n✅ Pipeline is running!", "SUCCESS")
    log("📊 Dashboard: http://localhost:5000", "INFO")
    log("📝 Logs: Check each terminal", "INFO")
    log("\nPress Ctrl+C to stop all components...\n", "INFO")
    
    return True

def stop_all():
    """Stop semua components."""
    log("Stopping all components...", "INFO")
    
    for name in list(PROCESSES.keys()):
        stop_component(name)
    
    log("✓ All components stopped", "SUCCESS")

def status_all():
    """Show status semua components."""
    log("╔════════════════════════════════════════════╗", "INFO")
    log("║  Pipeline Component Status                 ║", "INFO")
    log("╚════════════════════════════════════════════╝", "INFO")
    
    running = 0
    for name, config in COMPONENTS.items():
        if name in PROCESSES:
            process = PROCESSES[name]
            if process.poll() is None:  # Still running
                log(f"✓ {name:20} RUNNING (PID: {process.pid})", "SUCCESS")
                running += 1
            else:
                log(f"✗ {name:20} CRASHED", "ERROR")
        else:
            log(f"○ {name:20} NOT STARTED", "WARNING")
    
    log(f"\n{running}/{len(COMPONENTS)} components running", "INFO")

def logs_all():
    """Show recent logs from each component."""
    log("Recent component activity:", "INFO")
    
    for name in list(PROCESSES.keys()):
        process = PROCESSES[name]
        if process.poll() is None:
            log(f"\n{name} is still running...", "INFO")
        else:
            stdout, stderr = process.communicate()
            if stdout:
                log(f"\n=== {name} STDOUT ===", "INFO")
                print(stdout[-500:])  # Last 500 chars
            if stderr:
                log(f"\n=== {name} STDERR ===", "ERROR")
                print(stderr[-500:])

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    log("\n⚠️  Received interrupt signal, stopping all components...", "WARNING")
    stop_all()
    sys.exit(0)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    """Main entry point."""
    
    if len(sys.argv) < 2:
        print(f"""
Usage:
    {sys.argv[0]} start     - Start all components
    {sys.argv[0]} stop      - Stop all components
    {sys.argv[0]} status    - Show component status
    {sys.argv[0]} logs      - Show recent logs
""")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    if command == "start":
        if start_all():
            # Keep running
            try:
                while True:
                    time.sleep(1)
                    # Check if any process died
                    for name, process in list(PROCESSES.items()):
                        if process.poll() is not None:
                            log(f"⚠️  Process {name} died, stopping all...", "WARNING")
                            stop_all()
                            sys.exit(1)
            except KeyboardInterrupt:
                log("\nShutting down...", "INFO")
                stop_all()
    
    elif command == "stop":
        stop_all()
    
    elif command == "status":
        status_all()
    
    elif command == "logs":
        logs_all()
    
    else:
        log(f"Unknown command: {command}", "ERROR")
        sys.exit(1)

if __name__ == "__main__":
    main()
