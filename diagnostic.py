#!/usr/bin/env python3
"""
diagnostic.py — Pipeline Health Check Utility
Verifies all components are properly configured and running

Usage:
    python diagnostic.py
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent

CHECKS = {
    "Python Dependencies": {
        "items": ["kafka", "feedparser", "requests", "pyspark"],
        "type": "import",
    },
    "Project Files": {
        "items": [
            "kafka/producer_rss.py",
            "kafka/consumer_splitter.py",
            "spark/analysis_updated.py",
            "orchestrate.py",
        ],
        "type": "file",
    },
    "Output Directories": {
        "items": [
            "dashboard/data",
        ],
        "type": "directory",
    },
    "Configuration": {
        "items": [
            "PIPELINE_IMPLEMENTATION_GUIDE.md",
            "QUICK_REFERENCE.md",
        ],
        "type": "file",
    },
}

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

def check_import(module_name):
    """Check if Python module is importable."""
    try:
        __import__(module_name)
        return True, f"{module_name} installed"
    except ImportError:
        return False, f"{module_name} NOT INSTALLED"

def check_file(file_path):
    """Check if file exists."""
    full_path = BASE_DIR / file_path
    exists = full_path.exists()
    return exists, str(full_path)

def check_directory(dir_path):
    """Check if directory exists."""
    full_path = BASE_DIR / dir_path
    exists = full_path.is_dir()
    if not exists and full_path.exists():
        return False, f"{str(full_path)} is a file, not a directory"
    return exists, str(full_path)

def check_docker():
    """Check if Docker is running."""
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0, "Docker is accessible"
    except FileNotFoundError:
        return False, "Docker not found in PATH"
    except Exception as e:
        return False, f"Docker check failed: {e}"

def check_kafka_running():
    """Check if Kafka container is running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "-q"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return bool(result.stdout.strip()), "Kafka container running"
    except Exception as e:
        return False, f"Check failed: {e}"

def check_hadoop_running():
    """Check if Hadoop container is running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=namenode", "-q"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return bool(result.stdout.strip()), "Hadoop container running"
    except Exception as e:
        return False, f"Check failed: {e}"

def check_kafka_topics():
    """Check if Kafka topics exist."""
    try:
        result = subprocess.run(
            ["docker", "exec", "-it", "kafka", "kafka-topics", 
             "--list", "--bootstrap-server", "kafka:9092"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        topics = result.stdout.strip().split('\n')
        has_api = "pangan-api" in topics
        has_rss = "pangan-rss" in topics
        
        if has_api and has_rss:
            return True, f"Both topics exist: {topics}"
        else:
            return False, f"Missing topics. Found: {topics}"
    except Exception as e:
        return False, f"Check failed: {e}"

def check_hdfs_paths():
    """Check if HDFS paths exist."""
    try:
        result = subprocess.run(
            ["docker", "exec", "-it", "namenode", "hdfs", "dfs", 
             "-ls", "/data/pangan"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return True, "/data/pangan exists in HDFS"
        else:
            return False, "HDFS path /data/pangan not found"
    except Exception as e:
        return False, f"Check failed: {e}"

def check_dashboard_data():
    """Check if dashboard data directory has files."""
    dashboard_dir = BASE_DIR / "dashboard" / "data"
    if not dashboard_dir.exists():
        return False, "Dashboard data directory doesn't exist"
    
    files = list(dashboard_dir.glob("*.json"))
    if files:
        file_info = [f"{f.name} ({f.stat().st_size} bytes)" for f in files]
        return True, f"Found {len(files)} JSON files: {', '.join(file_info)}"
    else:
        return False, "No JSON files in dashboard/data/"

def check_spark_file():
    """Check if Spark analysis file is present."""
    spark_file = BASE_DIR / "spark" / "analysis_updated.py"
    if spark_file.exists():
        size = spark_file.stat().st_size
        return True, f"analysis_updated.py exists ({size} bytes)"
    return False, "analysis_updated.py not found"

# ─────────────────────────────────────────────────────────────
# MAIN DIAGNOSTIC
# ─────────────────────────────────────────────────────────────

def print_header(title):
    """Print section header."""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BLUE}{title:^60}{Colors.RESET}")
    print(f"{Colors.BLUE}{'='*60}{Colors.RESET}\n")

def print_result(name, passed, message):
    """Print check result."""
    symbol = f"{Colors.GREEN}✓{Colors.RESET}" if passed else f"{Colors.RED}✗{Colors.RESET}"
    status = f"{Colors.GREEN}PASS{Colors.RESET}" if passed else f"{Colors.RED}FAIL{Colors.RESET}"
    print(f"{symbol} {name:40} [{status}]")
    if message:
        print(f"  └─ {message}")

def run_diagnostics():
    """Run all diagnostic checks."""
    
    print_header("🔍 HARGAN PANGAN PIPELINE DIAGNOSTIC")
    
    total_checks = 0
    passed_checks = 0
    
    # Environment Checks
    print_header("1. ENVIRONMENT")
    
    checks_env = [
        ("Python Version", lambda: (sys.version.split()[0], "")),
        ("Working Directory", lambda: (str(BASE_DIR), "")),
        ("Docker Available", check_docker),
    ]
    
    for name, check_func in checks_env:
        total_checks += 1
        try:
            passed, message = check_func()
            print_result(name, passed, message)
            if passed:
                passed_checks += 1
        except Exception as e:
            print_result(name, False, str(e))
    
    # Dependencies
    print_header("2. PYTHON DEPENDENCIES")
    
    for module in CHECKS["Python Dependencies"]["items"]:
        total_checks += 1
        passed, message = check_import(module)
        print_result(module, passed, message)
        if passed:
            passed_checks += 1
    
    # Project Files
    print_header("3. PROJECT FILES")
    
    for file_path in CHECKS["Project Files"]["items"]:
        total_checks += 1
        passed, message = check_file(file_path)
        print_result(file_path, passed, message)
        if passed:
            passed_checks += 1
    
    # Directories
    print_header("4. DIRECTORIES")
    
    for dir_path in CHECKS["Output Directories"]["items"]:
        total_checks += 1
        passed, message = check_directory(dir_path)
        print_result(dir_path, passed, message)
        if passed:
            passed_checks += 1
    
    # Docker Services
    print_header("5. DOCKER SERVICES")
    
    services = [
        ("Kafka Container", check_kafka_running),
        ("Hadoop Container", check_hadoop_running),
    ]
    
    for name, check_func in services:
        total_checks += 1
        try:
            passed, message = check_func()
            print_result(name, passed, message)
            if passed:
                passed_checks += 1
        except Exception as e:
            print_result(name, False, str(e))
    
    # Kafka Configuration
    print_header("6. KAFKA CONFIGURATION")
    
    kafka_checks = [
        ("Kafka Topics", check_kafka_topics),
    ]
    
    for name, check_func in kafka_checks:
        total_checks += 1
        try:
            passed, message = check_func()
            print_result(name, passed, message)
            if passed:
                passed_checks += 1
        except Exception as e:
            print_result(name, False, str(e))
    
    # HDFS Configuration
    print_header("7. HDFS CONFIGURATION")
    
    hdfs_checks = [
        ("HDFS Paths", check_hdfs_paths),
    ]
    
    for name, check_func in hdfs_checks:
        total_checks += 1
        try:
            passed, message = check_func()
            print_result(name, passed, message)
            if passed:
                passed_checks += 1
        except Exception as e:
            print_result(name, False, str(e))
    
    # Data & Outputs
    print_header("8. DATA & OUTPUTS")
    
    data_checks = [
        ("Dashboard Data", check_dashboard_data),
        ("Spark Analysis File", check_spark_file),
    ]
    
    for name, check_func in data_checks:
        total_checks += 1
        try:
            passed, message = check_func()
            print_result(name, passed, message)
            if passed:
                passed_checks += 1
        except Exception as e:
            print_result(name, False, str(e))
    
    # Summary
    print_header("SUMMARY")
    
    pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    print(f"Total Checks: {total_checks}")
    print(f"Passed: {Colors.GREEN}{passed_checks}{Colors.RESET}")
    print(f"Failed: {Colors.RED}{total_checks - passed_checks}{Colors.RESET}")
    print(f"Pass Rate: {pass_rate:.1f}%")
    
    if pass_rate == 100:
        print(f"\n{Colors.GREEN}✓ All checks passed! Pipeline is ready.{Colors.RESET}")
        return 0
    elif pass_rate >= 80:
        print(f"\n{Colors.YELLOW}⚠ Most checks passed. Review failures above.{Colors.RESET}")
        return 1
    else:
        print(f"\n{Colors.RED}✗ Multiple failures. Please fix issues before deploying.{Colors.RESET}")
        return 2

if __name__ == "__main__":
    sys.exit(run_diagnostics())
