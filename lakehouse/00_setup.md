# ⚙️ Setup Guide — Data Lakehouse Environment

Complete setup instructions for Data Lakehouse pipeline (Bronze → Silver → Gold layers).

---

## 📋 Prerequisites

- **Python 3.9+** 
- **Java 11+** (for Spark)
- **Docker & Docker Compose** (for Kafka, Hadoop, ZooKeeper)
- **4GB+ RAM** available
- **Existing ETS Infrastructure**: API Producer, RSS Producer, Consumer, Spark Analysis, Dashboard

---

## 🔧 Step 1: Environment Setup

### 1a. Activate Virtual Environment

```bash
cd /home/riverz/College/bigdata/BigData-ETS

# Create venv (if not exists)
python3 -m venv venv

# Activate
source venv/bin/activate

# Verify activation
python --version  # Should be 3.9+
```

### 1b. Install Required Packages

```bash
# Install all dependencies (including Delta Lake)
pip install -r requirements.txt

# Verify installations
python -c "import pyspark; print('✓ PySpark installed')"
python -c "import delta; print('✓ Delta Lake installed')"
python -c "import pandas; print('✓ Pandas installed')"
```

**Key Packages for Lakehouse:**
```
pyspark>=3.3.0
delta-spark==3.1.0
kafka-python>=2.0.0
pandas>=1.3.0
```

---

## 🚀 Step 2: Start ETS Infrastructure

**Make sure existing ETS pipeline is running** — Bronze layer reads from HDFS or local fallback.

```bash
# Start all ETS services (Kafka, Hadoop, Spark, Dashboard)
./RUN_ALL.sh start

# Verify services
./RUN_ALL.sh status

# Wait 5-10 minutes untuk data flow ke accumulate
# API Producer & RSS Producer harus running
# Consumer harus buffer & flush ke HDFS/local
```

**What should be running:**
- ✅ Kafka broker (pangan-api, pangan-rss topics)
- ✅ Hadoop namenode + datanode
- ✅ API Producer (sends price data every 30 min)
- ✅ RSS Producer (scrapes news every 5 min)
- ✅ Consumer (buffers 2 min, flushes to HDFS/local)
- ✅ Dashboard (localhost:5000)

---

## 📂 Step 3: Verify Input Data

### 3a. Check Local Fallback Files

```bash
# Bronze layer akan fallback ke ini jika HDFS kosong
ls -lh ./dashboard/data/live_api.json ./dashboard/data/live_rss.json

# Expected: Files exist dengan > 0 bytes
# Example output:
# -rw-r--r-- 1 riverz riverz 5.2K May 26 16:00 live_api.json
# -rw-r--r-- 1 riverz riverz 3.8K May 26 16:01 live_rss.json

# View sample data
echo "=== API Sample ===" && head -5 ./dashboard/data/live_api.json | jq '.[0]'
echo "=== RSS Sample ===" && head -5 ./dashboard/data/live_rss.json | jq '.[0]'
```

### 3b. (Optional) Check HDFS Data

Jika HDFS available:

```bash
# Verify HDFS directories exist
docker exec namenode hdfs dfs -ls /data/pangan/

# Expected output:
# drwxr-xr-x   - root supergroup       /data/pangan/api/
# drwxr-xr-x   - root supergroup       /data/pangan/rss/
# drwxr-xr-x   - root supergroup       /data/pangan/hasil/

# Check if API files uploaded
docker exec namenode hdfs dfs -ls /data/pangan/api/

# If files exist, count records
docker exec namenode hdfs dfs -cat /data/pangan/api/part-*.json | head -1 | jq 'keys'
```

---

## 🥉 Step 4: Run BRONZE Layer (01_bronze.py)

### 4a. Execute Bronze Ingestion

```bash
# Make sure venv activated
source venv/bin/activate

cd /home/riverz/College/bigdata/BigData-ETS

# Run Bronze layer
python lakehouse/01_bronze.py 2>&1 | tee bronze_run.log

# Watch output for:
# ✓ Spark session berhasil dibuat (Delta Lake enabled)
# ✓ [API] Berhasil baca dari local file: X records
# ✓ [RSS] Berhasil baca dari local file: Y records
# ✓ Bronze API tersimpan: X records
# ✓ Bronze RSS tersimpan: Y records
# ✓ Status: SUCCESS
```

### 4b. Verify Bronze Output

```bash
# Check Bronze folders created
ls -la ./lakehouse_data/bronze/

# Expected:
# drwxr-xr-x pangan_api/
# drwxr-xr-x pangan_rss/

# Count records
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("bronze-verify").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

api_df = spark.read.format("delta").load("./lakehouse_data/bronze/pangan_api")
rss_df = spark.read.format("delta").load("./lakehouse_data/bronze/pangan_rss")

print(f"Bronze API: {api_df.count()} records")
print(f"Bronze RSS: {rss_df.count()} records")
print(f"\nBronze API Schema:")
api_df.printSchema()

spark.stop()
EOF

# Expected output:
# Bronze API: 8 records
# Bronze RSS: X records
# Schema should have: _ingested_at, _source columns
```

---

## 🥈 Step 5: Run SILVER Layer (02_silver.py)

### 5a. Execute Silver Cleaning & Transformation

```bash
# Run Silver layer
python lakehouse/02_silver.py 2>&1 | tee silver_run.log

# Watch output for:
# ✓ Silver pangan_api: X records siap diproses
# ✓ Silver pangan_rss: Y records siap diproses
# ✓ SILVER TABLE 1: pangan_api (Cleaning)
#   ✓ Dropping duplicates...
#   ✓ Casting timestamps...
#   ✓ Filtering invalid harga...
#   ✓ Extracting jam, hari_minggu...
# ✓ Silver pangan_api tersimpan: Z records (perhatian: Z < X jika ada data invalid)
# ✓ Status: SUCCESS
```

### 5b. Verify Silver Output & Data Quality

```bash
# Check Silver folders created
ls -la ./lakehouse_data/silver/

# Expected:
# drwxr-xr-x pangan_api/
# drwxr-xr-x pangan_rss/

# Verify records dan quality metrics
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("silver-verify").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read Silver tables
api_df = spark.read.parquet("./lakehouse_data/silver/pangan_api")
rss_df = spark.read.parquet("./lakehouse_data/silver/pangan_rss")

print("=== SILVER API ===")
print(f"Records: {api_df.count()}")
print(f"Columns: {len(api_df.columns)}")
print(f"Schema:")
api_df.printSchema()

spark.stop()
EOF

# Expected output:
# Records: X (should be <= Bronze count)
# All timestamp columns should be proper format (not strings)
```

---

## 🥇 Step 6: Run GOLD Layer (03_gold.py)

### 6a. Execute Gold Aggregation & Analysis

```bash
# Run Gold layer
python lakehouse/03_gold.py 2>&1 | tee gold_run.log

# Watch output for SUCCESS status
```

### 6b. Verify Gold Output

```bash
# Check Gold folders created
ls -la ./lakehouse_data/gold/

# Expected:
# drwxr-xr-x pangan_volatility/
# drwxr-xr-x pangan_trend/
# drwxr-xr-x pangan_alert/
# drwxr-xr-x pangan_news_correlation/

# Verify all Gold tables dapat dibaca
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("gold-verify").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

gold_tables = [
    ("pangan_volatility", "./lakehouse_data/gold/pangan_volatility"),
    ("pangan_trend", "./lakehouse_data/gold/pangan_trend"),
    ("pangan_alert", "./lakehouse_data/gold/pangan_alert"),
    ("pangan_news_correlation", "./lakehouse_data/gold/pangan_news_correlation"),
]

print("=== GOLD TABLES ===\n")
for name, path in gold_tables:
    try:
        df = spark.read.format("delta").load(path)
        print(f"✓ {name}: {df.count():,} records")
    except Exception as e:
        print(f"✗ {name}: {e}")

spark.stop()
EOF
```

---

## ✅ Complete Pipeline Execution

Once all setup complete, run full pipeline:

```bash
# Automated via RUN_ALL.sh (recommended)
./RUN_ALL.sh lakehouse

# OR manually one-by-one
source venv/bin/activate
python lakehouse/01_bronze.py && \
python lakehouse/02_silver.py && \
python lakehouse/03_gold.py
```

---

## 📚 References

- [Medallion Architecture — Databricks](https://www.databricks.com/blog/2022/06/24/data-lakehouse-101-architecture.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)