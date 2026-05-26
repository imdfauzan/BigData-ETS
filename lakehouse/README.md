# 📊 Data Lakehouse — Medallion Architecture Implementation

**HargaPangan Topic (Kelompok 6) — Big Data ETS Extended Assignment**

Dokumentasi lengkap untuk implementasi Data Lakehouse dengan Bronze → Silver → Gold pipeline menggunakan **Delta Lake** dan **Apache Spark**.

---

## 🏗️ Arsitektur Medallion (3 Layers)

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  🥉 BRONZE LAYER (01_bronze.py)                        │
│  └─ Raw data ingestion dari HDFS/local JSON            │
│     Input:  ./dashboard/data/live_api.json (fallback)  │
│              hdfs://localhost:9000/data/pangan/         │
│     Output: ./lakehouse_data/bronze/pangan_api         │
│             ./lakehouse_data/bronze/pangan_rss         │
│                                                         │
│  ⬇️  ADD METADATA (_ingested_at, _source)              │
│                                                         │
│  🥈 SILVER LAYER (02_silver.py)                        │
│  └─ Data cleaning & transformation                      │
│     Input:  ./lakehouse_data/bronze/                   │
│     Process: ✓ Deduplication                           │
│              ✓ Null handling                           │
│              ✓ Type casting (timestamp)                │
│              ✓ Data validation (filtering)             │
│              ✓ Feature engineering (jam, hari_minggu)  │
│     Output: ./lakehouse_data/silver/pangan_api         │
│             ./lakehouse_data/silver/pangan_rss         │
│                                                         │
│  ⬇️  AGGREGATION & ANALYTICS                            │
│                                                         │
│  🥇 GOLD LAYER (03_gold.py)                            │
│  └─ Business-ready aggregations & insights             │
│     Input:  ./lakehouse_data/silver/                   │
│     Outputs:                                           │
│       ✓ pangan_volatility     (Volatility index)       │
│       ✓ pangan_trend          (Price trends)           │
│       ✓ pangan_alert          (Alert events)           │
│       ✓ pangan_news_correlation (News vs Price)        │
│                                                         │
│  ⬇️  EXPORT TO DASHBOARD                                │
│     Output: ./dashboard/data/spark_results.json        │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start — Complete Pipeline

### 1️⃣ **Automated (RUN_ALL.sh)**

```bash
cd /home/riverz/College/bigdata/BigData-ETS

# Start semua (ETS + Lakehouse)
./RUN_ALL.sh start

# Run full lakehouse pipeline (Bronze → Silver → Gold)
./RUN_ALL.sh lakehouse

# View status
./RUN_ALL.sh status
```

### 2️⃣ **Manual — Step by Step**

```bash
# Activate environment
source venv/bin/activate

# Pastikan ETS infrastructure running (Kafka, HDFS, Spark)
./RUN_ALL.sh start

# Tunggu 5 menit untuk data flow

# === LAKEHOUSE PIPELINE ===

# Step 1: BRONZE LAYER (Ingest raw data)
python lakehouse/01_bronze.py
# Output: ./lakehouse_data/bronze/{pangan_api,pangan_rss}

# Step 2: SILVER LAYER (Clean & transform)
python lakehouse/02_silver.py
# Output: ./lakehouse_data/silver/{pangan_api,pangan_rss}

# Step 3: GOLD LAYER (Aggregate & analyze)
python lakehouse/03_gold.py
# Output: ./lakehouse_data/gold/{pangan_volatility,pangan_trend,pangan_alert,pangan_news_correlation}
#         ./dashboard/data/spark_results.json

# View results
cat ./dashboard/data/spark_results.json | jq '.'
```

---

## 📋 Detailed Layer Documentation

### 🥉 **BRONZE Layer (01_bronze.py)**
- **Purpose**: Raw data ingestion with metadata tagging
- **Input Sources**: 
  - HDFS: `hdfs://localhost:9000/data/pangan/api/` & `/data/pangan/rss/`
  - Local Fallback: `./dashboard/data/live_api.json` & `live_rss.json`
- **Schema Inference**: Explicit schema defined (avoids inference errors)
- **Processing**: Add `_ingested_at` & `_source` columns
- **Output Format**: Delta Lake (ACID-compliant)
- **Output Path**: `./lakehouse_data/bronze/{pangan_api,pangan_rss}`

**Key Features:**
- ✅ Automatic fallback from HDFS → Local files
- ✅ Explicit schema to prevent inference errors
- ✅ Metadata tracking (_ingested_at timestamp, _source)
- ✅ Append mode for incremental data ingestion

---

### 🥈 **SILVER Layer (02_silver.py)**
- **Purpose**: Data cleaning, validation, and transformation
- **Input**: Delta tables dari Bronze layer
- **Data Cleaning (Minimal 3 transformations)**:
  1. **Deduplication**: `dropDuplicates(["id_unik"])` 
  2. **Type Casting**: `to_timestamp()` untuk timestamp columns
  3. **Null Handling**: Filter invalid harga (< 100 or > 1,000,000)
  4. **Feature Engineering**: Extract jam dari timestamp, day-of-week
  5. **Validation**: Filter only komoditas dalam KOMODITAS_VALID list

- **Output Format**: Parquet (for Silver standard)
- **Output Path**: `./lakehouse_data/silver/{pangan_api,pangan_rss}`

**Data Quality Metrics Tracked:**
- Records sebelum cleaning: `X`
- Records setelah cleaning: `Y`
- % Data loss: `(X-Y)/X * 100`
- Reason: Duplikat, null harga, invalid komoditas

---

### 🥇 **GOLD Layer (03_gold.py)**
- **Purpose**: Business-ready aggregations and enhanced analytics
- **Input**: Parquet tables dari Silver layer
- **Output Format**: Delta Lake (for Time Travel support)

**4 Gold Tables:**

#### Table 1️⃣: `pangan_volatility` (Repro ETS Analysis 1)
- **Metric**: Volatility Index = (stddev_harga / mean_harga) * 100
- **Grouping**: Per komoditas
- **Status**: CRITICAL (>20%), WARNING (10-20%), NORMAL (<10%)
- **Use Case**: Bulog intervensi trigger

#### Table 2️⃣: `pangan_trend` (Repro ETS Analysis 2)
- **Metric**: Avg harga per tanggal & jam
- **Grouping**: komoditas, tanggal, jam, hari_minggu
- **Use Case**: Time-series trend analysis

#### Table 3️⃣: `pangan_alert` [ENHANCED]
- **Technique**: Window Function (lag) untuk detect price spikes
- **Alert Trigger**: |pct_change| > 5% dari observasi sebelumnya
- **Status**: ⚠️ NAIK SIGNIFIKAN, 📉 TURUN SIGNIFIKAN
- **Use Case**: Real-time early warning system

#### Table 4️⃣: `pangan_news_correlation` [ENHANCED]
- **Join**: Silver RSS (berita count) ⨝ Silver API (volatility)
- **Categories**: 
  - HIGH_COVERAGE + HIGH_VOLATILITY → WASPADA
  - HIGH_COVERAGE + LOW_VOLATILITY → STABIL
  - LOW_COVERAGE + HIGH_VOLATILITY → ANOMALI HARGA
  - LOW_COVERAGE + LOW_VOLATILITY → NORMAL
- **Use Case**: Assess media impact on price movements

**Bonus Feature: Time Travel Demo**
- Update pangan_volatility (mark CRITICAL items)
- Query historical versions menggunakan `versionAsOf(0)`
- Demonstrate Delta Lake ACID guarantees

---

## 🔧 Setup & Environment

See [**00_setup.md**](./00_setup.md) for detailed setup instructions.

**Requirements:**
- Python 3.9+
- PySpark 3.3+
- Delta-Spark 3.1.0+
- Java 11+

---

## 📊 Expected Output Structure

```
lakehouse_data/
├── bronze/
│   ├── pangan_api/
│   │   ├── _delta_log/
│   │   └── part-*.parquet
│   └── pangan_rss/
│       ├── _delta_log/
│       └── part-*.parquet
│
├── silver/
│   ├── pangan_api/
│   │   └── part-*.parquet
│   └── pangan_rss/
│       └── part-*.parquet
│
└── gold/
    ├── pangan_volatility/
    │   ├── _delta_log/
    │   └── part-*.parquet
    ├── pangan_trend/
    │   ├── _delta_log/
    │   └── part-*.parquet
    ├── pangan_alert/
    │   ├── _delta_log/
    │   └── part-*.parquet
    └── pangan_news_correlation/
        ├── _delta_log/
        └── part-*.parquet

dashboard/data/
└── spark_results.json  ← Flask dashboard reads from here
```

---

## 📝 Logs & Debugging

All pipeline logs stored in `./logs/`:

```bash
# View Bronze ingestion log
tail -f ./logs/lakehouse_bronze.log

# View Silver transformation log
tail -f ./logs/lakehouse_silver.log

# View Gold aggregation log
tail -f ./logs/lakehouse_gold.log
```

---

## ✅ Validation Checklist

After running complete pipeline:

```bash
# 1. Check all Delta tables exist
ls -la ./lakehouse_data/bronze/ ./lakehouse_data/silver/ ./lakehouse_data/gold/

# 2. Verify record counts
python3 << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("validate").getOrCreate()
layers = ["bronze", "silver", "gold"]
for layer in layers:
    df = spark.read.format("delta" if layer != "silver" else "parquet").load(f"./lakehouse_data/{layer}/pangan_api")
    print(f"{layer}/pangan_api: {df.count()} records")
spark.stop()
EOF

# 3. Verify dashboard JSON
cat ./dashboard/data/spark_results.json | jq '.volatility | length'
# Expected: 8 komoditas

# 4. Test Time Travel (Gold layer only)
python3 << 'EOF'
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
spark = SparkSession.builder.appName("test").getOrCreate()
dt = DeltaTable.forPath(spark, "./lakehouse_data/gold/pangan_volatility")
print("Time Travel History:")
dt.history().select("version", "timestamp").show()
spark.stop()
EOF
```

---

## 🎯 Next Steps

1. ✅ Run complete pipeline: `./RUN_ALL.sh lakehouse`
2. ✅ View logs: `tail -f ./logs/lakehouse_gold.log`
3. ✅ Test Gold tables: `python3 << 'EOF' ... EOF`
4. ✅ Update Flask dashboard to read from Gold (optional bonus)
5. ✅ Prepare presentation (Week 13)

---

## 📚 References

- [Medallion Architecture — Databricks](https://www.databricks.com/blog/2022/06/24/data-lakehouse-101-architecture.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)