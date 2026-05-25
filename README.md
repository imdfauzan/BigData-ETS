# 🌾 HargaPangan: Big Data Pipeline Monitoring System

**Kelompok 6 - Big Data ETS (End-to-End Test)**

---

## 📋 Profil Tim & Kontribusi

| Nama | Peran | Kontribusi |
|------|-------|-----------|
| **Imam Mahmud Dalil Fauzan** | Infrastructure & Docker | Setup Hadoop + Kafka, HDFS initialization, troubleshooting |
| **Kanafira Vanesha Putri** | Producer API Real-time | producer_api.py, API fallback logic, data validation |
| **Adiwidya Budi Pratama** | Producer RSS & Consumer | producer_rss.py, consumer_to_hdfs.py, HDFS upload |
| **Theodorus Aaron Ugraha** | Spark Analysis | spark/analysis.py, 3 analyses, interpretations, HDFS reading |
| **Oscaryavat Viryavan** | Flask Dashboard | app.py, templates/index.html, Chart.js visualization |

---

## ⚡ Quick Start (Automated)

Gunakan script master untuk menjalankan seluruh sistem dengan satu command:

```bash
# Navigate ke project directory
cd /path/to/BigData-ETS

# Start seluruh sistem (Docker + 5 komponen)
./RUN_ALL.sh start

# Cek status semua komponen
./RUN_ALL.sh status

# Buka dashboard di browser
open http://localhost:5000

# Watch live data flow untuk 60 detik
./RUN_ALL.sh demo

# View logs (api|rss|consumer|spark|dashboard|all)
./RUN_ALL.sh logs all

# Stop seluruh sistem
./RUN_ALL.sh stop

# Lihat dokumentasi lengkap
./RUN_ALL.sh help
```

---

## 🔧 Setup Manual (Detailed)

Jika ingin setup komponen satu per satu untuk debugging:

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- Java 11+ (untuk Spark)
- 4GB+ RAM available

### Step 1: Infrastructure Setup

```bash
cd /path/to/BigData-ETS

# Create custom network untuk inter-container communication
docker network create bigdata-network

# Start Hadoop HDFS
docker-compose -f docker-compose-hadoop.yml up -d

# Start Kafka & ZooKeeper
docker-compose -f docker-compose-kafka.yml up -d

sleep 20  # Tunggu containers ready

# Setup HDFS directories
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/{api,rss,hasil}
docker exec -it namenode hdfs dfs -chmod -R 777 /data/pangan

# Verify HDFS
docker exec -it namenode hdfs dfs -ls /data/pangan/
```

### Step 2: Python Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run Components (Open 5 Terminals)

**Terminal 1 - Kafka Producer (API Prices):**
```bash
source venv/bin/activate
python kafka/producer_api.py
# Output: ✓ Sends 8 commodities every 30 minutes
```

**Terminal 2 - Kafka Producer (RSS News):**
```bash
source venv/bin/activate
python kafka/producer_rss.py
# Output: ✓ Collects 200+ articles continuously
```

**Terminal 3 - Consumer (Buffer to HDFS):**
```bash
source venv/bin/activate
python kafka/consumer_to_hdfs.py
# Output: ✓ Buffers 2 min, flushes to JSON files
```

**Terminal 4 - Spark Analysis (Optional - runs periodically):**
```bash
source venv/bin/activate
python spark/analysis.py
# Output: ✓ Generates 3 analyses every X minutes
```

**Terminal 5 - Dashboard:**
```bash
source venv/bin/activate
python dashboard/app.py
# Output: ✓ Starts on http://localhost:5000
```

### Step 4: Verification

```bash
# Check Kafka topics created
docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Check messages in Kafka
docker exec kafka-broker kafka-console-consumer.sh \
  --topic pangan-api --from-beginning --bootstrap-server localhost:9092 | head -5

# Check HDFS files
docker exec namenode hdfs dfs -ls /data/pangan/api/

# Test dashboard API
curl http://localhost:5000/api/health
curl http://localhost:5000/api/data | jq '.status'

# Check dashboard UI
curl http://localhost:5000/ | head -20
```

---

## 🎯 Topik: HargaPangan - Monitor Harga Komoditas Bahan Pokok

### Latar Belakang & Justifikasi

**Masalah Bisnis:** 
Bulog (Badan Pangan Nasional) membutuhkan sistem early warning untuk memantau fluktuasi harga bahan pokok (beras, cabai, minyak goreng, dll.) dan menghubungkannya dengan berita ekonomi terkini.

**Solusi Kami:**
Pipeline Big Data end-to-end yang mengintegrasikan:
- Real-time price monitoring dari API resmi
- Berita ekonomi dari RSS Feeds (Bisnis.com, Kompas, Google News)
- Analisis volatilitas & tren dengan Spark
- Dashboard interaktif untuk decision makers

**Nilai Bisnis:**
- **Early Warning** untuk potensi krisis pangan
- **Data-driven Decisions** berdasarkan tren + liputan media
- **Scalable Architecture** yang bisa ditambah data sources

---

## 🏗️ Arsitektur Sistem

```
┌──────────────────────────────────────────────────────────────────┐
│                    HARGAPANGAN PIPELINE                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  INGESTION LAYER                                                │
│  ┌──────────────────┐        ┌──────────────────┐               │
│  │ API Producer     │        │ RSS Producer     │               │
│  │ (30 min)         │        │ (5 min)          │               │
│  │ Harga real-time  │        │ Berita ekonomi   │               │
│  └────────┬─────────┘        └────────┬─────────┘               │
│           │                          │                         │
│           ▼                          ▼                         │
│  ┌─────────────────────────────────────────────┐               │
│  │    Apache Kafka                             │               │
│  │ pangan-api / pangan-rss                     │               │
│  │ acks=all, idempotence=true                  │               │
│  └─────────────┬───────────────────────────────┘               │
│                │                                              │
│  STORAGE       ▼                                              │
│  LAYER    ┌─────────────────────────────┐                    │
│           │ Consumer → HDFS             │                    │
│           │ Buffer 2 min, flush to JSON │                    │
│           └─────────────┬───────────────┘                    │
│                        ▼                                     │
│         ┌────────────────────────────┐                      │
│         │ Hadoop HDFS                │                      │
│         │ /data/pangan/              │                      │
│         │  ├── api/ (JSON files)     │                      │
│         │  ├── rss/ (JSON files)     │                      │
│         │  └── hasil/ (Spark output) │                      │
│         └──────────┬───────────────┘                       │
│                    │                                        │
│  PROCESSING    ▼                                            │
│  LAYER    ┌──────────────────┐                              │
│           │ Apache Spark     │                              │
│           │ - 3 Analyses     │                              │
│           │ - Interpretasi   │                              │
│           └────────┬─────────┘                              │
│                    ▼                                        │
│       spark_results.json (dashboard/data/)                 │
│                                                            │
│  SERVING    ┌────────────────────────┐                     │
│  LAYER      │ Flask Dashboard        │                     │
│             │ localhost:5000         │                     │
│             │ - Chart.js viz         │                     │
│             │ - Real-time feeds      │                     │
│             │ - Auto-refresh 30s     │                     │
│             └────────────────────────┘                     │
│                                                            │
└──────────────────────────────────────────────────────────────────┘
```

---



## 📊 Data Format

### Price Data (Kafka: pangan-api)
```json
{
  "komoditas": "beras",
  "label": "Beras",
  "harga": 14500.00,
  "satuan": "kg",
  "perubahan_pct": 0.5,
  "tanggal": "2026-05-23",
  "jam": "14:30:00",
  "timestamp_iso": "2026-05-23T14:30:00",
  "sumber": "simulator"
}
```

### News Data (Kafka: pangan-rss)
```json
{
  "title": "Harga Cabai Naik di Pasar",
  "link": "https://bisnis.com/artikel/...",
  "summary": "Fluktuasi harga cabai...",
  "published": "2026-05-23T10:00:00",
  "komoditas": "cabai",
  "timestamp": "2026-05-23T14:30:00"
}
```

---

## 📈 Analysis Outputs

### Analysis 1: Volatilitas Harga
- **Indeks > 20%** = 🔴 CRITICAL (Bulog harus intervensi)
- **Indeks 10-20%** = 🟠 WARNING (Monitor ketat)
- **Indeks < 10%** = 🟢 NORMAL (Stabil)

### Analysis 2: Tren Harga
Timeline harga per komoditas untuk melihat tren naik/turun.

### Analysis 3: Korelasi Berita vs Harga
Hubungan antara media coverage dengan volatilitas harga.

---

## 🔍 Troubleshooting

### Kafka Connection Error
```bash
# Restart Kafka
docker-compose -f docker-compose-kafka.yml restart

# Check logs
docker logs kafka-broker
```

### HDFS Connection Error
```bash
# Verify HDFS is running
docker exec namenode hdfs dfsadmin -report

# Restart Hadoop
docker-compose -f docker-compose-hadoop.yml restart
```

### Dashboard Shows No Data
```bash
# Run Spark analysis
python spark/analysis.py

# Check results file
ls -la dashboard/data/spark_results.json

# Check Flask
curl http://localhost:5000/api/data | jq '.'
```

---

## 📁 Project Structure

```
BigData-ETS/
├── README.md                    # Main documentation
├── RUN_ALL.sh                   # Master control script
├── requirements.txt             # Python dependencies
├── .env                         # Environment configuration
├── .gitignore                   # Git ignore rules
│
├── docker-compose-hadoop.yml    # Hadoop HDFS infrastructure
├── docker-compose-kafka.yml     # Kafka + ZooKeeper infrastructure
├── hadoop.env                   # Hadoop environment variables
│
├── kafka/
│   ├── producer_api.py          # Real-time price producer
│   ├── producer_rss.py          # News RSS producer
│   └── consumer_to_hdfs.py      # Kafka consumer → HDFS buffering
│
├── spark/
│   └── analysis.py              # Spark analytics (3 analyses)
│
├── dashboard/
│   ├── app.py                   # Flask REST API
│   ├── templates/
│   │   └── index.html           # Dashboard UI (Chart.js)
│   ├── static/
│   │   └── (CSS/JS assets)
│   └── data/
│       ├── live_api.json        # Real-time prices (buffered)
│       ├── live_rss.json        # Real-time news (buffered)
│       └── spark_results.json   # Analysis outputs
│
├── assets/
│   └── Soal dan Ketentuan ETS BigData Kelompok 6.md  # Assignment specs
│
└── logs/                        # Application logs (gitignored)
```

---

## 🛠️ Key Improvements (v2)

✅ Environment variables untuk configuration  
✅ Data validation di producer level  
✅ Direct HDFS reading di Spark  
✅ Business narrative di analisis  
✅ Chart.js rendering di dashboard  
✅ Better error handling & logging  
✅ Deduplication di RSS producer  

---

## 📚 News Sources (RSS Feeds)

Sistem menggunakan Google News RSS feeds untuk real-time news collection:

```
1. https://news.google.com/rss/search?q=harga+bahan+pangan&hl=id&gl=ID&ceid=ID:id
2. https://news.google.com/rss/search?q=harga+pangan&hl=id&gl=ID&ceid=ID:id
3. https://news.google.com/rss/search?q=komoditas+pangan&hl=id&gl=ID&ceid=ID:id
```

Features:
- ✅ Automatic deduplication (tracking by article URL hash)
- ✅ Commodity detection dari article title
- ✅ 5-minute polling interval
- ✅ User-Agent header untuk menghindari 403 errors
- ✅ Retry logic (3 attempts dengan 2s backoff)

---

## � Documentation & Help

**Quick Commands:**
```bash
# View full help
./RUN_ALL.sh help

# View specific component logs
./RUN_ALL.sh logs api      # View API producer logs
./RUN_ALL.sh logs spark    # View Spark analysis logs

# View all logs in real-time
tail -f logs/*.log
```

**Troubleshooting:**
- Kafka connection error? → Restart with `./RUN_ALL.sh stop` then `./RUN_ALL.sh start`
- Dashboard not showing data? → Run `python spark/analysis.py` manually
- HDFS issues? → Check `docker logs namenode`

**Architecture Overview:**
```
Real-time Data Sources
  ↓
Apache Kafka (pangan-api, pangan-rss)
  ↓
Kafka Consumer (buffer 2 min)
  ↓
Hadoop HDFS + Local JSON files
  ↓
Apache Spark (3 analyses)
  ↓
Flask API + Dashboard UI
```

---

## 📋 Status

**Current:** Production Ready ✅  
**Updated:** 25 Mei 2026  
**Version:** v2 (Automated with RUN_ALL.sh)  
**Git Status:** Cleaned repository (logs/ dan data/ gitignored)

**Team:** Kelompok 6 - Big Data ETS

