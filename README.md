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

# Launch entire system
./RUN_ALL.sh start

# Check status
./RUN_ALL.sh status

# View live data flow
./RUN_ALL.sh demo

# Stop everything
./RUN_ALL.sh stop

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

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- Java 11+ (untuk Spark)
- 4GB+ RAM

### Step 1: Infrastructure Setup

```bash
cd /path/to/BigData-ETS

# Create network
docker network create bigdata-network

# Start infrastructure
docker-compose -f docker-compose-hadoop.yml up -d
docker-compose -f docker-compose-kafka.yml up -d

sleep 20  # Wait for containers to be ready

# Setup HDFS directories
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/{api,rss,hasil}
docker exec -it namenode hdfs dfs -chmod -R 777 /data/pangan

# Verify
docker exec -it namenode hdfs dfs -ls /data/pangan/
```

### Step 2: Python Setup

```bash
# Create virtual environment
python -m venv venv

# Activate
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run Pipeline (5 Terminals)

**Terminal 1 - Kafka Producer API:**
```bash
source venv/bin/activate
python kafka/producer_api.py
```

**Terminal 2 - Kafka Producer RSS:**
```bash
source venv/bin/activate
python kafka/producer_rss.py
```

**Terminal 3 - Consumer to HDFS:**
```bash
source venv/bin/activate
python kafka/consumer_to_hdfs.py
```

**Terminal 4 - Spark Analysis (run periodically):**
```bash
source venv/bin/activate
python spark/analysis.py
```

**Terminal 5 - Dashboard:**
```bash
source venv/bin/activate
python dashboard/app.py
# Open: http://localhost:5000
```

### Step 4: Verification

```bash
# Check Kafka topics
docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Check data in Kafka
docker exec kafka-broker kafka-console-consumer.sh \
  --topic pangan-api --from-beginning --bootstrap-server localhost:9092 | head -3

# Check HDFS files
docker exec namenode hdfs dfs -ls /data/pangan/api/

# Check Spark results
curl http://localhost:5000/api/data | jq '.analisis_volatilitas'

# Check Dashboard
curl http://localhost:5000/
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
├── ANALISIS_PROJECT.md          # Technical analysis
├── .env                         # Configuration
├── requirements.txt             # Python dependencies
│
├── docker-compose-hadoop.yml    # Hadoop infrastructure
├── docker-compose-kafka.yml     # Kafka infrastructure
│
├── kafka/
│   ├── producer_api.py          # Price producer
│   ├── producer_rss.py          # News producer
│   └── consumer_to_hdfs.py      # Consumer
│
├── spark/
│   └── analysis.py              # Analytics
│
├── dashboard/
│   ├── app.py                   # Flask app
│   ├── templates/index.html     # UI
│   ├── static/dashboard.js      # Charts
│   └── data/                    # Output data
│
└── logs/                        # Application logs
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

## 📚 RSS Feeds Used

- Primary: https://rss.bisnis.com/feed/rss2/ekonomi
- Backup: https://rss.kompas.com/feed/kompas.com/money  
- Tertiary: https://news.google.com/rss/search?q=harga+bahan+pangan

---

## 📞 Support

Untuk detail teknis lengkap, baca **ANALISIS_PROJECT.md**

**Status:** Production Ready ✅  
**Last Updated:** 23 Mei 2026

