## How to run:

```bash
# 1. Aktivasi venv
source venv/bin/activate

# 2. Pastikan ETS pipeline running (tunggu 10 menit)
./RUN_ALL.sh start

# 3. Verifikasi data ada di HDFS
docker exec namenode hdfs dfs -ls /data/pangan/

# 4. Jalankan Bronze ingestion
python lakehouse/01_bronze.py
```