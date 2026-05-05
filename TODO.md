# TODO - Data Pipeline Fix

## Step 1: Fix Consumer (consumer_to_hdfs.py)
- [x] Tambahkan proper consumer group_id
- [x] Implementasi buffering: kumpulkan pesan 2-3 menit, flush sebagai batch
- [x] Simpan salinan lokal live_api.json dan live_rss.json untuk dashboard
- [x] Gunakan subprocess.run alih-alih os.system
- [x] Threading untuk baca kedua topic paralel

## Step 2: Fix Dashboard HTML (templates/index.html)
- [x] Perbaiki semua tag HTML yang tidak ditutup (missing </div>)
- [x] Pisahkan JavaScript ke file static/dashboard.js untuk menghindari truncation
- [x] Tambahkan Panel 2: Data Live API
- [x] Tambahkan Panel 3: Berita/Feed RSS Terbaru
- [x] Pastikan auto-refresh setInterval bekerja

## Step 3: Fix Dashboard Backend (app.py)
- [x] Tambahkan pembacaan live_api.json dan live_rss.json
- [x] Tambahkan endpoint /api/live untuk data real-time terpisah

## Step 4: Testing & Verification
- [x] Python syntax check berhasil (app.py + consumer_to_hdfs.py)
- [x] Struktur HTML valid dengan semua tag ditutup
- [x] Placeholder live_api.json dan live_rss.json dibuat
- [x] Pipeline end-to-end siap dijalankan

## Status: COMPLETED

## Cara Menjalankan Pipeline:

1. **Jalankan Docker Infrastruktur:**
   ```bash
   docker network create bigdata-network
   docker-compose -f docker-compose-hadoop.yml up -d
   docker-compose -f docker-compose-kafka.yml up -d
   ```

2. **Buat folder HDFS:**
   ```bash
   docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/api
   docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/rss
   docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/hasil
   docker exec -it namenode hdfs dfs -chmod -R 777 /data/pangan
   ```

3. **Jalankan Producer API:**
   ```bash
   python kafka/producer_api.py
   ```

4. **Jalankan Producer RSS:**
   ```bash
   python kafka/producer_rss.py
   ```

5. **Jalankan Consumer ke HDFS:**
   ```bash
   python kafka/consumer_to_hdfs.py
   ```

6. **Jalankan Spark Analysis:**
   ```bash
   python spark/analysis.py
   ```

7. **Jalankan Dashboard:**
   ```bash
   python dashboard/app.py
   ```
   Buka browser ke http://localhost:5000

## Perubahan Penting yang Dilakukan:

### consumer_to_hdfs.py
- **Sebelum:** Simpan setiap pesan satu per satu ke HDFS (tidak efisien, ribuan file)
- **Sesudah:** Buffer pesan selama 2 menit, flush sebagai 1 file batch ke HDFS
- **Sebelum:** Tidak ada salinan lokal untuk dashboard
- **Sesudah:** Menyimpan live_api.json dan live_rss.json di dashboard/data/
- **Sebelum:** group_id=None
- **Sesudah:** group_id="pangan-consumer-hdfs"

### dashboard/templates/index.html
- **Sebelum:** Banyak tag </div> hilang, JavaScript terputus di tengah
- **Sesudah:** Struktur HTML valid, JavaScript dipisah ke static/dashboard.js
- **Sebelum:** Hanya 1 panel (Spark results)
- **Sesudah:** 3 panel: Spark Results + Live API + Live RSS

### dashboard/app.py
- **Sebelum:** Hanya endpoint /api/data untuk spark_results.json
- **Sesudah:** Tambah endpoint /api/live untuk live_api.json dan live_rss.json

