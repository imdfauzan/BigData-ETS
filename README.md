# HargaPangan: Big Data Pipeline Monitoring System (Kelompok 6)

## 📋 Profil Tim
* **Imam Mahmud Dalil Fauzan** - (Setup Infrastructure & Docker)
* **Kanafira Vanesha Putri** - (Producer API Real-time)
* **Adiwidya Budi Pratama** - (Producer RSS & Consumer to HDFS)
* **Theodorus Aaron Ugraha** - (Spark Analysis)
* **Oscaryavat Viryavan** - (Flask Dashboard)

## 🏗️ Topik & Justifikasi
Kami memilih topik **HargaPangan** untuk memantau fluktuasi harga komoditas bahan pokok (Beras, Cabai, Minyak Goreng, dll.) secara real-time.

Sistem ini bertujuan memberikan *early warning* bagi pihak terkait (seperti Bulog) dengan mengorelasikan data harga dari API dengan berita ekonomi terkini dari RSS Feed Bisnis.com dan Kompas.

## ⚙️ Arsitektur Sistem
Sistem ini dibangun dengan pipeline end-to-end sebagai berikut:
1.  **Ingestion:** Data diambil via Python Producer dan dikirim ke **Apache Kafka**.
2.  **Storage:** Data dari Kafka dibaca oleh Consumer dan disimpan ke **Hadoop HDFS** dalam format JSON bertanda waktu.
3.  **Processing:** **Apache Spark** membaca data dari HDFS untuk melakukan analisis volatilitas dan tren.
4.  **Serving:** Hasil analisis ditampilkan melalui **Flask Dashboard** yang melakukan auto-refresh.

## 🚀 Cara Menjalankan Sistem

### 1. Persiapan Infrastruktur (Docker)
Jalankan Hadoop dan Kafka menggunakan Docker Compose:
```bash
# Jalankan Hadoop
docker-compose -f docker-compose-hadoop.yml up -d

# Jalankan Kafka
docker-compose -f docker-compose-kafka.yml up -d
```

## 🛠️ Dokumentasi Pengerjaan
### Fauzan - Anggota 1
- Membuat Repo Github `https://github.com/imdfauzan/BigData-ETS`
- Setup environment docker `docker-compose-kafka.yml`, `docker-compose-hadoop.yml`, dan `hadoop.env`
```bash
# setup env kafka dan hadoop
docker network create bigdata-network
docker-compose -f docker-compose-kafka.yml up -d
docker-compose -f docker-compose-hadoop.yml up -d

# buat folder di hdfs
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/api
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/rss
docker exec -it namenode hdfs dfs -mkdir -p /data/pangan/hasil

# beri akses hadoop 
docker exec -it namenode hdfs dfs -chmod -R 777 /data/pangan

# verif hasil, harus muncul folder /api, /rss, /hasil
docker exec -it namenode hdfs dfs -ls -R /data/pangan/
```
![Infrastruktur Docker HDFS (localhost:9870/explorer.html#/data/pangan)](assets/infrastrukurfolder-fauzan.png)