# 00 Setup: Spark & Delta Lake Environment

Panduan setup environment untuk menjalankan arsitektur Data Lakehouse (Bronze, Silver, Gold layer) dengan Delta Lake.

## 1. Install Dependencies
Pastikan virtual environment (`venv`) sudah aktif. Kita perlu menginstall `pyspark` dan `delta-spark` dengan versi yang kompatibel (Spark 3.5.x dan Delta 3.1.0 direkomendasikan).
```bash
# aktifkan venv di Powershell (kalau belum)
.venv\Scripts\Activate
```
```bash
pip install pyspark==3.5.0 delta-spark==3.1.0
```

## 2. Konfigurasi SparkSession dengan Delta Lake
Di setiap script (`01_bronze.py`, `02_silver.py`, `03_gold.py`), inisialisasi `SparkSession` wajib menggunakan konfigurasi berikut agar Delta Lake dan HDFS bisa terbaca:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("DataLakehouse-Pangan") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") # Port HDFS lokal 9000

# Menambahkan package Delta Lake saat runtime Spark
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()

# Atur log level supaya terminal tidak terlalu penuh
spark.sparkContext.setLogLevel("WARN")
```
*(Catatan: pastikan port HDFS sesuai dengan docker kalian, biasanya `hdfs://localhost:9000` atau `hdfs://localhost:8020`)*

## 3. Testing Koneksi HDFS (Sanity Check)
Sebelum anggota lain (Orang B, C, D) mulai kerja, pastikan script testing sederhana ini berjalan tanpa error untuk membuktikan Spark bisa membaca data dari HDFS.

Buat file sementara `test_hdfs.py` lalu jalankan:
```python
# Masukkan konfigurasi SparkSession di atas...

try:
    print("Mencoba membaca data dari HDFS...")
    api_df = spark.read.option("multiLine", True).json("hdfs://localhost:9000/data/pangan/api/")
    
    print(f"✅ Berhasil! Ditemukan {api_df.count()} baris data API.")
    api_df.printSchema()
    
except Exception as e:
    print("❌ Gagal membaca HDFS. Pastikan Hadoop Docker nyala dan data sudah terisi.")
    print(e)
```

## 4. Struktur Folder Baru
Nantinya data Delta Lake akan disimpan di lokal (bukan HDFS) agar lebih mudah, sesuai arahan soal. Struktur foldernya akan terbentuk otomatis saat script dijalankan:
```text
lakehouse_data/
├── bronze/
├── silver/
└── gold/
```

---