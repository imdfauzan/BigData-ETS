# Tugas Lanjutan ETS: Upgrade Pipeline ke Data Lakehouse

|                       |                                                                         |
| --------------------- | ----------------------------------------------------------------------- |
| **Mata Kuliah**       | Big Data dan Data Lakehouse                                             |
| **Jenis Tugas**       | Tugas Kelompok Lanjutan ETS (Kelompok sama dengan ETS)                  |
| **Pertemuan Terkait** | P12 (Data Lakehouse), lanjutan ETS (Kafka + HDFS + Spark)               |
| **Deadline**          | Dikerjakan Week 12, dipresentasikan Week 13                             |
| **Deliverables**      | Tambahan kode di repo ETS (folder `lakehouse/`) + presentasi 10 menit  |

---

## 🎯 Konteks: Apa yang Sudah Kalian Bangun?

Pada ETS, kelompok kalian membangun pipeline Big Data end-to-end:

```
[API Real-time] → Kafka → Consumer → HDFS (/data/[tema]/api/)
[RSS Feed]      → Kafka → Consumer → HDFS (/data/[tema]/rss/)
                                          ↓
                                    Spark analysis.py
                                    (3 analisis, simpan ke HDFS)
                                          ↓
                                    Flask Dashboard
```

Data sudah tersimpan di HDFS sebagai **file JSON mentah** — tidak ada transaksi ACID, tidak ada versioning, tidak ada schema yang ketat. Jika ada update data, tidak ada cara mudah untuk melacak perubahannya.

---

## 🎯 Apa yang Harus Ditambahkan di Tugas Ini?

Kalian akan **meng-upgrade pipeline ETS** dengan menambahkan lapisan **Data Lakehouse (Medallion Architecture + Delta Lake)** di atas data yang sudah ada di HDFS.

```
SEBELUM (ETS):                           SESUDAH (Tugas Ini):
                                         
[API/RSS] → Kafka → HDFS (JSON) → Spark   [API/RSS] → Kafka → HDFS (JSON)
                                                                     ↓
                                              [BRONZE] Delta Lake (raw)
                                                         ↓
                                              [SILVER] Delta Lake (cleaned)
                                                         ↓
                                               [GOLD] Delta Lake (aggregated)
                                                         ↓
                                               Dashboard (baca dari Gold)
```

**Perbedaan utama yang harus kalian rasakan:**
- Sebelumnya: Spark membaca JSON mentah dari HDFS — tidak ada schema enforcement, tidak ada versioning
- Sesudah: Data tersimpan di Delta Lake dengan ACID, bisa di-query ulang di versi mana pun

---

## 📋 Yang Harus Dibangun

Tambahkan folder `lakehouse/` di dalam repository ETS kalian dengan struktur berikut:

```
[repo-ets-kalian]/
├── ... (kode ETS yang lama, jangan diubah)
└── lakehouse/
    ├── README_lakehouse.md     ← Dokumentasi khusus tugas ini
    ├── 00_setup.md             ← Cara menjalankan Spark + Delta Lake
    ├── 01_bronze.py            ← Ingest dari HDFS ke Bronze Delta layer
    ├── 02_silver.py            ← Cleaning → Silver Delta layer
    └── 03_gold.py              ← Aggregasi → Gold Delta layer
```

---

## 🔧 Spesifikasi Teknis (3 Script Utama)

### Script 1: `01_bronze.py` — Ingest dari HDFS ke Bronze Layer

**Sumber data**: File JSON yang sudah ada di HDFS dari consumer ETS kalian:
- `hdfs://namenode:8020/data/[tema]/api/`
- `hdfs://namenode:8020/data/[tema]/rss/`

**Yang harus dilakukan:**
1. Baca semua file JSON dari HDFS menggunakan PySpark (`spark.read.json(...)`)
2. Tambahkan kolom metadata: `_ingested_at` (waktu ingest sekarang) dan `_source` (nama sumber, misal `"api"` atau `"rss"`)
3. Simpan ke format **Delta Lake** (bukan parquet biasa)

```python
# Hint: Cara inisialisasi SparkSession dengan Delta Lake + HDFS
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, lit

builder = SparkSession.builder.appName("Bronze-[Tema]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")

spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()

# Baca dari HDFS (path sesuai tema ETS kalian)
api_df = spark.read.option("multiLine", True).json("hdfs://namenode:8020/data/[tema]/api/")

# Tambahkan metadata
bronze_df = api_df.withColumn("_ingested_at", current_timestamp()) \
                  .withColumn("_source", lit("api"))

# Simpan ke Delta Lake (Bronze) — bisa di lokal atau HDFS
bronze_df.write.format("delta").mode("append").save("./lakehouse_data/bronze/[tema]_api")
```

> 📌 **Catatan**: Jika Hadoop tidak aktif, boleh menggunakan file JSON lokal yang sudah didownload dari HDFS sebagai alternatif. Dokumentasikan pilihan ini.

---

### Script 2: `02_silver.py` — Cleaning ke Silver Layer

**Sumber data**: Bronze Delta layer yang dibuat di script 1

**Yang harus dilakukan:**
1. Baca dari Bronze Delta layer
2. Lakukan **minimal 3 transformasi cleaning** yang relevan dengan domain ETS kalian:

| Jenis Transformasi | Contoh untuk Topik Berbeda |
|--------------------|---------------------------|
| Hapus duplikat | `dropDuplicates(["id_unik_data"])` |
| Cast tipe data | String timestamp → `to_timestamp()` |
| Handle null | `fillna()` atau `filter(col(...).isNotNull())` |
| Filter data invalid | Harga < 0, magnitude < 0, AQI < 0 |
| Ekstrak kolom | Ambil jam dari timestamp (`hour(col("timestamp"))`) |
| Standarisasi nilai | Uppercase nama kota, dsb. |

3. Simpan ke Silver Delta layer

```python
# Contoh cleaning (sesuaikan dengan skema domain kalian)
from pyspark.sql.functions import col, to_timestamp, hour

bronze_df = spark.read.format("delta").load("./lakehouse_data/bronze/[tema]_api")

silver_df = bronze_df \
    .dropDuplicates(["[kolom_id_unik]"]) \
    .filter(col("[kolom_nilai_utama]").isNotNull()) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("jam", hour(col("timestamp")))

silver_df.write.format("delta").mode("overwrite").save("./lakehouse_data/silver/[tema]")
```

> 💡 **Justifikasi Transformasi**: Di file `README_lakehouse.md`, jelaskan MENGAPA setiap transformasi dilakukan. Berapa baris data yang hilang setelah cleaning? Apa artinya?

---

### Script 3: `03_gold.py` — Agregasi & Enhanced Analysis di Gold Layer

**Sumber data**: Silver Delta layer (API + RSS)

**Mengapa Medallion Memungkinkan Analisis Lebih Baik?**

Di ETS, Spark membaca JSON mentah dari HDFS — tipe data belum tentu benar, ada duplikat, timestamp belum di-parse. Akibatnya analisis terbatas pada agregasi dasar.

Dengan Silver yang sudah bersih:
- Timestamp sudah bertipe `TimestampType` → bisa pakai **Window Functions** (lag, rolling avg)
- Tidak ada duplikat → agregasi akurat
- Kolom `jam` sudah diekstrak → analisis temporal lebih mudah
- Data API + RSS bisa di-**join** → analisis lintas sumber

```
  Silver (API)  ──┐
                  ├── JOIN di Gold → Insight lintas sumber
  Silver (RSS)  ──┘
```

**Yang harus dilakukan:**
Buat **minimal 3 tabel Gold** — 2 tabel mereproduksi analisis ETS yang lama, dan **1 tabel Enhanced** yang tidak bisa dibuat di ETS.

---

### 🗂️ Panduan Gold Enhanced untuk Topik Harga Pangan

| Tabel Gold | ETS? | Enhancement |
|------------|------|-------------|
| `gold/pangan_volatility` | ✅ Repro | Indeks volatilitas per komoditas |
| `gold/pangan_trend` | ✅ Repro | Rata-rata harga per periode |
| `gold/pangan_alert` | 🆕 **Enhanced** | Early warning: komoditas dengan kenaikan harga > 5% dalam 3 observasi terakhir |
| `gold/pangan_news_correlation` | 🆕 **Enhanced** | Join RSS: komoditas yang sering disebut berita vs fluktuasi harga aktual |

```python
from pyspark.sql import Window
from pyspark.sql.functions import lag, col, when

silver = spark.read.format("delta").load("./lakehouse_data/silver/pangan")

# Early warning: deteksi kenaikan harga > 5% dari harga sebelumnya
window_spec = Window.partitionBy("komoditas").orderBy("timestamp")

gold_alert = silver \
    .withColumn("prev_harga", lag("harga", 1).over(window_spec)) \
    .withColumn("pct_change", (col("harga") - col("prev_harga")) / col("prev_harga") * 100) \
    .withColumn("alert", when(col("pct_change") > 5, "⚠️ NAIK SIGNIFIKAN")
                          .when(col("pct_change") < -5, "📉 TURUN SIGNIFIKAN")
                          .otherwise("Normal")) \
    .filter(col("alert") != "Normal") \
    .select("komoditas", "harga", "prev_harga", "pct_change", "alert", "timestamp")

gold_alert.write.format("delta").mode("overwrite").save("./lakehouse_data/gold/pangan_alert")
print(f"{gold_alert.count()} kejadian fluktuasi harga signifikan terdeteksi")
gold_alert.show()
```

---

## 🔍 Demonstrasi Time Travel (Wajib Ada)

Setelah membuat Silver layer, tunjukkan kemampuan **Time Travel Delta Lake**:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

silver_path = "./lakehouse_data/silver/[tema]"
deltaTable = DeltaTable.forPath(spark, silver_path)

# 1. Lihat history tabel
print("=== History Tabel Silver ===")
deltaTable.history().select("version", "timestamp", "operation").show()

# 2. Lakukan sebuah perubahan (misalnya: update salah satu nilai)
# Contoh: update semua null jadi nilai default
deltaTable.update(
    condition="[kolom_tertentu] IS NULL",
    set={"[kolom_tertentu]": lit("[nilai_default]")}
)

# 3. Bandingkan data sebelum dan sesudah update
print("=== Data SEKARANG ===")
spark.read.format("delta").load(silver_path).groupBy("[kolom_tertentu]").count().show()

print("=== Data VERSI 0 (sebelum update) ===")
spark.read.format("delta").option("versionAsOf", 0).load(silver_path) \
    .groupBy("[kolom_tertentu]").count().show()
```

---

## 📝 Deliverables

### 1. Kode (Folder `lakehouse/` di repo ETS)
- [ ] `01_bronze.py` berjalan tanpa error
- [ ] `02_silver.py` berjalan, ada minimal 3 transformasi terdokumentasi
- [ ] `03_gold.py` berjalan, minimal 2 tabel Gold tersimpan di Delta format
- [ ] Demonstrasi Time Travel ada dan berjalan

### 2. `README_lakehouse.md`
Wajib berisi:
- Diagram arsitektur baru (sebelum vs sesudah ada Lakehouse)
- Penjelasan setiap transformasi di Silver (mengapa transformasi itu penting?)
- Perbandingan analisis Gold vs analisis Spark ETS yang lama (apa yang lebih baik?)
- Screenshot: output tabel Delta, hasil Time Travel
- Refleksi: **Apa keuntungan nyata Delta Lake dibanding menyimpan langsung di HDFS/CSV seperti sebelumnya?**

### 3. Presentasi Week 13 (10 menit per kelompok)
| Durasi | Konten |
|--------|--------|
| 2 menit | Demo arsitektur ETS lama vs baru (slide/diagram) |
| 3 menit | Live demo: jalankan pipeline Bronze → Silver → Gold |
| 2 menit | Demo Time Travel — tunjukkan data versi lama |
| 3 menit | Insight dari Gold layer + tanya jawab |

---

## ⚖️ Rubrik Penilaian

**Total: 100 poin**

| Komponen | Skor Maks | Kriteria |
|----------|-----------|---------|
| **Bronze Layer** | 15 | Data dari HDFS berhasil diingest ke Delta format; metadata `_ingested_at` & `_source` ada; API + RSS keduanya masuk |
| **Silver Layer** | 25 | Minimal 3 transformasi cleaning relevan, terdokumentasi, jumlah baris berkurang dengan alasan valid |
| **Gold — Reproduksi ETS** | 20 | Minimal 2 tabel Gold yang mereproduksi analisis Spark ETS sebelumnya |
| **Gold — Enhanced Analysis** | 20 | Minimal 1 tabel Gold **Enhanced** (pakai Window Function, cross-source join, atau derived metric) yang tidak ada di ETS |
| **Time Travel** | 10 | Demonstrasi perubahan data (update/delete) dan query versi lama berhasil, output perbandingan ditampilkan |
| **README & Refleksi** | 10 | Diagram arsitektur sebelum/sesudah, justifikasi transformasi Silver, perbandingan hasil Gold vs ETS |

**Bonus (+10 poin):**
- `+5`: Dashboard Flask diupdate membaca langsung dari tabel Gold Delta (bukan `spark_results.json`)
- `+3`: Cross-source join (gabungkan Silver API + Silver RSS) menghasilkan insight baru di Gold
- `+2`: Schema Evolution — tunjukkan penambahan kolom baru ke Silver menggunakan `mergeSchema`

---

## ❓ FAQ

**Q: Data HDFS dari ETS sudah tidak ada / container sudah dihapus?**
> Jalankan ulang `producer_api.py` dan `consumer_to_hdfs.py` selama ~10 menit untuk mengumpulkan data baru. Minimal 100 record sudah cukup untuk demonstrasi.

**Q: Boleh menggunakan file JSON lokal (bukan dari HDFS)?**
> Boleh, sebagai alternatif jika HDFS sulit diaktifkan. Catat keterbatasan ini di README. Nilai Bronze layer maks 15 poin.

**Q: Apakah kode ETS yang lama harus diubah?**
> **Tidak.** Tugas ini bersifat **additive** — hanya menambahkan folder `lakehouse/` baru. Kode ETS yang lama tidak boleh dimodifikasi agar perbandingan "sebelum vs sesudah" tetap jelas.

**Q: Delta Lake disimpan di mana — HDFS atau lokal?**
> Untuk kemudahan, simpan di folder lokal (`./lakehouse_data/`) di dalam container Spark. Jika mau disimpan ke HDFS sebagai bonus, nilai tidak ditambah tapi sangat diapresiasi.
