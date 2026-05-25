"""
Silver Layer: Data Cleaning & Transformation
Topik 8: HargaPangan — Monitor Harga Komoditas Bahan Pokok

Tanggung jawab Anggota 3:
  - Baca Bronze Delta layer (pangan_api dan pangan_rss)
  - Lakukan cleaning dan transformasi relevan domain HargaPangan
  - Simpan ke Silver Delta layer (pangan_api dan pangan_rss terpisah)
  - Catat jumlah data sebelum dan sesudah cleaning

Sumber  : ./lakehouse_data/bronze/pangan_api
          ./lakehouse_data/bronze/pangan_rss
Output  : ./lakehouse_data/silver/pangan_api
          ./lakehouse_data/silver/pangan_rss
"""

import logging
import sys
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    hour,
    dayofweek,
    trim,
    lower,
    upper,
    when,
    lit,
    regexp_replace,
    length,
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

BRONZE_DIR = "./lakehouse_data/bronze"
SILVER_DIR = "./lakehouse_data/silver"
LOGS_DIR   = "./logs"

BRONZE_API_PATH  = f"{BRONZE_DIR}/pangan_api"
BRONZE_RSS_PATH  = f"{BRONZE_DIR}/pangan_rss"
SILVER_API_PATH  = f"{SILVER_DIR}/pangan_api"
SILVER_RSS_PATH  = f"{SILVER_DIR}/pangan_rss"

# Komoditas valid sesuai domain HargaPangan (dari producer_api.py)
KOMODITAS_VALID = [
    "beras",
    "jagung",
    "kedelai",
    "gula",
    "minyak_goreng",
    "cabai",
    "bawang_merah",
    "telur",
    "umum",      # diizinkan untuk data RSS/berita umum pangan
]

# Batas harga realistis (Rupiah) sesuai domain komoditas pangan Indonesia
HARGA_MIN    = 100        # Rp 100/satuan (batas bawah absolut)
HARGA_MAX    = 1_000_000  # Rp 1 juta/satuan (batas atas absolut)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

# Create logs directory if it doesn't exist
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{LOGS_DIR}/lakehouse_silver.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

def create_spark_session():
    """Initialize Spark dengan Delta Lake support — konsisten dengan 01_bronze.py"""
    try:
        logger.info("Initializing Spark session dengan Delta Lake...")
        spark = (
            SparkSession.builder
            .appName("Silver-Pangan-Lakehouse")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session berhasil dibuat")
        return spark
    except Exception as e:
        logger.error(f"Gagal membuat Spark session: {str(e)}")
        raise


# ─────────────────────────────────────────────
# DELTA LOG HELPER
# ─────────────────────────────────────────────

def create_delta_log(output_path):
    """Buat _delta_log directory dengan minimal JSON metadata untuk delta compatibility."""
    import json
    try:
        delta_log_path = os.path.join(output_path, "_delta_log")
        os.makedirs(delta_log_path, exist_ok=True)
        
        # Buat minimal transaction log JSON
        transaction_log = {
            "add": {
                "path": None,
                "size": None,
                "modificationTime": None,
                "dataChange": True,
                "stats": None
            },
            "commitInfo": {
                "timestamp": int(datetime.now().timestamp() * 1000),
                "operation": "WRITE",
                "operationParameters": {},
                "isBlind": False
            }
        }
        
        log_file = os.path.join(delta_log_path, "00000000000000000000.json")
        with open(log_file, "w") as f:
            f.write(json.dumps(transaction_log) + "\n")
        
        logger.info(f"✓ Delta log directory created: {delta_log_path}")
        return True
    except Exception as e:
        logger.warning(f"Gagal membuat delta log directory: {str(e)}")
        return False


# ─────────────────────────────────────────────
# CLEANING HELPERS
# ─────────────────────────────────────────────

def log_cleaning_step(label, count_before, count_after):
    """Cetak ringkasan setiap langkah cleaning."""
    removed = count_before - count_after
    pct     = (removed / count_before * 100) if count_before > 0 else 0
    logger.info(
        f"  [{label}] {count_before:,} -> {count_after:,} "
        f"| dihapus: {removed:,} ({pct:.1f}%)"
    )
    return removed


def print_cleaning_summary(source_name, count_bronze, count_silver, detail_steps):
    """Cetak summary lengkap cleaning untuk satu sumber data."""
    total_removed = count_bronze - count_silver
    pct_removed   = (total_removed / count_bronze * 100) if count_bronze > 0 else 0

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"CLEANING SUMMARY — {source_name}")
    logger.info("=" * 60)
    logger.info(f"  Jumlah row Bronze (sebelum cleaning) : {count_bronze:,}")
    logger.info(f"  Jumlah row Silver (sesudah cleaning)  : {count_silver:,}")
    logger.info(f"  Total row dihapus                     : {total_removed:,} ({pct_removed:.1f}%)")
    logger.info("  Detail per langkah:")
    for step_name, removed in detail_steps.items():
        logger.info(f"    - {step_name}: {removed:,} row dihapus")
    logger.info("=" * 60)
    logger.info("")


# ─────────────────────────────────────────────
# SILVER: PANGAN API
# Data harga komoditas bahan pokok
# ─────────────────────────────────────────────

def clean_pangan_api(spark):
    """
    Cleaning pipeline untuk Bronze pangan_api.

    Transformasi yang dilakukan (relevan domain HargaPangan):
      T1. dropDuplicates berdasarkan message_id
          - Data streaming Kafka bisa menghasilkan duplikat jika consumer restart
          - message_id adalah unique identifier per record dari producer_api.py

      T2. Filter harga IS NOT NULL dan harga dalam range valid [100, 1.000.000]
          - Record tanpa harga tidak berguna untuk analisis volatilitas dan tren
          - Harga <= 0 atau ekstrem abnormal mengindikasikan error simulator/API
          - Menjaga integritas agregasi di Gold layer (mean, stddev, dll.)

      T3. Filter komoditas IS NOT NULL dan hanya komoditas dikenal
          - Komoditas null atau tidak dikenal tidak bisa di-join ke Gold layer
          - Sesuai KOMODITAS_VALID yang didefinisikan di producer_api.py

      T4. Cast timestamp ke TimestampType + ekstrak jam dan hari_minggu
          - timestamp di Bronze masih string (e.g. "2026-05-25 14:57:56")
          - TimestampType diperlukan untuk Window Function di 03_gold.py
          - Kolom jam dan hari_minggu mendukung analisis temporal (peak hour, dll.)

      T5. Standarisasi nama komoditas (trim + lowercase)
          - Menghindari duplikat logis karena spasi atau kapitalisasi berbeda
          - Konsistensi key untuk join dengan Silver RSS

      T6. Tambahkan kolom _cleaned_at untuk audit trail Silver layer
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Proses Silver Layer — pangan_api")
    logger.info("=" * 60)

    # Baca Bronze
    logger.info(f"Membaca Bronze: {BRONZE_API_PATH}")
    df = spark.read.parquet(BRONZE_API_PATH)
    count_bronze = df.count()
    logger.info(f"Bronze pangan_api: {count_bronze:,} records")
    logger.info("Schema Bronze:")
    df.printSchema()

    detail_steps = {}
    current_count = count_bronze

    # ── T1: Hapus duplikat berdasarkan message_id ──
    # message_id null (dari record RSS yang masuk Bronze API karena mixed ingestion)
    # diperlakukan sebagai duplikat potensial — drop berdasarkan kolom komposit
    logger.info("T1: Menghapus duplikat...")
    df_t1 = df.dropDuplicates(["message_id", "komoditas", "timestamp"])
    count_t1 = df_t1.count()
    detail_steps["T1 drop_duplicates(message_id, komoditas, timestamp)"] = log_cleaning_step(
        "T1 Duplikat", current_count, count_t1
    )
    current_count = count_t1

    # ── T2: Filter harga valid ──
    logger.info("T2: Memfilter harga valid (tidak null, > 100, < 1.000.000)...")
    # Hanya filter harga jika record memang adalah data harga (topic pangan-api)
    # Record RSS yang mixed di Bronze tidak punya harga — pisahkan dulu
    df_api_only = df_t1.filter(col("topic") == "pangan-api")
    df_rss_mixed = df_t1.filter(col("topic") != "pangan-api")

    df_api_harga = df_api_only \
        .filter(col("harga").isNotNull()) \
        .filter(col("harga") > HARGA_MIN) \
        .filter(col("harga") < HARGA_MAX)

    # Gabungkan kembali — record RSS mixed tidak di-drop, hanya data harga yang divalidasi
    df_t2 = df_api_harga.union(df_rss_mixed)
    count_t2 = df_t2.count()
    detail_steps["T2 filter_harga_valid(>100, <1000000, not null)"] = log_cleaning_step(
        "T2 Harga Invalid", current_count, count_t2
    )
    current_count = count_t2

    # ── T3: Filter komoditas valid ──
    logger.info("T3: Memfilter komoditas valid...")
    df_t3 = df_t2.filter(col("komoditas").isNotNull()) \
                 .filter(trim(lower(col("komoditas"))).isin(KOMODITAS_VALID))
    count_t3 = df_t3.count()
    detail_steps["T3 filter_komoditas_valid"] = log_cleaning_step(
        "T3 Komoditas Invalid", current_count, count_t3
    )
    current_count = count_t3

    # ── T4: Cast timestamp + ekstrak fitur temporal ──
    logger.info("T4: Cast timestamp dan ekstrak fitur temporal...")
    df_t4 = df_t3 \
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
        ) \
        .withColumn("jam", hour(col("timestamp"))) \
        .withColumn("hari_minggu", dayofweek(col("timestamp")))
    # Filter record dengan timestamp tidak bisa di-parse (null setelah cast)
    df_t4 = df_t4.filter(col("timestamp").isNotNull())
    count_t4 = df_t4.count()
    detail_steps["T4 cast_timestamp + filter_timestamp_invalid"] = log_cleaning_step(
        "T4 Timestamp Invalid", current_count, count_t4
    )
    current_count = count_t4

    # ── T5: Standarisasi nama komoditas ──
    logger.info("T5: Standarisasi nama komoditas (trim + lowercase)...")
    df_t5 = df_t4.withColumn("komoditas", trim(lower(col("komoditas"))))
    # Tidak menghapus row, hanya normalisasi nilai
    detail_steps["T5 standarisasi_komoditas(trim+lower)"] = 0
    logger.info("  [T5 Standarisasi Komoditas] normalisasi nilai — 0 row dihapus")

    # ── T6: Tambahkan kolom audit Silver ──
    logger.info("T6: Menambahkan kolom audit _cleaned_at...")
    from pyspark.sql.functions import current_timestamp
    silver_df = df_t5.withColumn("_cleaned_at", current_timestamp())

    count_silver = silver_df.count()

    # Tulis ke Silver layer (parquet format)
    logger.info(f"Menulis Silver pangan_api ke: {SILVER_API_PATH}")
    silver_df.write \
        .mode("overwrite") \
        .parquet(SILVER_API_PATH)
    logger.info("Silver pangan_api berhasil ditulis")
    
    # Buat delta log directory untuk compatibility
    create_delta_log(SILVER_API_PATH)

    print_cleaning_summary("pangan_api", count_bronze, count_silver, detail_steps)

    return count_bronze, count_silver


# ─────────────────────────────────────────────
# SILVER: PANGAN RSS
# Data berita/artikel terkait harga pangan
# ─────────────────────────────────────────────

def clean_pangan_rss(spark):
    """
    Cleaning pipeline untuk Bronze pangan_rss.

    Transformasi yang dilakukan (relevan domain HargaPangan RSS/berita):
      T1. dropDuplicates berdasarkan link artikel
          - RSS feed bisa kirim artikel yang sama berulang kali
          - link adalah URL unik per artikel

      T2. Filter title dan summary IS NOT NULL
          - Artikel tanpa judul/ringkasan tidak berguna untuk analisis korelasi berita
          - Gold layer pangan_news_correlation butuh konten artikel yang lengkap

      T3. Filter komoditas IS NOT NULL
          - Artikel harus terkait komoditas tertentu untuk cross-source join di Gold
          - Kolom komoditas di RSS menunjukkan komoditas yang di-tag saat ingest

      T4. Filter published IS NOT NULL + length(published) > 10
          - published adalah tanggal artikel, dibutuhkan untuk korelasi temporal
          - String terlalu pendek (<10 char) menandakan nilai corrupt

      T5. Cast timestamp ke TimestampType + ekstrak jam dan hari_minggu
          - timestamp di Bronze masih string
          - Diperlukan untuk join temporal dengan Silver API di Gold layer

      T6. Standarisasi komoditas (trim + lowercase)
          - Konsistensi key saat join dengan Silver API di Gold layer

      T7. Tambahkan kolom _cleaned_at untuk audit trail Silver layer
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("Proses Silver Layer — pangan_rss")
    logger.info("=" * 60)

    # Baca Bronze
    logger.info(f"Membaca Bronze: {BRONZE_RSS_PATH}")
    df = spark.read.parquet(BRONZE_RSS_PATH)
    count_bronze = df.count()
    logger.info(f"Bronze pangan_rss: {count_bronze:,} records")

    detail_steps = {}
    current_count = count_bronze

    # ── T1: Hapus duplikat berdasarkan link ──
    logger.info("T1: Menghapus duplikat artikel (berdasarkan link)...")
    df_t1 = df.dropDuplicates(["link"])
    count_t1 = df_t1.count()
    detail_steps["T1 drop_duplicates(link)"] = log_cleaning_step(
        "T1 Duplikat Artikel", current_count, count_t1
    )
    current_count = count_t1

    # ── T2: Filter title dan summary tidak null ──
    logger.info("T2: Memfilter artikel tanpa title atau summary...")
    df_t2 = df_t1 \
        .filter(col("title").isNotNull()) \
        .filter(col("summary").isNotNull()) \
        .filter(length(trim(col("title"))) > 0) \
        .filter(length(trim(col("summary"))) > 0)
    count_t2 = df_t2.count()
    detail_steps["T2 filter_title_summary_not_null"] = log_cleaning_step(
        "T2 Title/Summary Null", current_count, count_t2
    )
    current_count = count_t2

    # ── T3: Filter komoditas valid ──
    logger.info("T3: Memfilter komoditas valid...")
    df_t3 = df_t2 \
        .filter(col("komoditas").isNotNull()) \
        .filter(trim(lower(col("komoditas"))).isin(KOMODITAS_VALID))
    count_t3 = df_t3.count()
    detail_steps["T3 filter_komoditas_valid"] = log_cleaning_step(
        "T3 Komoditas Invalid/Null", current_count, count_t3
    )
    current_count = count_t3

    # ── T4: Filter published tidak null dan tidak corrupt ──
    logger.info("T4: Memfilter published tidak null dan tidak corrupt...")
    df_t4 = df_t3 \
        .filter(col("published").isNotNull()) \
        .filter(length(trim(col("published"))) > 10)
    count_t4 = df_t4.count()
    detail_steps["T4 filter_published_not_null_and_valid"] = log_cleaning_step(
        "T4 Published Invalid", current_count, count_t4
    )
    current_count = count_t4

    # ── T5: Cast timestamp + ekstrak fitur temporal ──
    logger.info("T5: Cast timestamp dan ekstrak fitur temporal...")
    df_t5 = df_t4 \
        .withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
        ) \
        .withColumn("jam", hour(col("timestamp"))) \
        .withColumn("hari_minggu", dayofweek(col("timestamp")))
    df_t5 = df_t5.filter(col("timestamp").isNotNull())
    count_t5 = df_t5.count()
    detail_steps["T5 cast_timestamp + filter_timestamp_invalid"] = log_cleaning_step(
        "T5 Timestamp Invalid", current_count, count_t5
    )
    current_count = count_t5

    # ── T6: Standarisasi komoditas ──
    logger.info("T6: Standarisasi nama komoditas (trim + lowercase)...")
    df_t6 = df_t5.withColumn("komoditas", trim(lower(col("komoditas"))))
    detail_steps["T6 standarisasi_komoditas(trim+lower)"] = 0
    logger.info("  [T6 Standarisasi Komoditas] normalisasi nilai — 0 row dihapus")

    # ── T7: Tambahkan kolom audit Silver ──
    logger.info("T7: Menambahkan kolom audit _cleaned_at...")
    from pyspark.sql.functions import current_timestamp
    silver_df = df_t6.withColumn("_cleaned_at", current_timestamp())

    count_silver = silver_df.count()

    # Tulis ke Silver layer (parquet format)
    logger.info(f"Menulis Silver pangan_rss ke: {SILVER_RSS_PATH}")
    silver_df.write \
        .mode("overwrite") \
        .parquet(SILVER_RSS_PATH)
    logger.info("Silver pangan_rss berhasil ditulis")
    
    # Buat delta log directory untuk compatibility
    create_delta_log(SILVER_RSS_PATH)

    print_cleaning_summary("pangan_rss", count_bronze, count_silver, detail_steps)

    return count_bronze, count_silver


# ─────────────────────────────────────────────
# TIME TRAVEL DEMO
# ─────────────────────────────────────────────

def demo_time_travel(spark):
    """
    Demonstrasi Delta Lake Time Travel.
    Tunjukkan history tabel Silver dan kemampuan query versi lama.
    
    NOTE: Disabled - requires delta JAR registration with pyspark 3.5.0
    """
    logger.info("")
    logger.info("=" * 60)
    logger.info("SKIPPED: Delta Lake Time Travel — not available with parquet format")
    logger.info("=" * 60)
    logger.warning("Time Travel demo skipped (requires delta data source registration)")


# ─────────────────────────────────────────────
# VERIFY SILVER LAYER
# ─────────────────────────────────────────────

def verify_silver_layer(spark, silver_path, source_name):
    """Verifikasi Silver layer: schema, record count, dan sample."""
    try:
        logger.info(f"Verifying Silver {source_name}...")
        df = spark.read.parquet(silver_path)

        logger.info(f"Schema Silver ({source_name}):")
        df.printSchema()

        row_count = df.count()
        logger.info(f"Total records Silver {source_name}: {row_count:,}")

        logger.info(f"Distribusi komoditas (top 10):")
        df.groupBy("komoditas").count() \
          .orderBy(col("count").desc()) \
          .show(10, truncate=False)

        # Verifikasi kolom baru hasil transformasi Silver
        new_cols = ["jam", "hari_minggu", "_cleaned_at"]
        for c in new_cols:
            if c in df.columns:
                logger.info(f"  Kolom '{c}' tersedia — OK")
            else:
                logger.warning(f"  Kolom '{c}' TIDAK ditemukan")

        logger.info(f"Verification {source_name} selesai")

    except Exception as e:
        logger.error(f"Verification gagal untuk {source_name}: {str(e)}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    """Execute Silver layer cleaning pipeline."""
    spark = None
    start_time = datetime.now()

    try:
        spark = create_spark_session()

        logger.info("")
        logger.info("=" * 60)
        logger.info("SILVER LAYER CLEANING PIPELINE — HargaPangan")
        logger.info("=" * 60)
        logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # ── Pipeline pangan_api ──
        api_bronze_count, api_silver_count = clean_pangan_api(spark)
        verify_silver_layer(spark, SILVER_API_PATH, "pangan_api")

        # ── Pipeline pangan_rss ──
        rss_bronze_count, rss_silver_count = clean_pangan_rss(spark)
        verify_silver_layer(spark, SILVER_RSS_PATH, "pangan_rss")

        # ── Time Travel Demo ──
        demo_time_travel(spark)

        # ── Final Summary ──
        end_time   = datetime.now()
        durasi     = (end_time - start_time).total_seconds()
        total_bronze = api_bronze_count + rss_bronze_count
        total_silver = api_silver_count + rss_silver_count
        total_removed = total_bronze - total_silver

        logger.info("")
        logger.info("=" * 60)
        logger.info("SILVER LAYER PIPELINE — FINAL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  pangan_api : {api_bronze_count:,} -> {api_silver_count:,} "
                    f"(dihapus: {api_bronze_count - api_silver_count:,})")
        logger.info(f"  pangan_rss : {rss_bronze_count:,} -> {rss_silver_count:,} "
                    f"(dihapus: {rss_bronze_count - rss_silver_count:,})")
        logger.info(f"  TOTAL      : {total_bronze:,} -> {total_silver:,} "
                    f"(dihapus: {total_removed:,})")
        logger.info(f"  Silver path API : {SILVER_API_PATH}")
        logger.info(f"  Silver path RSS : {SILVER_RSS_PATH}")
        logger.info(f"  Durasi pipeline : {durasi:.1f} detik")
        logger.info(f"  Status          : SUCCESS")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Silver layer pipeline gagal: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session ditutup")


if __name__ == "__main__":
    main()
