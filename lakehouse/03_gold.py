"""
03_gold.py — Gold Layer: Agregasi & Enhanced Analysis
Topik 8: HargaPangan — Monitor Harga Komoditas Bahan Pokok

Tanggung jawab Orang D:
  - Baca Silver Delta layer (pangan_api dan pangan_rss)
  - Produksi 4 tabel Gold:
      1. pangan_volatility     → Repro ETS Analysis 1: indeks volatilitas per komoditas
      2. pangan_trend          → Repro ETS Analysis 2: rata-rata harga per periode waktu
      3. pangan_alert          → Enhanced: early warning Window Function (lag harga)
      4. pangan_news_correlation → Enhanced: cross-source join Silver API + Silver RSS
  - Demonstrasi Time Travel Delta Lake
  - Export spark_results.json untuk Flask Dashboard (bonus)

Sumber  : ./lakehouse_data/silver/pangan_api   (parquet — ditulis oleh 02_silver.py)
          ./lakehouse_data/silver/pangan_rss   (parquet — ditulis oleh 02_silver.py)
Output  : ./lakehouse_data/gold/pangan_volatility     (Delta format)
          ./lakehouse_data/gold/pangan_trend           (Delta format)
          ./lakehouse_data/gold/pangan_alert           (Delta format)
          ./lakehouse_data/gold/pangan_news_correlation (Delta format)
          ./dashboard/data/spark_results.json          (untuk Flask Dashboard)

Catatan konsistensi:
  - SparkSession menggunakan configure_spark_with_delta_pip (sama seperti 01_bronze.py)
    karena Gold menulis Delta format dan butuh Time Travel
  - Silver ditulis dalam parquet biasa; Gold membacanya dengan spark.read.parquet()
  - Port HDFS: localhost:9000 (sesuai docker-compose-hadoop.yml yang mem-forward 9000:9000)
"""

import json
import logging
import os
import sys
from datetime import datetime

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_trunc,
    dayofweek,
    first,
    hour,
    lag,
    last,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    to_date,
    when,
)
from pyspark.sql.types import DoubleType

# ─────────────────────────────────────────────
# KONFIGURASI — selaras dengan 01_bronze.py & 02_silver.py
# ─────────────────────────────────────────────

SILVER_DIR  = "./lakehouse_data/silver"
GOLD_DIR    = "./lakehouse_data/gold"
LOGS_DIR    = "./logs"

# Input: Silver paths (parquet — ditulis oleh 02_silver.py)
SILVER_API_PATH = f"{SILVER_DIR}/pangan_api"
SILVER_RSS_PATH = f"{SILVER_DIR}/pangan_rss"

# Output: Gold paths (Delta format)
GOLD_VOLATILITY_PATH    = f"{GOLD_DIR}/pangan_volatility"
GOLD_TREND_PATH         = f"{GOLD_DIR}/pangan_trend"
GOLD_ALERT_PATH         = f"{GOLD_DIR}/pangan_alert"
GOLD_NEWS_CORR_PATH     = f"{GOLD_DIR}/pangan_news_correlation"

# Dashboard output — Flask membaca dari sini
DASHBOARD_DATA_DIR  = "./dashboard/data"
SPARK_RESULTS_FILE  = f"{DASHBOARD_DATA_DIR}/spark_results.json"

# Threshold untuk klasifikasi volatilitas (konsisten dengan analisis ETS lama)
VOLATILITY_CRITICAL = 20.0   # Indeks > 20% → CRITICAL
VOLATILITY_WARNING  = 10.0   # Indeks 10–20% → WARNING
                              # Indeks < 10%  → NORMAL

# Threshold early warning (Gold Enhanced)
ALERT_PCT_THRESHOLD = 5.0    # Perubahan harga > 5% dari observasi sebelumnya

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(f"{LOGS_DIR}/lakehouse_gold.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SPARK SESSION
# Menggunakan configure_spark_with_delta_pip agar Gold bisa menulis
# Delta format dan menjalankan Time Travel — konsisten dengan 01_bronze.py
# ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Inisialisasi SparkSession dengan Delta Lake support.
    Sama seperti 01_bronze.py — diperlukan untuk write Delta dan Time Travel.
    """
    try:
        logger.info("Initializing Spark session dengan Delta Lake support...")
        builder = (
            SparkSession.builder
            .appName("Gold-Pangan-Lakehouse")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory",  "2g")
            .config("spark.executor.memory", "2g")
            # HDFS — port 9000 sesuai docker-compose-hadoop.yml (9000:9000)
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session berhasil dibuat (Delta Lake enabled)")
        return spark
    except Exception as e:
        logger.error(f"Gagal membuat Spark session: {str(e)}")
        raise


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def baca_silver(spark, path: str, nama: str):
    """
    Baca Silver layer (parquet — ditulis oleh 02_silver.py).
    Menggunakan spark.read.parquet() untuk konsistensi dengan output Silver.
    """
    logger.info(f"Membaca Silver {nama} dari: {path}")
    try:
        df = spark.read.parquet(path)
        n  = df.count()
        logger.info(f"✓ Silver {nama}: {n:,} records siap diproses")
        return df
    except Exception as e:
        logger.error(f"Gagal membaca Silver {nama}: {e}")
        raise


def tulis_gold_delta(df, path: str, nama: str, mode: str = "overwrite"):
    """
    Tulis tabel Gold ke Delta format.
    Gold menggunakan Delta (berbeda dari Silver yang plain parquet)
    agar Time Travel dan ACID bisa didemonstrasikan.
    """
    logger.info(f"Menulis Gold {nama} ke: {path}")
    try:
        df.write.format("delta").mode(mode).save(path)
        n = spark_count_delta(df.sparkSession, path)
        logger.info(f"✓ Gold {nama} tersimpan: {n:,} records")
        return n
    except Exception as e:
        logger.error(f"Gagal menulis Gold {nama}: {e}")
        raise


def spark_count_delta(spark, path: str) -> int:
    """Hitung jumlah row di Delta table."""
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return -1


def tampilkan_section(judul: str):
    """Tampilkan header section yang rapi di log."""
    logger.info("")
    logger.info("=" * 65)
    logger.info(f"  {judul}")
    logger.info("=" * 65)


# ─────────────────────────────────────────────
# GOLD TABLE 1: pangan_volatility
# REPRO ETS Analysis 1 — Indeks Volatilitas Harga per Komoditas
# ─────────────────────────────────────────────

def build_pangan_volatility(spark, silver_api):
    """
    Tabel Gold: pangan_volatility

    Tujuan: Reproduksi Analisis 1 ETS — indeks volatilitas harga per komoditas.
    Definisi indeks volatilitas = (stddev_harga / mean_harga) * 100

    Peningkatan vs ETS lama:
      - ETS membaca JSON mentah → tipe data belum tentu benar, ada duplikat
      - Gold membaca dari Silver yang sudah bersih + timestamp valid
      - stddev dan mean akurat karena tidak ada outlier/duplikat
      - Kolom 'status_alert' langsung di-derive dari indeks (no manual threshold)

    Output kolom:
      komoditas, mean_harga, stddev_harga, min_harga, max_harga,
      volatility_index, status_alert, jumlah_observasi, _gold_created_at
    """
    tampilkan_section("GOLD TABLE 1: pangan_volatility (Repro ETS Analysis 1)")

    # Filter hanya record API (harga valid)
    api_df = silver_api.filter(
        col("harga").isNotNull() & (col("harga") > 0)
    )

    gold_df = (
        api_df
        .groupBy("komoditas")
        .agg(
            spark_round(avg("harga"),    2).alias("mean_harga"),
            spark_round(stddev("harga"), 2).alias("stddev_harga"),
            spark_round(spark_min("harga"), 0).alias("min_harga"),
            spark_round(spark_max("harga"), 0).alias("max_harga"),
            count("harga").alias("jumlah_observasi"),
        )
        # Hitung indeks volatilitas = (stddev / mean) * 100
        .withColumn(
            "volatility_index",
            spark_round(
                (col("stddev_harga") / col("mean_harga") * 100).cast(DoubleType()),
                2
            ),
        )
        # Klasifikasi status berdasarkan threshold ETS
        .withColumn(
            "status_alert",
            when(col("volatility_index") > VOLATILITY_CRITICAL, "CRITICAL")
            .when(col("volatility_index") > VOLATILITY_WARNING,  "WARNING")
            .otherwise("NORMAL"),
        )
        # Handle kasus stddev null (hanya 1 observasi)
        .withColumn(
            "volatility_index",
            when(col("volatility_index").isNull(), lit(0.0))
            .otherwise(col("volatility_index")),
        )
        .withColumn("_gold_created_at", current_timestamp())
        .orderBy(col("volatility_index").desc())
    )

    n = tulis_gold_delta(gold_df, GOLD_VOLATILITY_PATH, "pangan_volatility")

    logger.info("Preview pangan_volatility:")
    spark.read.format("delta").load(GOLD_VOLATILITY_PATH).show(truncate=False)

    return n


# ─────────────────────────────────────────────
# GOLD TABLE 2: pangan_trend
# REPRO ETS Analysis 2 — Tren Harga per Periode
# ─────────────────────────────────────────────

def build_pangan_trend(spark, silver_api):
    """
    Tabel Gold: pangan_trend

    Tujuan: Reproduksi Analisis 2 ETS — tren rata-rata harga per komoditas
    per hari dan per jam.

    Peningkatan vs ETS lama:
      - ETS hanya bisa agregasi kasar karena timestamp masih string
      - Silver sudah cast ke TimestampType → bisa date_trunc dan group per hari
      - Kolom 'jam' sudah tersedia dari Silver → tren per jam lebih akurat
      - Kolom 'hari_minggu' mendukung pola mingguan (Senin mahal, weekend murah?)

    Output kolom:
      komoditas, tanggal, jam, hari_minggu,
      avg_harga, min_harga, max_harga, jumlah_data, _gold_created_at
    """
    tampilkan_section("GOLD TABLE 2: pangan_trend (Repro ETS Analysis 2)")

    api_df = silver_api.filter(
        col("harga").isNotNull()
        & col("timestamp").isNotNull()
        & (col("harga") > 0)
    )

    gold_df = (
        api_df
        # Ekstrak tanggal dari timestamp (TimestampType sudah tersedia dari Silver)
        .withColumn("tanggal", to_date(col("timestamp")))
        .groupBy("komoditas", "tanggal", "jam", "hari_minggu")
        .agg(
            spark_round(avg("harga"),          0).alias("avg_harga"),
            spark_round(spark_min("harga"),    0).alias("min_harga"),
            spark_round(spark_max("harga"),    0).alias("max_harga"),
            count("harga").alias("jumlah_data"),
        )
        .withColumn("_gold_created_at", current_timestamp())
        .orderBy("komoditas", "tanggal", "jam")
    )

    n = tulis_gold_delta(gold_df, GOLD_TREND_PATH, "pangan_trend")

    logger.info("Preview pangan_trend (sample 10 rows):")
    spark.read.format("delta").load(GOLD_TREND_PATH).show(10, truncate=False)

    return n


# ─────────────────────────────────────────────
# GOLD TABLE 3: pangan_alert  ← ENHANCED
# Window Function: Early Warning Kenaikan Harga Signifikan
# ─────────────────────────────────────────────

def build_pangan_alert(spark, silver_api):
    """
    Tabel Gold: pangan_alert  ← TABEL ENHANCED (tidak ada di ETS)

    Tujuan: Deteksi early warning — komoditas yang mengalami perubahan harga
    > ALERT_PCT_THRESHOLD (default 5%) dibandingkan observasi sebelumnya.

    Mengapa ini tidak bisa dibuat di ETS?
      - ETS membaca JSON dengan timestamp sebagai string → tidak bisa Window.orderBy(timestamp)
      - ETS punya duplikat → lag() menghasilkan nilai yang salah
      - Silver sudah cast timestamp + dedup → Window Function akurat

    Teknik:
      - Window.partitionBy("komoditas").orderBy("timestamp")
      - lag("harga", 1) → harga observasi sebelumnya
      - pct_change = (harga - prev_harga) / prev_harga * 100
      - Filter hanya event yang |pct_change| > threshold

    Output kolom:
      komoditas, harga, prev_harga, pct_change, arah_perubahan,
      status_alert, tanggal, jam, timestamp, _gold_created_at
    """
    tampilkan_section("GOLD TABLE 3: pangan_alert [ENHANCED — Window Function]")
    logger.info(f"  Threshold early warning: |pct_change| > {ALERT_PCT_THRESHOLD}%")

    api_df = silver_api.filter(
        col("harga").isNotNull()
        & col("timestamp").isNotNull()
        & (col("harga") > 0)
    )

    # Window: partisi per komoditas, urut berdasarkan timestamp
    window_komoditas = Window.partitionBy("komoditas").orderBy("timestamp")

    alert_df = (
        api_df
        # Ambil harga observasi sebelumnya menggunakan lag
        .withColumn("prev_harga", lag("harga", 1).over(window_komoditas))
        # Hitung persentase perubahan
        .withColumn(
            "pct_change",
            spark_round(
                ((col("harga") - col("prev_harga")) / col("prev_harga") * 100)
                .cast(DoubleType()),
                2,
            ),
        )
        # Hanya proses record yang punya prev_harga (bukan row pertama per komoditas)
        .filter(col("prev_harga").isNotNull() & col("pct_change").isNotNull())
        # Arah perubahan dan status alert
        .withColumn(
            "arah_perubahan",
            when(col("pct_change") > 0,  "NAIK")
            .when(col("pct_change") < 0,  "TURUN")
            .otherwise("STABIL"),
        )
        .withColumn(
            "status_alert",
            when(col("pct_change") >  ALERT_PCT_THRESHOLD, "⚠ NAIK SIGNIFIKAN")
            .when(col("pct_change") < -ALERT_PCT_THRESHOLD, "📉 TURUN SIGNIFIKAN")
            .otherwise("Normal"),
        )
        # Hanya simpan kejadian signifikan
        .filter(col("status_alert") != "Normal")
        .withColumn("tanggal", to_date(col("timestamp")))
        .select(
            "komoditas",
            "harga",
            "prev_harga",
            "pct_change",
            "arah_perubahan",
            "status_alert",
            "tanggal",
            "jam",
            "timestamp",
        )
        .withColumn("_gold_created_at", current_timestamp())
        .orderBy(col("pct_change").desc())
    )

    n = tulis_gold_delta(alert_df, GOLD_ALERT_PATH, "pangan_alert")

    if n > 0:
        logger.info(f"Preview pangan_alert ({n} kejadian fluktuasi signifikan terdeteksi):")
        spark.read.format("delta").load(GOLD_ALERT_PATH).show(20, truncate=False)
    else:
        logger.info("Tidak ada kejadian fluktuasi signifikan — data harga mungkin masih sedikit.")

    return n


# ─────────────────────────────────────────────
# GOLD TABLE 4: pangan_news_correlation  ← ENHANCED
# Cross-Source Join: Silver API + Silver RSS
# ─────────────────────────────────────────────

def build_pangan_news_correlation(spark, silver_api, silver_rss):
    """
    Tabel Gold: pangan_news_correlation  ← TABEL ENHANCED (tidak ada di ETS)

    Tujuan: Gabungkan data harga (Silver API) dengan liputan berita (Silver RSS)
    untuk mengukur apakah intensitas pemberitaan korelasi dengan volatilitas harga.

    Insight yang dihasilkan:
      - Komoditas dengan banyak berita → apakah juga punya volatilitas tinggi?
      - "Sentiment proxy": frekuensi berita sebagai sinyal media pressure
      - Berguna sebagai early warning: lonjakan artikel → biasanya diikuti kenaikan harga

    Mengapa ini tidak bisa dibuat di ETS?
      - ETS analisis API dan RSS secara terpisah — tidak ada join lintas sumber
      - ETS tidak punya tabel volatilitas yang bersih sebagai base untuk join

    Teknik:
      - Agregasi Silver RSS: jumlah artikel per komoditas
      - Agregasi Silver API: volatility index per komoditas (dari pangan_volatility)
      - LEFT JOIN berdasarkan kolom 'komoditas'

    Output kolom:
      komoditas, jumlah_berita, mean_harga, volatility_index,
      status_volatility, kategori_korelasi, _gold_created_at
    """
    tampilkan_section("GOLD TABLE 4: pangan_news_correlation [ENHANCED — Cross-Source Join]")

    # ── Sisi RSS: agregasi jumlah artikel per komoditas ──
    logger.info("  Agregasi Silver RSS: jumlah artikel per komoditas...")
    rss_agg = (
        silver_rss
        .filter(col("komoditas").isNotNull())
        .groupBy("komoditas")
        .agg(
            count("*").alias("jumlah_berita"),
            count(when(col("jam").between(6, 12),  True)).alias("berita_pagi"),
            count(when(col("jam").between(13, 18), True)).alias("berita_siang"),
            count(when(col("jam").between(19, 23), True)).alias("berita_malam"),
        )
    )

    # ── Sisi API: ringkasan harga per komoditas ──
    logger.info("  Agregasi Silver API: statistik harga per komoditas...")
    api_agg = (
        silver_api
        .filter(col("harga").isNotNull() & (col("harga") > 0))
        .groupBy("komoditas")
        .agg(
            spark_round(avg("harga"),    0).alias("mean_harga"),
            spark_round(stddev("harga"), 2).alias("stddev_harga"),
            count("harga").alias("jumlah_observasi_harga"),
        )
        .withColumn(
            "volatility_index",
            spark_round(
                (col("stddev_harga") / col("mean_harga") * 100).cast(DoubleType()),
                2,
            ),
        )
        .withColumn(
            "status_volatility",
            when(col("volatility_index") > VOLATILITY_CRITICAL, "CRITICAL")
            .when(col("volatility_index") > VOLATILITY_WARNING,  "WARNING")
            .otherwise("NORMAL"),
        )
    )

    # ── JOIN: RSS kiri ← → API kanan ──
    # LEFT JOIN agar komoditas yang hanya ada di berita tetap muncul
    logger.info("  JOIN Silver RSS ↔ Silver API berdasarkan komoditas...")
    joined_df = rss_agg.alias("rss").join(
        api_agg.alias("api"),
        on="komoditas",
        how="left",
    )

    # ── Derive kategori korelasi berita-harga ──
    gold_df = (
        joined_df
        .withColumn(
            "kategori_korelasi",
            when(
                (col("jumlah_berita") > 5) & (col("volatility_index") > VOLATILITY_WARNING),
                "HIGH_COVERAGE + HIGH_VOLATILITY → WASPADA",
            )
            .when(
                (col("jumlah_berita") > 5) & (col("volatility_index") <= VOLATILITY_WARNING),
                "HIGH_COVERAGE + LOW_VOLATILITY → STABIL",
            )
            .when(
                (col("jumlah_berita") <= 5) & (col("volatility_index") > VOLATILITY_WARNING),
                "LOW_COVERAGE + HIGH_VOLATILITY → ANOMALI HARGA",
            )
            .otherwise("LOW_COVERAGE + LOW_VOLATILITY → NORMAL"),
        )
        .select(
            "komoditas",
            "jumlah_berita",
            "berita_pagi",
            "berita_siang",
            "berita_malam",
            "mean_harga",
            "stddev_harga",
            "volatility_index",
            "status_volatility",
            "jumlah_observasi_harga",
            "kategori_korelasi",
        )
        .withColumn("_gold_created_at", current_timestamp())
        .orderBy(col("jumlah_berita").desc())
    )

    n = tulis_gold_delta(gold_df, GOLD_NEWS_CORR_PATH, "pangan_news_correlation")

    logger.info("Preview pangan_news_correlation:")
    spark.read.format("delta").load(GOLD_NEWS_CORR_PATH).show(truncate=False)

    return n


# ─────────────────────────────────────────────
# TIME TRAVEL DEMO
# Demonstrasi kemampuan Delta Lake yang tidak ada di ETS
# ─────────────────────────────────────────────

def demo_time_travel(spark):
    """
    Demonstrasi Time Travel Delta Lake menggunakan tabel pangan_volatility.

    Alur demo:
      1. Tampilkan history tabel Gold (versi awal setelah build)
      2. Lakukan UPDATE: tandai semua komoditas CRITICAL dengan flag khusus
      3. Tampilkan data SEKARANG (setelah update)
      4. Query data VERSI 0 (sebelum update) menggunakan versionAsOf
      5. Bandingkan: buktikan Delta Lake menyimpan riwayat perubahan
    """
    tampilkan_section("DEMONSTRASI TIME TRAVEL Delta Lake — pangan_volatility")

    try:
        delta_table = DeltaTable.forPath(spark, GOLD_VOLATILITY_PATH)

        # ── Step 1: Tampilkan history ──
        logger.info("Step 1: History tabel Gold pangan_volatility")
        delta_table.history().select(
            "version", "timestamp", "operation", "operationParameters"
        ).show(truncate=False)

        # ── Step 2: Catat jumlah baris sebelum update ──
        count_v0 = spark.read.format("delta") \
            .option("versionAsOf", 0) \
            .load(GOLD_VOLATILITY_PATH) \
            .count()
        logger.info(f"Step 2: Versi 0 — {count_v0} baris (sebelum update)")

        # ── Step 3: Lakukan UPDATE — tambah kolom flag intervensi ──
        logger.info("Step 3: UPDATE — tambahkan kolom 'butuh_intervensi' = True untuk CRITICAL...")
        delta_table.update(
            condition = col("status_alert") == "CRITICAL",
            set       = {"status_alert": lit("CRITICAL ⚠ — PERLU INTERVENSI BULOG")},
        )
        logger.info("✓ Update selesai")

        # ── Step 4: History setelah update ──
        logger.info("Step 4: History setelah UPDATE:")
        delta_table.history().select(
            "version", "timestamp", "operation"
        ).show(truncate=False)

        # ── Step 5: Data SEKARANG (versi terbaru) ──
        logger.info("Step 5: Data SEKARANG (versi terbaru setelah update):")
        spark.read.format("delta") \
            .load(GOLD_VOLATILITY_PATH) \
            .select("komoditas", "volatility_index", "status_alert") \
            .orderBy(col("volatility_index").desc()) \
            .show(truncate=False)

        # ── Step 6: Query versi 0 (sebelum update) ──
        logger.info("Step 6: Data VERSI 0 (sebelum update — Time Travel):")
        spark.read.format("delta") \
            .option("versionAsOf", 0) \
            .load(GOLD_VOLATILITY_PATH) \
            .select("komoditas", "volatility_index", "status_alert") \
            .orderBy(col("volatility_index").desc()) \
            .show(truncate=False)

        logger.info("✓ Time Travel BERHASIL — Delta Lake menyimpan riwayat setiap versi.")
        logger.info("  Berbeda dengan HDFS/parquet biasa yang hanya menyimpan state terkini.")

    except Exception as e:
        logger.error(f"Time Travel gagal: {e}", exc_info=True)
        logger.warning("Pastikan Gold pangan_volatility sudah dibuat terlebih dahulu.")


# ─────────────────────────────────────────────
# EXPORT KE FLASK DASHBOARD
# Bonus: Dashboard membaca langsung dari Gold (bukan spark_results.json lama)
# ─────────────────────────────────────────────

def export_dashboard_json(spark):
    """
    Export ringkasan Gold layer ke spark_results.json untuk Flask Dashboard.

    Format:
      {
        "generated_at": "...",
        "source": "Gold Delta Layer",
        "volatility": [...],
        "trend": [...],
        "alerts": [...],
        "news_correlation": [...]
      }

    Dashboard Flask (app.py) membaca dari dashboard/data/spark_results.json.
    Dengan ini, dashboard menampilkan data yang sudah melalui pipeline
    Bronze → Silver → Gold, bukan langsung dari Spark analysis.py ETS.
    """
    tampilkan_section("EXPORT spark_results.json untuk Flask Dashboard")

    try:
        os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

        def delta_to_list(path: str, limit: int = 50) -> list:
            """Baca Delta table dan konversi ke list of dict."""
            try:
                df   = spark.read.format("delta").load(path)
                rows = df.limit(limit).collect()
                result = []
                for row in rows:
                    d = row.asDict()
                    # Konversi tipe non-serializable ke string
                    for k, v in d.items():
                        if hasattr(v, "isoformat"):          # datetime
                            d[k] = v.isoformat()
                        elif v is None:
                            d[k] = None
                    result.append(d)
                return result
            except Exception as e:
                logger.warning(f"Gagal baca {path} untuk export: {e}")
                return []

        hasil = {
            "generated_at" : datetime.now().isoformat(),
            "source"        : "Gold Delta Layer — lakehouse/03_gold.py",
            "pipeline"      : "HDFS → Bronze → Silver → Gold",
            "volatility"    : delta_to_list(GOLD_VOLATILITY_PATH),
            "trend"         : delta_to_list(GOLD_TREND_PATH, limit=100),
            "alerts"        : delta_to_list(GOLD_ALERT_PATH),
            "news_correlation": delta_to_list(GOLD_NEWS_CORR_PATH),
        }

        with open(SPARK_RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(hasil, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"✓ spark_results.json ditulis ke: {SPARK_RESULTS_FILE}")
        logger.info(f"  - volatility      : {len(hasil['volatility'])} rows")
        logger.info(f"  - trend           : {len(hasil['trend'])} rows")
        logger.info(f"  - alerts          : {len(hasil['alerts'])} rows")
        logger.info(f"  - news_correlation: {len(hasil['news_correlation'])} rows")

    except Exception as e:
        logger.error(f"Gagal export dashboard JSON: {e}", exc_info=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    spark = None
    start_time = datetime.now()

    try:
        logger.info("")
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║  HargaPangan — 03_gold.py                                ║")
        logger.info("║  Gold Layer: Agregasi & Enhanced Analysis                ║")
        logger.info("║  Orang D — Data Lakehouse Tugas Lanjutan ETS             ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")
        logger.info(f"Start time    : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Silver API    : {SILVER_API_PATH}")
        logger.info(f"Silver RSS    : {SILVER_RSS_PATH}")
        logger.info(f"Gold output   : {GOLD_DIR}/")
        logger.info(f"Dashboard     : {SPARK_RESULTS_FILE}")

        spark = create_spark_session()

        # ── Baca Silver (parquet — output dari 02_silver.py) ──
        tampilkan_section("MEMBACA SILVER LAYER")
        silver_api = baca_silver(spark, SILVER_API_PATH, "pangan_api")
        silver_rss = baca_silver(spark, SILVER_RSS_PATH, "pangan_rss")

        logger.info("Schema Silver API:")
        silver_api.printSchema()
        logger.info("Schema Silver RSS:")
        silver_rss.printSchema()

        # ── Build 4 Gold Tables ──
        n_volatility  = build_pangan_volatility(spark, silver_api)
        n_trend        = build_pangan_trend(spark, silver_api)
        n_alert        = build_pangan_alert(spark, silver_api)
        n_news_corr    = build_pangan_news_correlation(spark, silver_api, silver_rss)

        # ── Time Travel Demo ──
        demo_time_travel(spark)

        # ── Export Dashboard JSON ──
        export_dashboard_json(spark)

        # ── Final Summary ──
        end_time = datetime.now()
        durasi   = (end_time - start_time).total_seconds()

        tampilkan_section("GOLD LAYER PIPELINE — FINAL SUMMARY")
        logger.info(f"  pangan_volatility      (Repro ETS 1) : {n_volatility:,} rows")
        logger.info(f"  pangan_trend           (Repro ETS 2) : {n_trend:,} rows")
        logger.info(f"  pangan_alert           [ENHANCED]    : {n_alert:,} events")
        logger.info(f"  pangan_news_correlation [ENHANCED]   : {n_news_corr:,} rows")
        logger.info(f"  Dashboard JSON         : {SPARK_RESULTS_FILE}")
        logger.info(f"  Durasi pipeline        : {durasi:.1f} detik")
        logger.info(f"  Status                 : SUCCESS ✓")
        logger.info("=" * 65)

        # Struktur folder yang terbentuk
        logger.info("")
        logger.info("Struktur folder Gold yang terbentuk:")
        logger.info("  lakehouse_data/gold/")
        logger.info("    ├── pangan_volatility/     (Delta — Repro ETS Analysis 1)")
        logger.info("    ├── pangan_trend/          (Delta — Repro ETS Analysis 2)")
        logger.info("    ├── pangan_alert/          (Delta — Enhanced: Window Function)")
        logger.info("    └── pangan_news_correlation/ (Delta — Enhanced: Cross-Source Join)")

    except Exception as e:
        logger.error(f"Gold layer pipeline gagal: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session ditutup.")


if __name__ == "__main__":
    main()