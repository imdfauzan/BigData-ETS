"""
spark/analysis.py — Theodorus Aaron Ugraha (Anggota 4)
Topik: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Improvement v2:
  - Direct HDFS reading (native Spark)
  - Fallback ke lokal copy jika HDFS tidak accessible
  - Narasi & interpretasi bisnis di setiap analisis
  - Better error handling dan logging
  - NaN/Inf handling di DataFrame level
"""

import json
import math
import os
import subprocess
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("spark_analysis")

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

BASE_DIR           = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_STAGING_DIR  = "/tmp/pangan_spark_staging"
DASHBOARD_DATA_DIR = os.path.join(BASE_DIR, "dashboard", "data")

# HDFS Configuration
HDFS_NAMENODE     = os.getenv("HDFS_NAMENODE", "namenode:8020")
HDFS_API_PATH     = "hdfs://" + HDFS_NAMENODE + "/data/pangan/api"
HDFS_RSS_PATH     = "hdfs://" + HDFS_NAMENODE + "/data/pangan/rss"
HDFS_HASIL_PATH   = "/data/pangan/hasil"

os.makedirs(LOCAL_STAGING_DIR,  exist_ok=True)
os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

print("=" * 70)
print("  HargaPangan - Spark Analysis (Native HDFS + Fallback)")
print("=" * 70)
log.info(f"HDFS Namenode: {HDFS_NAMENODE}")
log.info(f"Dashboard output: {DASHBOARD_DATA_DIR}")


# ─────────────────────────────────────────────
# SPARK SESSION INITIALIZATION
# ─────────────────────────────────────────────

log.info("Initializing Spark Session...")

spark = (
    SparkSession.builder
    .appName("HargaPangan-Kelompok6")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_NAMENODE}")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
log.info("✓ Spark session initialized")


# ─────────────────────────────────────────────
# DATA LOADING (Local JSON Files)
# ─────────────────────────────────────────────

def load_data_from_local(json_file: str, data_type: str = "api") -> tuple:
    """
    Load data dari local JSON file.
    Return: (DataFrame, source: str)
    """
    try:
        log.info(f"Reading from local file: {json_file}")
        
        if not os.path.exists(json_file):
            log.error(f"  ✗ File not found: {json_file}")
            return None, "failed"
        
        # Read JSON file
        with open(json_file, "r", encoding="utf-8") as f:
            file_data = json.load(f)
        
        # Extract records based on data structure
        records = []
        if isinstance(file_data, dict) and "data" in file_data:
            records = file_data["data"]
        elif isinstance(file_data, list):
            records = file_data
        
        if not records:
            log.warning(f"  ✗ No records found in {json_file}")
            return None, "empty"
        
        log.info(f"  ✓ Successfully read {len(records)} records from {json_file}")
        
        # Convert to DataFrame
        df = spark.createDataFrame(records)
        return df, "local"
        
    except Exception as e:
        log.error(f"  ✗ Failed to load {json_file}: {e}")
        return None, "failed"





# Load data
LOCAL_API_FILE = os.path.join(DASHBOARD_DATA_DIR, "live_api.json")
LOCAL_RSS_FILE = os.path.join(DASHBOARD_DATA_DIR, "live_rss.json")

df_api, source_api = load_data_from_local(LOCAL_API_FILE, data_type="api")
if df_api is None:
    log.error("Cannot load API data. Exiting.")
    sys.exit(1)

df_rss, source_rss = load_data_from_local(LOCAL_RSS_FILE, data_type="rss")
if df_rss is None:
    log.warning("No RSS data available, creating empty DataFrame")
    df_rss = spark.createDataFrame(
        [{"title": "", "link": "", "komoditas": "umum", "timestamp": ""}]
    )

log.info(f"API data source: {source_api} ({df_api.count()} records)")
log.info(f"RSS data source: {source_rss} ({df_rss.count()} records)")

# Data cleaning
df_api = df_api.withColumn("harga", F.col("harga").cast(DoubleType()))
df_api = df_api.withColumn("harga", 
    F.when(F.isnan(F.col("harga")), F.lit(0.0))
    .otherwise(F.col("harga"))
)

# Add datetime columns
if "jam" in df_api.columns and "tanggal" in df_api.columns:
    df_api = (
        df_api
        .withColumn("datetime",
            F.to_timestamp(F.concat_ws(" ", "tanggal", "jam"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hari", F.date_format("datetime", "yyyy-MM-dd"))
        .withColumn("jam_ke", F.hour("datetime"))
    )
elif "tanggal" in df_api.columns:
    df_api = df_api.withColumn("hari", F.col("tanggal")).withColumn("jam_ke", F.lit(0))
else:
    df_api = df_api.withColumn("hari", F.lit("unknown")).withColumn("jam_ke", F.lit(0))

df_api.createOrReplaceTempView("harga_pangan")
df_rss.createOrReplaceTempView("berita_pangan")


# ─────────────────────────────────────────────
# ANALISIS 1: VOLATILITAS HARGA
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALISIS 1: VOLATILITAS HARGA PER KOMODITAS")
print("=" * 70)

df_volatilitas = (
    df_api
    .groupBy("komoditas", "label")
    .agg(
        F.max("harga").alias("harga_max"),
        F.min("harga").alias("harga_min"),
        F.avg("harga").alias("harga_avg"),
        F.stddev("harga").alias("harga_stddev"),
        F.count("harga").alias("jumlah_data"),
    )
    .withColumn("indeks_volatilitas",
        F.when(F.col("harga_avg") > 0,
            F.round((F.col("harga_max") - F.col("harga_min")) / F.col("harga_avg") * 100, 2)
        ).otherwise(F.lit(0.0))
    )
    .withColumn("harga_stddev",
        F.when(F.col("harga_stddev").isNull(), F.lit(0.0))
        .otherwise(F.round("harga_stddev", 2))
    )
    .withColumn("harga_max", F.round("harga_max", 2))
    .withColumn("harga_min", F.round("harga_min", 2))
    .withColumn("harga_avg", F.round("harga_avg", 2))
    .orderBy(F.col("indeks_volatilitas").desc())
)

df_volatilitas.show(truncate=False)

# INTERPRETASI ANALISIS 1
print("\n📊 INTERPRETASI & REKOMENDASI:")
print("─" * 70)
volatilitas_data = df_volatilitas.toPandas()
for idx, row in volatilitas_data.iterrows():
    vol = row['indeks_volatilitas']
    komoditas = row['label']
    
    if vol > 20:
        status = "🔴 CRITICAL"
        rec = "Bulog HARUS segera intervensi supply chain"
    elif vol > 10:
        status = "🟠 WARNING"
        rec = "Monitoring ketat, cek ketersediaan stok"
    else:
        status = "🟢 NORMAL"
        rec = "Kondisi stabil, lanjutkan monitoring rutin"
    
    print(f"{status} {komoditas:15s} (vol={vol:6.2f}%)")
    print(f"           Rekomendasi: {rec}")
    print(f"           Harga: Rp {row['harga_min']:>10,.0f} - Rp {row['harga_max']:>10,.0f}")

df_volatilitas.createOrReplaceTempView("volatilitas")


# ─────────────────────────────────────────────
# ANALISIS 2: TREN HARGA
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("ANALISIS 2: TREN HARGA PER KOMODITAS (TIME SERIES)")
print("=" * 70)

df_ringkasan_tren = spark.sql("""
    SELECT
        komoditas, label,
        ROUND(AVG(harga), 2)      AS harga_rata2_keseluruhan,
        ROUND(MIN(harga), 2)      AS harga_terendah_sepanjang,
        ROUND(MAX(harga), 2)      AS harga_tertinggi_sepanjang,
        COUNT(DISTINCT hari)      AS jumlah_hari_data,
        COUNT(*)                  AS total_observasi
    FROM harga_pangan
    WHERE komoditas IS NOT NULL
    GROUP BY komoditas, label
    ORDER BY harga_rata2_keseluruhan DESC
""")
df_ringkasan_tren.show(truncate=False)

# INTERPRETASI ANALISIS 2
print("\n📈 INTERPRETASI & REKOMENDASI:")
print("─" * 70)
tren_data = df_ringkasan_tren.toPandas()
for idx, row in tren_data.iterrows():
    komoditas = row['label']
    min_p = row['harga_terendah_sepanjang']
    max_p = row['harga_tertinggi_sepanjang']
    avg_p = row['harga_rata2_keseluruhan']
    range_pct = ((max_p - min_p) / min_p * 100) if min_p > 0 else 0
    
    print(f"📍 {komoditas:15s}")
    print(f"   Harga rata-rata: Rp {avg_p:>10,.0f}")
    print(f"   Range sepanjang waktu: Rp {min_p:>10,.0f} - Rp {max_p:>10,.0f} ({range_pct:.1f}% range)")
    print(f"   Data points: {row['total_observasi']} observasi dalam {int(row['jumlah_hari_data'])} hari")
    
    if avg_p > 30000:
        print(f"   💰 Catatan: Harga tinggi - monitor untuk potensi dampak daya beli")
    print()

df_ringkasan_tren.createOrReplaceTempView("ringkasan_tren")


# ─────────────────────────────────────────────
# ANALISIS 3: KORELASI BERITA vs HARGA
# ─────────────────────────────────────────────

print("=" * 70)
print("ANALISIS 3: KORELASI BERITA vs VOLATILITAS HARGA")
print("=" * 70)

KOMODITAS_LIST = [
    "beras","jagung","kedelai","gula",
    "minyak_goreng","cabai","bawang_merah","telur"
]
KEYWORD_MAP = {
    "beras":"beras","jagung":"jagung","kedelai":"kedelai","gula":"gula",
    "minyak_goreng":"minyak","cabai":"cabai","bawang_merah":"bawang","telur":"telur",
}

title_col = "title" if "title" in df_rss.columns else df_rss.columns[0]
df_tagged = df_rss
for kom, kw in KEYWORD_MAP.items():
    df_tagged = df_tagged.withColumn(
        f"sebut_{kom}",
        F.when(F.lower(F.col(title_col)).contains(kw), 1).otherwise(0)
    )

sebutan_rows = []
for kom in KOMODITAS_LIST:
    col_name = f"sebut_{kom}"
    count = 0
    if col_name in df_tagged.columns:
        val = df_tagged.agg(F.sum(col_name)).collect()[0][0]
        count = int(val) if val else 0
    sebutan_rows.append((kom, KEYWORD_MAP.get(kom, kom), count))

df_sebutan = spark.createDataFrame(sebutan_rows, ["komoditas","keyword","jumlah_sebutan"])
df_sebutan.createOrReplaceTempView("sebutan_berita")

# Create korelasi query
df_korelasi = spark.sql("""
    SELECT * FROM (
        SELECT
            v.komoditas, v.label,
            COALESCE(v.indeks_volatilitas, 0.0) AS indeks_volatilitas,
            COALESCE(v.harga_avg, 0.0)          AS harga_avg,
            COALESCE(s.jumlah_sebutan, 0)       AS jumlah_sebutan,
            CASE
                WHEN s.jumlah_sebutan > 5 AND v.indeks_volatilitas > 10
                    THEN 'TINGGI: Banyak liputan + Harga bergejolak'
                WHEN s.jumlah_sebutan > 5 AND v.indeks_volatilitas <= 10
                    THEN 'MODERAT: Sering diberitakan, harga relatif stabil'
                WHEN s.jumlah_sebutan <= 5 AND v.indeks_volatilitas > 10
                    THEN 'WASPADA: Harga bergejolak tapi minim liputan'
                ELSE 'RENDAH: Stabil dan jarang diberitakan'
            END AS status_korelasi
        FROM volatilitas v
        LEFT JOIN sebutan_berita s ON v.komoditas = s.komoditas
    )
    ORDER BY jumlah_sebutan DESC, indeks_volatilitas DESC
""")

df_korelasi.show(truncate=False)

# INTERPRETASI ANALISIS 3
print("\n📰 INTERPRETASI & INSIGHT:")
print("─" * 70)
korelasi_data = df_korelasi.toPandas()
for idx, row in korelasi_data.iterrows():
    status = row['status_korelasi']
    komoditas = row['label']
    mentions = row['jumlah_sebutan']
    vol = row['indeks_volatilitas']
    
    print(f"📍 {komoditas:15s} → {status}")
    print(f"   Liputan: {int(mentions)} artikel | Volatilitas: {vol:.2f}%")
    
    if mentions > 5 and vol > 10:
        print(f"   ⚠️  ALERT: Media sering membahas dan harga sangat fluktuatif")
    elif mentions > 5:
        print(f"   ℹ️  Komoditas mendapat perhatian media (monitoring diperlukan)")
    elif vol > 10:
        print(f"   🔔 Harga bergejolak namun media coverage terbatas")
    print()


# ─────────────────────────────────────────────
# SAVE RESULTS
# ─────────────────────────────────────────────

def safe_float(v):
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 2)
    except (TypeError, ValueError):
        return 0.0

def safe_int(v):
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0

def rows_to_clean_list(rows, float_cols=(), int_cols=()):
    result = []
    for row in rows:
        d = row.asDict()
        for col in float_cols:
            if col in d:
                d[col] = safe_float(d[col])
        for col in int_cols:
            if col in d:
                d[col] = safe_int(d[col])
        result.append(d)
    return result

log.info("Saving analysis results...")

spark_results = {
    "metadata": {
        "generated_by": "Theodorus Aaron Ugraha (Anggota 4)",
        "generated_at": datetime.now().isoformat(),
        "pipeline": "HargaPangan-Kelompok6",
        "data_source_api": source_api,
        "data_source_rss": source_rss,
        "hdfs_namenode": HDFS_NAMENODE,
        "total_data_api": df_api.count(),
        "total_data_rss": df_rss.count(),
    },
    "analyses": {
        "analisis_1_volatilitas": rows_to_clean_list(
            df_volatilitas.collect(),
            float_cols=("harga_max","harga_min","harga_avg","harga_stddev","indeks_volatilitas"),
            int_cols=("jumlah_data",)
        ),
        "analisis_2_tren_harga": rows_to_clean_list(
            df_ringkasan_tren.collect(),
            float_cols=("harga_rata2_keseluruhan","harga_terendah_sepanjang","harga_tertinggi_sepanjang"),
            int_cols=("jumlah_hari_data","total_observasi")
        ),
        "analisis_3_korelasi_berita": rows_to_clean_list(
            df_korelasi.collect(),
            float_cols=("indeks_volatilitas","harga_avg"),
            int_cols=("jumlah_sebutan",)
        ),
    },
}

# Save to dashboard
output_path = os.path.join(DASHBOARD_DATA_DIR, "spark_results.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(spark_results, f, ensure_ascii=False, indent=2)
log.info(f"✓ Saved to dashboard: {output_path}")

# Upload to HDFS
tmp_path = "/tmp/spark_results_upload.json"
with open(tmp_path, "w", encoding="utf-8") as f:
    json.dump(spark_results, f, ensure_ascii=False, indent=2)

try:
    subprocess.run(["docker", "cp", tmp_path, "namenode:/tmp/spark_results.json"], check=True)
    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", HDFS_HASIL_PATH], check=True)
    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f", "/tmp/spark_results.json", HDFS_HASIL_PATH], check=True)
    log.info(f"✓ Saved to HDFS: {HDFS_HASIL_PATH}/spark_results.json")
except Exception as e:
    log.warning(f"Could not save to HDFS: {e}")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("RINGKASAN EKSEKUSI")
print("=" * 70)
print(f"Waktu generasi: {spark_results['metadata']['generated_at']}")
print(f"Total data API: {spark_results['metadata']['total_data_api']} records")
print(f"Total data RSS: {spark_results['metadata']['total_data_rss']} articles")
print(f"Hasil disimpan ke: {output_path}")
print("=" * 70)

spark.stop()
log.info("Spark session closed.")
