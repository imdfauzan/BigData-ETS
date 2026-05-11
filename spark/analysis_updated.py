# Theoidorus Aaron Ugraha - 5027241056
# Updated: Integrated with RSS Google News pipeline

import json
import os
import re
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, ArrayType, IntegerType
)

# ─────────────────────────────────────────────────────────────
# KONFIGURASI SPARK & HDFS
# ─────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("HargaPangan-Analysis-Kelompok6") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_API_PATH   = "hdfs://localhost:9000/data/pangan/api/"
HDFS_RSS_PATH   = "hdfs://localhost:9000/data/pangan/rss/"
HDFS_HASIL_PATH = "hdfs://localhost:9000/data/pangan/hasil/"

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def sanitize_path(path):
    """Pastikan path ada dan clean."""
    # Remove trailing slashes
    return path.rstrip("/")

def word_frequency(text: str, min_length: int = 3) -> dict:
    """Extract word frequencies dari text (untuk analisis sentiment sederhana)."""
    if not text:
        return {}
    words = re.findall(r'\b[a-z]+\b', text.lower())
    freq = {}
    for word in words:
        if len(word) >= min_length:
            freq[word] = freq.get(word, 0) + 1
    return freq

# ─────────────────────────────────────────────────────────────
# STEP 1: LOAD DATA
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  STEP 1: LOAD DATA FROM HDFS")
print("="*70)

try:
    df_api = spark.read.option("multiLine", True).json(sanitize_path(HDFS_API_PATH))
    print(f"✓ API data dimuat: {df_api.count()} records")
    df_api.printSchema()
except Exception as e:
    print(f"✗ Gagal load API data: {e}")
    df_api = None

try:
    df_rss = spark.read.option("multiLine", True).json(sanitize_path(HDFS_RSS_PATH))
    print(f"✓ RSS data dimuat: {df_rss.count()} records")
    df_rss.printSchema()
except Exception as e:
    print(f"✗ Gagal load RSS data: {e}")
    df_rss = None

# ─────────────────────────────────────────────────────────────
# STEP 2: DATA PREPARATION & NORMALIZATION
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  STEP 2: DATA PREPARATION")
print("="*70)

# Prepare API data (harga komoditas)
if df_api is not None and df_api.count() > 0:
    df_api = df_api.withColumn("harga", F.col("harga").cast(DoubleType()))
    
    # Pastikan timestamp ada
    if "timestamp" not in df_api.columns:
        df_api = df_api.withColumn("timestamp", F.current_timestamp())
    
    # Parse timestamp ke date/hour
    df_api = df_api.withColumn(
        "ts_parsed", 
        F.from_unixtime(F.unix_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    ).withColumn(
        "tanggal", F.date_format(F.col("ts_parsed"), "yyyy-MM-dd")
    ).withColumn(
        "jam_ke", F.hour(F.col("ts_parsed"))
    )
    
    df_api.createOrReplaceTempView("harga_pangan")
    print(f"✓ API data prepared: {df_api.columns}")

# Prepare RSS data (berita)
if df_rss is not None and df_rss.count() > 0:
    # Ensure key columns
    if "title" not in df_rss.columns:
        df_rss = df_rss.withColumnRenamed("judul", "title")
    if "description" not in df_rss.columns:
        df_rss = df_rss.withColumnRenamed("summary", "description")
    
    df_rss = df_rss.fillna("", ["title", "description", "link"])
    
    # Parse published date
    if "published" in df_rss.columns:
        df_rss = df_rss.withColumn(
            "published_date", 
            F.to_date(F.col("published"), "yyyy-MM-dd HH:mm:ss")
        )
    else:
        df_rss = df_rss.withColumn("published_date", F.current_date())
    
    df_rss.createOrReplaceTempView("berita_pangan")
    print(f"✓ RSS data prepared: {df_rss.columns}")

# ─────────────────────────────────────────────────────────────
# ANALISIS 1: VOLATILITAS KOMODITAS HARGA
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  ANALISIS 1: VOLATILITAS KOMODITAS")
print("="*70)

if df_api is not None and df_api.count() > 0:
    # Hitung indeks volatilitas
    df_volatilitas = df_api.groupBy("komoditas", "label") \
        .agg(
            F.max("harga").alias("harga_max"),
            F.min("harga").alias("harga_min"),
            F.avg("harga").alias("harga_avg"),
            F.stddev("harga").alias("harga_stddev"),
            F.count("harga").alias("jumlah_data"),
            F.count(F.distinct("tanggal")).alias("jumlah_hari")
        ) \
        .withColumn(
            "indeks_volatilitas",
            F.round(
                F.when(F.col("harga_avg") > 0,
                    (F.col("harga_max") - F.col("harga_min")) / F.col("harga_avg") * 100
                ).otherwise(0),
                2
            )
        ) \
        .withColumn("harga_max", F.round(F.col("harga_max"), 0)) \
        .withColumn("harga_min", F.round(F.col("harga_min"), 0)) \
        .withColumn("harga_avg", F.round(F.col("harga_avg"), 0)) \
        .withColumn("harga_stddev", F.round(F.col("harga_stddev"), 0)) \
        .orderBy(F.col("indeks_volatilitas").desc())
    
    df_volatilitas.createOrReplaceTempView("volatilitas_analysis")
    
    print("\n📊 Ranking Volatilitas Komoditas (tertinggi → terendah):")
    df_volatilitas.show(10, truncate=False)
    
    vol_rows = df_volatilitas.collect()
    if vol_rows:
        top = vol_rows[0]
        print(f"""
🎯 INSIGHT #1:
• Komoditas PALING VOLATIL: {top['label']}
  Indeks volatilitas: {top['indeks_volatilitas']}%
  Rentang harga: Rp {top['harga_min']:,.0f} - Rp {top['harga_max']:,.0f}
  Rata-rata: Rp {top['harga_avg']:,.0f}
  Data: {top['jumlah_data']} observasi selama {top['jumlah_hari']} hari
  
✓ Rekomendasi: Perlu intervensi pasar dan buffer stok nasional untuk 
  mencegah lonjakan harga yang merugikan konsumen.
""")

# ─────────────────────────────────────────────────────────────
# ANALISIS 2: TREN HARGA TEMPORAL
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  ANALISIS 2: TREN HARGA TEMPORAL")
print("="*70)

if df_api is not None and df_api.count() > 0:
    # Tren per hari
    query_tren = """
    SELECT
        komoditas,
        label,
        tanggal,
        ROUND(AVG(harga), 0)   AS harga_rata2,
        ROUND(MIN(harga), 0)   AS harga_min,
        ROUND(MAX(harga), 0)   AS harga_max,
        COUNT(*)               AS jumlah_observasi
    FROM harga_pangan
    WHERE komoditas IS NOT NULL AND harga IS NOT NULL
    GROUP BY komoditas, label, tanggal
    ORDER BY komoditas, tanggal DESC
    LIMIT 30
    """
    df_tren = spark.sql(query_tren)
    
    print("\n📊 Rata-rata Harga per Komoditas per Hari (30 hari terakhir):")
    df_tren.show(30, truncate=False)
    
    # Ringkasan keseluruhan
    query_ringkasan = """
    SELECT
        komoditas,
        label,
        ROUND(AVG(harga), 0)   AS harga_avg,
        ROUND(MIN(harga), 0)   AS harga_min,
        ROUND(MAX(harga), 0)   AS harga_max,
        COUNT(DISTINCT tanggal) AS jumlah_hari,
        COUNT(*) AS total_observasi
    FROM harga_pangan
    WHERE komoditas IS NOT NULL AND harga IS NOT NULL
    GROUP BY komoditas, label
    ORDER BY harga_avg DESC
    """
    df_ringkasan = spark.sql(query_ringkasan)
    
    print("\n📊 Ringkasan Tren Harga Keseluruhan:")
    df_ringkasan.show(truncate=False)
    
    ringkasan_rows = df_ringkasan.collect()
    if ringkasan_rows:
        termahal = ringkasan_rows[0]
        print(f"""
🎯 INSIGHT #2:
• Komoditas TERMAHAL: {termahal['label']}
  Harga rata-rata: Rp {termahal['harga_avg']:,.0f}
  Range: Rp {termahal['harga_min']:,.0f} - Rp {termahal['harga_max']:,.0f}
  Periode: {termahal['jumlah_hari']} hari dengan {termahal['total_observasi']} data points

✓ Rekomendasi: Monitor tren jangka panjang untuk deteksi pola musiman
  dan anomali harga yang memerlukan respons cepat.
""")

# ─────────────────────────────────────────────────────────────
# ANALISIS 3: KORELASI BERITA ↔ HARGA GEJOLAK
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  ANALISIS 3: KORELASI BERITA vs VOLATILITAS HARGA")
print("="*70)

if df_rss is not None and df_rss.count() > 0:
    # Buat UDF untuk deteksi komoditas dari text
    komoditas_keywords = {
        "beras": ["beras", "padi", "gabah"],
        "jagung": ["jagung"],
        "kedelai": ["kedelai", "tempe", "tahu"],
        "gula": ["gula", "tebu"],
        "minyak_goreng": ["minyak", "sawit", "cpo"],
        "cabai": ["cabai", "cabe"],
        "bawang_merah": ["bawang"],
        "telur": ["telur", "ayam"],
    }
    
    @F.udf("string")
    def detect_komoditas_udf(text):
        if not text:
            return "umum"
        text_lower = text.lower()
        for kom, keywords in komoditas_keywords.items():
            for kw in keywords:
                if kw in text_lower:
                    return kom
        return "umum"
    
    # Tag komoditas di setiap berita
    df_berita_tagged = df_rss.withColumn(
        "komoditas_detected",
        detect_komoditas_udf(F.concat(F.col("title"), F.lit(" "), F.col("description")))
    )
    
    # Hitung frekuensi sebutan per komoditas
    df_sebutan = df_berita_tagged.groupBy("komoditas_detected") \
        .agg(F.count("*").alias("jumlah_berita")) \
        .withColumnRenamed("komoditas_detected", "komoditas") \
        .orderBy(F.col("jumlah_berita").desc())
    
    print("\n📰 Frekuensi Sebutan Komoditas dalam Berita:")
    df_sebutan.show(truncate=False)
    
    total_berita = df_rss.count()
    print(f"\nTotal artikel RSS: {total_berita}")
    
    # Jika ada data API, correlate dengan volatilitas
    if df_api is not None and df_api.count() > 0:
        df_volatilitas_temp = df_api.groupBy("komoditas") \
            .agg(
                F.avg("harga").alias("harga_avg"),
                F.stddev("harga").alias("harga_stddev"),
                F.max("harga").alias("harga_max"),
                F.min("harga").alias("harga_min")
            ) \
            .withColumn(
                "indeks_volatilitas",
                F.round(
                    F.when(F.col("harga_avg") > 0,
                        (F.col("harga_max") - F.col("harga_min")) / F.col("harga_avg") * 100
                    ).otherwise(0),
                    2
                )
            )
        
        # Join dengan sebutan
        df_korelasi = df_sebutan.join(
            df_volatilitas_temp,
            on="komoditas",
            how="left"
        ).fillna(0, ["indeks_volatilitas"]) \
        .withColumn(
            "sentiment_indicator",
            F.case() \
                .when(F.col("jumlah_berita") > 10, "HIGH_COVERAGE") \
                .when(F.col("jumlah_berita") > 5, "MEDIUM_COVERAGE") \
                .otherwise("LOW_COVERAGE")
        ) \
        .orderBy(F.col("jumlah_berita").desc())
        
        print("\n📊 Korelasi Berita vs Volatilitas Harga:")
        df_korelasi.show(truncate=False)
        
        print("""
🎯 INSIGHT #3:
• Komoditas dengan HIGH_COVERAGE dan HIGH VOLATILITY:
  → Indikasi media merespons dengan baik terhadap gejolak harga
  → Public awareness tinggi
  
• Komoditas dengan LOW_COVERAGE tapi HIGH VOLATILITY:
  → ⚠️  BLIND SPOT: Fluktuasi harga tidak terdeteksi media
  → Risiko lonjakan harga yang mengejutkan kebijakan publik

✓ Rekomendasi: Prioritaskan monitoring untuk blind spots dan pastikan
  kebijakan intervensi harga tidak terlambat.
""")

# ─────────────────────────────────────────────────────────────
# ANALISIS 4: NEWS SENTIMENT PROXY (WORD ANALYSIS)
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  ANALISIS 4: NEWS TONE & KEYWORDS")
print("="*70)

if df_rss is not None and df_rss.count() > 0:
    # Deteksi tone dari judul (sentiment proxy)
    negative_keywords = ["kenaikan", "lonjakan", "melonjak", "ancaman", "krisis", 
                        "kelangkaan", "defisit", "impor", "darurat"]
    positive_keywords = ["panen", "surplus", "stabil", "turun", "efisien"]
    
    @F.udf("string")
    def detect_tone(text):
        if not text:
            return "neutral"
        text_lower = text.lower()
        neg_count = sum(1 for kw in negative_keywords if kw in text_lower)
        pos_count = sum(1 for kw in positive_keywords if kw in text_lower)
        
        if neg_count > pos_count:
            return "negative"
        elif pos_count > neg_count:
            return "positive"
        return "neutral"
    
    df_tone = df_rss.withColumn("tone", detect_tone(F.col("title"))) \
        .groupBy("tone") \
        .agg(F.count("*").alias("count")) \
        .orderBy(F.col("count").desc())
    
    print("\n🎭 Tone Deteksi dari Judul Berita:")
    df_tone.show(truncate=False)

# ─────────────────────────────────────────────────────────────
# STEP 3: SIMPAN HASIL KE HDFS & LOKAL
# ─────────────────────────────────────────────────────────────

print("\n" + "="*70)
print("  STEP 3: SAVE RESULTS")
print("="*70)

# Prepare hasil dictionary
spark_results = {
    "metadata": {
        "generated_at": datetime.now().isoformat(),
        "generated_by": "Theodorus Aaron Ugraha (Anggota 4) - Updated",
        "pipeline": "HargaPangan-Kelompok6-Enhanced",
    },
    "summary": {
        "total_data_api": int(df_api.count()) if df_api else 0,
        "total_data_rss": int(df_rss.count()) if df_rss else 0,
    }
}

# Add analysis results
if df_api is not None:
    try:
        df_volatilitas.coalesce(1).write.mode("overwrite") \
            .json(sanitize_path(HDFS_HASIL_PATH + "volatilitas"))
        print(f"✓ Volatilitas saved to HDFS: {HDFS_HASIL_PATH}volatilitas/")
        
        spark_results["analisis_1_volatilitas"] = \
            df_volatilitas.toPandas().to_dict(orient="records")
    except Exception as e:
        print(f"✗ Error saving volatilitas: {e}")

if df_rss is not None:
    try:
        df_rss.coalesce(1).write.mode("overwrite") \
            .json(sanitize_path(HDFS_HASIL_PATH + "berita_raw"))
        print(f"✓ Berita saved to HDFS: {HDFS_HASIL_PATH}berita_raw/")
        
        spark_results["analisis_3_sebutan_komoditas"] = \
            df_sebutan.toPandas().to_dict(orient="records")
    except Exception as e:
        print(f"✗ Error saving berita: {e}")

# Save ke local file untuk dashboard
dashboard_data_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
    "dashboard", 
    "data"
)
os.makedirs(dashboard_data_dir, exist_ok=True)

output_path = os.path.join(dashboard_data_dir, "spark_results.json")
try:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(spark_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"✓ Dashboard results saved: {output_path}")
except Exception as e:
    print(f"✗ Error saving dashboard results: {e}")

print("\n" + "="*70)
print("  ✓ ANALISIS SELESAI")
print("="*70)

spark.stop()
print("Spark session ditutup.")
