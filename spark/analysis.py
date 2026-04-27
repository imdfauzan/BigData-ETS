#Theoidorus Aaron Ugraha - 5027241056

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType
# Konfigurasi Spark dengan koneksi ke HDFS
spark = SparkSession.builder \
    .appName("HargaPangan-Analysis-Kelompok6") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_API_PATH = "hdfs://localhost:9000/data/pangan/api/"
HDFS_RSS_PATH = "hdfs://localhost:9000/data/pangan/rss/"
HDFS_HASIL_PATH = "hdfs://localhost:9000/data/pangan/hasil/"
# Membaca semua file JSON dari folder HDFS sekaligus
df_api = spark.read.option("multiLine", True).json(HDFS_API_PATH)
print(f"  → {df_api.count()} record API dimuat")
df_api.printSchema()
df_api.show(5, truncate=False)
df_rss = spark.read.option("multiLine", True).json(HDFS_RSS_PATH)
print(f"  → {df_rss.count()} record RSS dimuat")
df_rss.printSchema()
df_rss.show(5, truncate=False)
# Pastikan kolom harga bertipe numerik
df_api = df_api.withColumn("harga", F.col("harga").cast(DoubleType()))
# Buat kolom datetime dari tanggal + jam (jika ada)
if "jam" in df_api.columns and "tanggal" in df_api.columns:
    df_api = df_api.withColumn(
        "datetime",
        F.to_timestamp(F.concat_ws(" ", F.col("tanggal"), F.col("jam")), "yyyy-MM-dd HH:mm:ss")
    )
    df_api = df_api.withColumn("hari", F.date_format(F.col("datetime"), "yyyy-MM-dd"))
    df_api = df_api.withColumn("jam_ke", F.hour(F.col("datetime")))
elif "tanggal" in df_api.columns:
    df_api = df_api.withColumn("hari", F.col("tanggal"))
    df_api = df_api.withColumn("jam_ke", F.lit(0))
# Register sebagai temp view untuk Spark SQL
df_api.createOrReplaceTempView("harga_pangan")
df_rss.createOrReplaceTempView("berita_pangan")
print("  → Temp view 'harga_pangan' dan 'berita_pangan' terdaftar")
print(f"  → Kolom API: {df_api.columns}")
print(f"  → Kolom RSS: {df_rss.columns}")
# Daftar komoditas yang dipantau
KOMODITAS = ["beras", "jagung", "kedelai", "gula", "minyak_goreng", "cabai", "bawang_merah", "telur"]
# Hitung indeks volatilitas relatif per komoditas
df_volatilitas = df_api.groupBy("komoditas", "label") \
    .agg(
        F.max("harga").alias("harga_max"),
        F.min("harga").alias("harga_min"),
        F.avg("harga").alias("harga_avg"),
        F.stddev("harga").alias("harga_stddev"),
        F.count("harga").alias("jumlah_data")
    ) \
    .withColumn(
        "indeks_volatilitas",
        F.round((F.col("harga_max") - F.col("harga_min")) / F.col("harga_avg") * 100, 2)
    ) \
    .withColumn("harga_max", F.round(F.col("harga_max"), 2)) \
    .withColumn("harga_min", F.round(F.col("harga_min"), 2)) \
    .withColumn("harga_avg", F.round(F.col("harga_avg"), 2)) \
    .withColumn("harga_stddev", F.round(F.col("harga_stddev"), 2)) \
    .orderBy(F.col("indeks_volatilitas").desc())
print("\n📊 Ranking Volatilitas Komoditas (tertinggi → terendah):")
df_volatilitas.show(truncate=False)
# --- Narasi Interpretasi Bisnis ---
print("Interpretasi ANALISIS 1:")
vol_rows = df_volatilitas.collect()
if vol_rows:
    top = vol_rows[0]
    bottom = vol_rows[-1]
    print(f"""
Berdasarkan analisis volatilitas dari {sum(r['jumlah_data'] for r in vol_rows)} total data harga:
• Komoditas PALING VOLATIL: {top['label']} dengan indeks {top['indeks_volatilitas']}%
  Rentang harga: Rp {top['harga_min']:,.0f} - Rp {top['harga_max']:,.0f} (rata-rata Rp {top['harga_avg']:,.0f})
  Ini menunjukkan fluktuasi harga yang signifikan dan memerlukan perhatian khusus
  dari Bulog untuk stabilisasi pasokan.
• Komoditas PALING STABIL: {bottom['label']} dengan indeks {bottom['indeks_volatilitas']}%
  Rentang harga: Rp {bottom['harga_min']:,.0f} - Rp {bottom['harga_max']:,.0f}
  Stabilitas ini mengindikasikan rantai pasok yang baik dan intervensi yang efektif.
• Rekomendasi: Komoditas dengan indeks volatilitas > 10% sebaiknya mendapat
  prioritas intervensi harga dan buffer stok nasional.
""")
# Query Spark SQL untuk tren harga per hari
query_tren = """
SELECT
    komoditas,
    label,
    hari,
    ROUND(AVG(harga), 2)   AS harga_rata2,
    ROUND(MIN(harga), 2)   AS harga_min,
    ROUND(MAX(harga), 2)   AS harga_max,
    COUNT(*)               AS jumlah_observasi
FROM harga_pangan
WHERE komoditas IS NOT NULL AND harga IS NOT NULL
GROUP BY komoditas, label, hari
ORDER BY komoditas, hari
"""
df_tren = spark.sql(query_tren)
print("\n📊 Rata-rata Harga per Komoditas per Hari:")
df_tren.show(50, truncate=False)
# Query untuk perubahan harga antar periode (jika multi-hari)
query_perubahan = """
SELECT
    komoditas,
    label,
    ROUND(AVG(harga), 2) AS harga_rata2_keseluruhan,
    ROUND(MIN(harga), 2) AS harga_terendah_sepanjang,
    ROUND(MAX(harga), 2) AS harga_tertinggi_sepanjang,
    COUNT(DISTINCT hari) AS jumlah_hari,
    COUNT(*) AS total_observasi
FROM harga_pangan
WHERE komoditas IS NOT NULL
GROUP BY komoditas, label
ORDER BY harga_rata2_keseluruhan DESC
"""
df_ringkasan_tren = spark.sql(query_perubahan)
print("📊 Ringkasan Tren Harga Keseluruhan:")
df_ringkasan_tren.show(truncate=False)
# --- Narasi Interpretasi Bisnis ---
print("Interpretasi ANALISIS 2:")
tren_rows = df_ringkasan_tren.collect()
if tren_rows:
    termahal = tren_rows[0]
    termurah = tren_rows[-1]
    print(f"""
Analisis tren harga berdasarkan data dari {termahal['jumlah_hari']} hari pengamatan:
• Komoditas TERMAHAL rata-rata: {termahal['label']}
  Harga rata-rata: Rp {termahal['harga_rata2_keseluruhan']:,.0f}
  dengan {termahal['total_observasi']} titik data terekam.
• Komoditas TERMURAH rata-rata: {termurah['label']}
  Harga rata-rata: Rp {termurah['harga_rata2_keseluruhan']:,.0f}
• Tren ini membantu Bulog dalam merencanakan anggaran pengadaan dan
  menentukan timing intervensi pasar yang optimal. Data per-periode
  memungkinkan deteksi pola musiman dan anomali harga dini.
""")
komoditas_patterns = {
    "beras": "beras",
    "jagung": "jagung",
    "kedelai": "kedelai",
    "gula": "gula",
    "minyak_goreng": "minyak",
    "cabai": "cabai",
    "bawang_merah": "bawang",
    "telur": "telur"
}
# Buat kolom flag untuk setiap komoditas yang disebut di title
title_col = "title" if "title" in df_rss.columns else df_rss.columns[0]
df_berita_tagged = df_rss
for kom, keyword in komoditas_patterns.items():
    df_berita_tagged = df_berita_tagged.withColumn(
        f"sebut_{kom}",
        F.when(F.lower(F.col(title_col)).contains(keyword), 1).otherwise(0)
    )
# Hitung frekuensi sebutan per komoditas
sebutan_data = []
for kom in KOMODITAS:
    count_val = df_berita_tagged.agg(F.sum(f"sebut_{kom}")).collect()[0][0] or 0
    sebutan_data.append((kom, komoditas_patterns[kom], int(count_val)))
df_sebutan = spark.createDataFrame(sebutan_data, ["komoditas", "keyword", "jumlah_sebutan"])
df_sebutan.createOrReplaceTempView("sebutan_berita")
# Gabungkan dengan data volatilitas menggunakan Spark SQL
df_volatilitas.createOrReplaceTempView("volatilitas")
query_korelasi = """
SELECT
    v.komoditas,
    v.label,
    v.indeks_volatilitas,
    v.harga_avg,
    s.jumlah_sebutan,
    CASE
        WHEN s.jumlah_sebutan > 5 AND v.indeks_volatilitas > 5
            THEN 'TINGGI — berita banyak & harga bergejolak'
        WHEN s.jumlah_sebutan > 5 AND v.indeks_volatilitas <= 5
            THEN 'MODERAT — banyak diberitakan tapi harga stabil'
        WHEN s.jumlah_sebutan <= 5 AND v.indeks_volatilitas > 5
            THEN 'WASPADA — harga bergejolak tapi minim liputan'
        ELSE 'RENDAH — stabil dan minim berita'
    END AS status_korelasi
FROM volatilitas v
JOIN sebutan_berita s ON v.komoditas = s.komoditas
ORDER BY s.jumlah_sebutan DESC, v.indeks_volatilitas DESC
"""
df_korelasi = spark.sql(query_korelasi)
print("\n📊 Korelasi Sebutan Berita vs Volatilitas Harga:")
df_korelasi.show(truncate=False)
# Tampilkan total berita
total_berita = df_rss.count()
print(f"Total artikel berita RSS yang dianalisis: {total_berita}")
# --- Narasi Interpretasi Bisnis ---
print("\n📝 NARASI INTERPRETASI ANALISIS 3:")
kor_rows = df_korelasi.collect()
if kor_rows:
    paling_disebut = kor_rows[0]
    waspada = [r for r in kor_rows if "WASPADA" in r["status_korelasi"]]
    print(f"""
Analisis korelasi berita terhadap {total_berita} artikel RSS:
• Komoditas PALING BANYAK diberitakan: {paling_disebut['label']}
  Disebut {paling_disebut['jumlah_sebutan']}x dalam judul berita
  dengan indeks volatilitas {paling_disebut['indeks_volatilitas']}%.
  Status: {paling_disebut['status_korelasi']}
""")
    if waspada:
        print("⚠️  PERINGATAN — Komoditas berikut bergejolak TANPA liputan media:")
        for w in waspada:
            print(f"   • {w['label']}: volatilitas {w['indeks_volatilitas']}%, hanya {w['jumlah_sebutan']}x disebut")
        print("   → Ini bisa menjadi blind-spot bagi pengambil kebijakan.\n")
    print("""• Insight: Korelasi positif antara frekuensi pemberitaan dan volatilitas
  menunjukkan media cukup responsif terhadap gejolak harga. Namun komoditas
  dengan status 'WASPADA' perlu mendapat perhatian ekstra karena fluktuasi
  harganya tidak terdeteksi oleh media publik.
""")
# Simpan ke HDFS dalam format JSON
df_volatilitas.coalesce(1).write.mode("overwrite").json(HDFS_HASIL_PATH + "volatilitas")
print("  → Volatilitas tersimpan ke HDFS: /data/pangan/hasil/volatilitas/")
df_tren.coalesce(1).write.mode("overwrite").json(HDFS_HASIL_PATH + "tren_harga")
print("  → Tren harga tersimpan ke HDFS: /data/pangan/hasil/tren_harga/")
df_korelasi.coalesce(1).write.mode("overwrite").json(HDFS_HASIL_PATH + "korelasi_berita")
print("  → Korelasi berita tersimpan ke HDFS: /data/pangan/hasil/korelasi_berita/")
# Konversi ke Pandas lalu simpan sebagai JSON lokal
spark_results = {
    "metadata": {
        "generated_by": "Theodorus Aaron Ugraha (Anggota 4)",
        "pipeline": "HargaPangan-Kelompok6",
        "total_data_api": df_api.count(),
        "total_data_rss": df_rss.count(),
    },
    "analisis_1_volatilitas": df_volatilitas.toPandas().to_dict(orient="records"),
    "analisis_2_tren_harga": df_ringkasan_tren.toPandas().to_dict(orient="records"),
    "analisis_3_korelasi_berita": df_korelasi.toPandas().to_dict(orient="records"),
}
# Pastikan folder dashboard/data/ ada
dashboard_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard", "data")
os.makedirs(dashboard_data_dir, exist_ok=True)
output_path = os.path.join(dashboard_data_dir, "spark_results.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(spark_results, f, ensure_ascii=False, indent=2, default=str)
print(f"  → Tersimpan: {output_path}")
print(f"  • HDFS: {HDFS_HASIL_PATH}volatilitas/")
print(f"  • HDFS: {HDFS_HASIL_PATH}tren_harga/")
print(f"  • HDFS: {HDFS_HASIL_PATH}korelasi_berita/")
print(f"  • Lokal: {output_path}")

spark.stop()
print("Spark session ditutup.")
