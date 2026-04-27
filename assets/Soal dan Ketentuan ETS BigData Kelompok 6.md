# ETS Praktik: Big Data Pipeline End-to-End

## 📋 Latar Belakang

**DataPipeline.id** adalah startup konsultan data yang baru berdiri. Mereka mendapat beberapa kontrak sekaligus dari klien di berbagai industri — semuanya mengeluhkan masalah yang sama:

> *"Kami punya banyak data yang terus mengalir setiap menit — dari API, dari berita, dari sensor — tapi kami tidak tahu harus menyimpan di mana, bagaimana memprosesnya, dan bagaimana menampilkannya secara bermakna."*

CEO DataPipeline.id membentuk **8 tim Data Engineer** untuk mengerjakan 8 klien sekaligus. Setiap tim mendapat satu domain klien dan harus membangun fondasi sistem Big Data yang terdiri dari pipeline ingestion, penyimpanan terdistribusi, analisis batch, dan dashboard monitoring.

**Kelompok kalian adalah salah satu dari 8 tim tersebut.**

---

## 🏗️ Arsitektur Sistem
```
╔═══════════════════════════════════════════════════════════════════╗
║              ARSITEKTUR PIPELINE BIG DATA                         ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║  [SUMBER 1: API Real-time]         [SUMBER 2: RSS/Berita]         ║
║       │                                    │                      ║
║       ▼                                    ▼                      ║
║  ┌─────────┐                       ┌─────────┐                    ║
║  │Producer │                       │Producer │                    ║
║  │API.py   │                       │RSS.py   │                    ║
║  └────┬────┘                       └────┬────┘                    ║
║       │                                 │                         ║
║       ▼                                 ▼                         ║
║  ╔══════════════════════════════════════════╗                      ║
║  ║           APACHE KAFKA                   ║                      ║
║  ║  Topic: [tema]-api   Topic: [tema]-rss   ║                      ║
║  ╚══════════════════════╤═══════════════════╝                      ║
║                         │                                         ║
║                         ▼                                         ║
║                  ┌─────────────┐                                  ║
║                  │Consumer     │       ← membaca dari kedua topic  ║
║                  │to HDFS.py   │       ← menyimpan ke HDFS        ║
║                  └──────┬──────┘                                  ║
║                         │                                         ║
║                         ▼                                         ║
║  ╔══════════════════════════════════════════╗                      ║
║  ║         HADOOP HDFS                      ║                      ║
║  ║   /data/[tema]/api/   /data/[tema]/rss/  ║                      ║
║  ╚═════════════╤════════════════════════════╝                      ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌─────────────┐                                            ║
║         │Apache Spark │   ← baca dari HDFS, analisis, simpan hasil ║
║         │analysis.py  │                                            ║
║         └──────┬───────┘                                           ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌─────────────┐                                            ║
║         │  Dashboard  │   ← Flask, localhost:5000                  ║
║         │  (Flask)    │   ← baca hasil Spark + event terbaru       ║
║         └─────────────┘                                            ║
╚═══════════════════════════════════════════════════════════════════╝
```

**Mengapa arsitektur ini?**

| Teknologi | Peran dalam Pipeline | Alasan Dipilih |
|-----------|---------------------|----------------|
| **Apache Kafka** | *Ingestion Layer* — menerima dan menyangga data dari sumber manapun | Decouples sumber dari storage; producer tidak perlu tahu ke mana data pergi |
| **HDFS** | *Storage Layer* — menyimpan semua data secara terdistribusi | Fault-tolerant, skalabel, menjadi single source of truth |
| **Apache Spark** | *Processing Layer* — membaca HDFS, menganalisis, menghasilkan insight | Distributed processing, DataFrame + SQL API, hasil bisa disimpan kembali ke HDFS |
| **Dashboard** | *Serving Layer* — menampilkan hasil untuk pengambilan keputusan | Membuat data bermakna bagi non-engineer |

---

## 📦 Komponen yang Harus Dibangun
---

### Komponen 1 — Apache Kafka: Ingestion Layer

**Tujuan:** Kafka berfungsi sebagai pintu masuk data — semua data yang mengalir dari luar masuk melalui Kafka sebelum disimpan.

**Yang harus dibangun:**

- [ ] Setup Kafka menggunakan Docker Compose dari materi P8
- [ ] Buat **2 Kafka topic** sesuai domain:
  - Topic 1: data dari API real-time (nama: `[tema]-api`)
  - Topic 2: data dari RSS feed (nama: `[tema]-rss`)
- [ ] Buat **Producer 1** (`producer_api.py`): polling API eksternal setiap 60 detik, format data sebagai JSON, kirim ke topic API dengan **key** berdasarkan identifier data (misalnya simbol koin, kode kota, dst.)
- [ ] Buat **Producer 2** (`producer_rss.py`): polling RSS feed setiap 5 menit, parse feed menggunakan library `feedparser`, hindari duplikat dengan menyimpan ID yang sudah dikirim, kirim ke topic RSS

**Hint teknis Kafka:**
- Gunakan `kafka-python` library: `pip install kafka-python`
- Producer harus menggunakan `enable_idempotence=True` dan `acks="all"` agar tidak kehilangan data
- RSS: gunakan `feedparser.parse(url)` untuk parsing, field penting: `entry.title`, `entry.link`, `entry.summary`, `entry.published`
- Setiap event harus dalam format JSON yang konsisten (sama strukturnya), sertakan field `timestamp` di setiap event

**Cara memverifikasi Kafka berjalan:**
```bash
# Cek topic yang dibuat
docker exec -it kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Cek event masuk ke topic
docker exec -it kafka-broker kafka-console-consumer.sh \
  --topic [tema]-api --from-beginning --bootstrap-server localhost:9092
```

---

### Komponen 2 — HDFS: Storage Layer

**Tujuan:** Consumer membaca dari Kafka dan menyimpan data ke HDFS. HDFS menjadi *single source of truth* — semua data tersimpan di sini sebelum diproses Spark.

**Yang harus dibangun:**

- [ ] Setup Hadoop menggunakan Docker Compose dari materi P4
- [ ] Buat struktur direktori di HDFS:
  ```
  /data/[tema]/api/    ← tempat file JSON dari topic API
  /data/[tema]/rss/    ← tempat file JSON dari topic RSS
  /data/[tema]/hasil/  ← tempat output Spark (dibuat saat Spark berjalan)
  ```
- [ ] Buat **Consumer** (`consumer_to_hdfs.py`): baca dari kedua topic Kafka, kumpulkan event dalam buffer, setiap 2–5 menit simpan buffer ke HDFS sebagai file JSON bernama timestamp

**Hint teknis HDFS:**
- Gunakan `KafkaConsumer` dengan `group_id` unik per consumer, `auto_offset_reset="earliest"`
- Strategi penyimpanan ke HDFS (pilih salah satu):
  - **Opsi A (Mudah):** Consumer simpan ke file lokal dulu, lalu jalankan perintah `hdfs dfs -put [file_lokal] [path_hdfs]` menggunakan `subprocess.run()`
  - **Opsi B (Lebih baik):** Gunakan library `hdfs` Python (`pip install hdfs`) untuk menulis langsung ke HDFS
- Nama file di HDFS: gunakan timestamp, misalnya `2026-04-20_14-30.json`
- Gunakan threading untuk membaca 2 topic secara paralel

**Cara memverifikasi HDFS berjalan:**
```bash
# Cek isi direktori
hdfs dfs -ls -R /data/[tema]/

# Cek ukuran file
hdfs dfs -du -h /data/[tema]/api/
```

---

### Komponen 3 — Apache Spark: Processing Layer

**Tujuan:** Spark membaca semua data yang sudah tersimpan di HDFS dan menghasilkan insight bermakna. Spark **tidak** membaca dari Kafka secara langsung — semua data sudah ada di HDFS.

**Yang harus dibangun:**

- [ ] Buat `spark/analysis.py` atau `spark/analysis.ipynb`
- [ ] Spark membaca dari **HDFS** (bukan file lokal)
- [ ] **3 analisis wajib** (berbeda-beda per domain — lihat bagian Pilihan Topik)
- [ ] Gunakan kombinasi **DataFrame API** dan **Spark SQL**
- [ ] Setiap analisis harus disertai **narasi interpretasi** (bukan hanya tabel output)
- [ ] Simpan hasil ke HDFS: `hdfs dfs -ls /data/[tema]/hasil/`
- [ ] Simpan juga ringkasan sebagai `dashboard/data/spark_results.json` untuk dashboard

**Hint teknis Spark:**
- Inisialisasi SparkSession dengan koneksi ke HDFS:
  ```
  SparkSession.builder.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
  ```
- Baca file JSON dari HDFS: `spark.read.option("multiLine", True).json("hdfs://namenode:8020/data/[tema]/api/")`
- Spark akan membaca **semua file** dalam folder sekaligus — tidak perlu loop per file
- Gunakan `createOrReplaceTempView()` agar bisa query dengan Spark SQL
- Konversi hasil ke Pandas (`toPandas()`) hanya untuk menyimpan ke JSON lokal untuk dashboard

---

### Komponen 4 — Dashboard: Serving Layer

**Tujuan:** Tampilan web sederhana yang menggabungkan output Spark (data historis yang sudah dianalisis) dengan data terbaru dari consumer (event terkini dari Kafka).

**Yang harus dibangun:**

- [ ] Flask web app (`dashboard/app.py`) yang berjalan di `localhost:5000`
- [ ] **3 panel minimum** di halaman utama:
  - Panel 1: Hasil analisis Spark (baca dari `spark_results.json`)
  - Panel 2: Data live terbaru (baca dari file JSON yang diupdate consumer)
  - Panel 3: Berita/feed terbaru (baca dari file JSON yang diupdate consumer)
- [ ] Auto-refresh halaman atau data setiap 30–60 detik

**Hint teknis Dashboard:**
- Flask minimal butuh `pip install flask`
- Buat endpoint `/api/data` yang return JSON dari ketiga file, frontend-nya fetch endpoint ini
- Consumer (Komponen 2) harus menyimpan **salinan lokal** di `dashboard/data/live_api.json` dan `dashboard/data/live_rss.json` agar dashboard bisa membacanya
- Auto-refresh: gunakan `setInterval(fetch, 30000)` di JavaScript halaman HTML
- Tidak harus cantik — yang penting **fungsional** dan data nyata terlihat

---

## 🎯 Topik

### 🛒 HargaPangan: Monitor Harga Komoditas Bahan Pokok

**Skenario klien:** Tim riset Bulog yang membutuhkan sistem early warning untuk memantau fluktuasi harga bahan pokok dan menghubungkannya dengan berita ekonomi.

**Pertanyaan bisnis yang harus dijawab:**
> *"Komoditas mana yang paling bergejolak harganya hari ini, dan apakah ada berita ekonomi yang menjelaskan penyebabnya?"*

| | Detail |
|-|--------|
| **API Real-time** | Pilih salah satu: **a)** Panel Harga Badanpangan: `https://panelharga.badanpangan.go.id/` (perlu inspect endpoint via DevTools) **b)** World Bank Commodity API: `https://api.worldbank.org/v2/en/indicator/PNRG_CS?downloadformat=json` **c)** Simulator realistis berbasis data historis (jika kedua API di atas tidak bisa diakses — **wajib didokumentasikan di README**) |
| **Komoditas** | Beras, Jagung, Kedelai, Gula, Minyak Goreng, Cabai, Bawang Merah, Telur |
| **Interval polling** | Setiap 30 menit |
| **RSS Feed** | `https://rss.bisnis.com/feed/rss2/ekonomi` |
| **Backup RSS** | `https://rss.kompas.com/feed/kompas.com/money` |
| **Kafka Topic 1** | `pangan-api` — key: nama komoditas |
| **Kafka Topic 2** | `pangan-rss` — key: hash URL |
| **HDFS Path** | `/data/pangan/api/` dan `/data/pangan/rss/` |

**3 Analisis Spark Wajib:**
1. **Volatilitas harga per komoditas:** hitung `(max_price - min_price) / avg_price * 100` sebagai indeks volatilitas relatif — ranking komoditas dari paling bergejolak
2. **Rata-rata harga per komoditas per periode:** tren harga dari waktu ke waktu (groupBy komoditas + jam/hari)
3. **Sebutan komoditas di berita:** hitung kemunculan nama komoditas ("beras", "cabai", "minyak") di judul artikel RSS — korelasi antara frekuensi berita dan perubahan harga

**Fokus dashboard:** Tabel harga + indikator naik/turun · Ranking volatilitas · Berita ekonomi terbaru

---

## 📁 Struktur Repository

```
kelompok-6-ets-bigdata/
├── README.md                    ← WAJIB: dokumentasi lengkap
├── docker-compose-hadoop.yml    ← dari materi P4
├── hadoop.env                   ← dari materi P4
├── docker-compose-kafka.yml     ← dari materi P8
│
├── kafka/
│   ├── producer_api.py          ← producer untuk API real-time
│   ├── producer_rss.py          ← producer untuk RSS feed
│   └── consumer_to_hdfs.py      ← consumer yang simpan ke HDFS
│
├── spark/
│   └── analysis.ipynb           ← notebook Spark (atau .py)
│
└── dashboard/
    ├── app.py                   ← Flask app
    ├── templates/
    │   └── index.html
    ├── static/
    │   └── style.css            ← opsional
    └── data/                    ← folder ini di .gitignore
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```

**README.md wajib berisi:**
- Nama anggota kelompok + kontribusi masing-masing
- Topik yang dipilih + justifikasi singkat (mengapa menarik)
- Diagram arsitektur (boleh gambar tangan yang difoto / draw.io)
- Cara menjalankan sistem step-by-step
- Screenshot: HDFS Web UI + Kafka consumer output + Dashboard berjalan
- Tantangan terbesar yang dihadapi dan cara mengatasinya

---

## 📋 Deliverable

| # | Item | Ketentuan |
|---|------|-----------|
| 1 | **GitHub Repository** | Public, struktur folder sesuai, semua kode ada |
| 2 | **README.md** | Nama anggota, cara menjalankan, screenshot, refleksi tantangan |
| 3 | **Demo Live** | 10 menit (7 menit demo + 3 menit tanya jawab), sistem berjalan live saat demo |

> ⚠️ **Sistem harus bisa dijalankan** dari instruksi di README oleh orang lain. Demo menggunakan laptop kelompok sendiri.

---

## ⚖️ Rubrik Penilaian

> **Total: 100 poin + bonus 10 poin**

### Dimensi 1 — Apache Kafka (30 poin)

| Skor | Deskripsi |
|------|-----------|
| 27–30 | 2 topic aktif; producer API berjalan dengan interval yang benar dan event JSON-nya valid dan konsisten; producer RSS berjalan, menghindari duplikat, mengekstrak field bermakna; consumer group terdaftar; `--describe` menunjukkan LAG |
| 21–26 | Minimal 1 producer berjalan dengan baik; event valid; consumer ada tapi mungkin ada isu kecil |
| 14–20 | Kafka berjalan dan topic dibuat, tapi hanya dibuktikan via CLI; producer Python ada tapi output belum konsisten |
| 7–13 | Kafka berjalan, tapi producer minimal atau tidak berjalan otomatis |
| 0–6 | Kafka tidak berjalan atau tidak ada producer Python |

### Dimensi 2 — HDFS (25 poin)

| Skor | Deskripsi |
|------|-----------|
| 22–25 | File JSON tersimpan di `/data/[tema]/api/` dan `/data/[tema]/rss/`; `hdfs dfs -ls -R /data/[tema]/` membuktikan; Web UI screenshot di README; file dinamai dengan timestamp |
| 17–21 | Minimal 1 direktori berisi file; Hadoop berjalan dengan semua container; dokumentasi cukup |
| 11–16 | Hadoop berjalan, direktori ada, tapi consumer tidak menyimpan ke HDFS (hanya lokal) |
| 5–10 | Hadoop berjalan tapi tidak ada data yang tersimpan dari consumer |
| 0–4 | Hadoop tidak berjalan |

### Dimensi 3 — Apache Spark (30 poin)

| Skor | Deskripsi |
|------|-----------|
| 27–30 | 3 analisis wajib berjalan; Spark membaca dari HDFS (bukan file lokal); menggunakan DataFrame API dan Spark SQL; hasil tersimpan ke HDFS dan `spark_results.json`; setiap analisis ada narasi interpretasi bisnis |
| 21–26 | 2–3 analisis berjalan; HDFS digunakan sebagai sumber; interpretasi ada tapi ringkas |
| 14–20 | Spark berjalan tapi membaca dari file lokal, bukan HDFS; analisis ada |
| 7–13 | Hanya 1 analisis atau menggunakan Pandas/Python tanpa Spark |
| 0–6 | Spark tidak berjalan |

### Dimensi 4 — Dashboard (15 poin)

| Skor | Deskripsi |
|------|-----------|
| 13–15 | Flask di localhost:5000; 3 panel menampilkan data nyata dari Spark + Kafka; auto-refresh berjalan; terasa seperti sistem monitoring yang hidup |
| 9–12 | Dashboard berjalan; minimal 2 panel menampilkan data nyata |
| 5–8 | Dashboard ada tapi data statis atau hardcoded; tidak terhubung ke hasil pipeline |
| 0–4 | Tidak ada dashboard atau tidak bisa diakses |

**Bonus (kumulatif, maks 10 poin):**
- **+5 poin:** Tambahkan satu analisis menggunakan **Spark MLlib** — misal prediksi tren (Linear Regression) atau clustering (K-Means) dari data historis HDFS
- **+3 poin:** Dashboard menampilkan **grafik/chart** berbasis data Spark (Chart.js atau Plotly.js)
- **+2 poin:** Consumer menyimpan ke HDFS menggunakan **library Python langsung** (bukan subprocess)

---

## ✅ Checklist Sebelum Demo

```
KAFKA:
[ ] docker compose (Kafka) berjalan — kafka-broker aktif
[ ] kafka-topics.sh --list menampilkan 2 topic [tema]-api dan [tema]-rss
[ ] producer_api.py berjalan dan output event terlihat di terminal
[ ] producer_rss.py berjalan dan output artikel terlihat di terminal
[ ] consumer_to_hdfs.py berjalan
[ ] kafka-consumer-groups.sh --describe menampilkan consumer group

HDFS:
[ ] docker compose (Hadoop) berjalan — 4 container aktif
[ ] hdfs dfs -ls /data/[tema]/api/ menampilkan file JSON
[ ] hdfs dfs -ls /data/[tema]/rss/ menampilkan file JSON
[ ] Screenshot HDFS Web UI (localhost:9870) ada di README

SPARK:
[ ] Analisis 1 berjalan tanpa error dari HDFS
[ ] Analisis 2 berjalan (Spark SQL)
[ ] Analisis 3 berjalan
[ ] hdfs dfs -ls /data/[tema]/hasil/ menampilkan output Spark
[ ] dashboard/data/spark_results.json ada

DASHBOARD:
[ ] python dashboard/app.py berjalan
[ ] localhost:5000 bisa dibuka di browser
[ ] Panel data Spark menampilkan data nyata (bukan placeholder)
[ ] Panel data live menampilkan event terbaru
[ ] Panel berita menampilkan artikel terbaru
[ ] Auto-refresh terbukti berjalan

REPOSITORY:
[ ] GitHub repo public
[ ] Semua file kode ada (tidak ada file yang "lupa di-push")
[ ] README berisi nama anggota + kontribusi + cara menjalankan + screenshot
[ ] Link repository sudah dikirim ke LMS sebelum deadline
```

---

## ❓ FAQ

**Q: API saya kena rate limit atau perlu berbayar. Apa yang harus dilakukan?**
> Buat **simulator data** yang menghasilkan angka realistis berdasarkan distribusi statistik (mean ± std) dari dataset publik yang relevan. Simulator ini tetap harus berjalan sebagai producer dan mengirim ke Kafka. Dokumentasikan pendekatan ini di README — nilai tidak dikurangi jika didokumentasikan dengan baik.

**Q: Boleh pakai Google Colab untuk Spark?**
> Boleh, tapi Spark harus membaca dari HDFS yang berjalan di Docker. Jika koneksi Colab ke HDFS lokal sulit, Spark boleh membaca file lokal sebagai alternatif, tapi catat keterbatasan ini di README. Nilai Dimensi 3 maks dikurangi 5 poin untuk kasus ini.

**Q: Bagaimana cara menyimpan ke HDFS dari Python?**
> Dua cara: **(a)** Simpan ke file lokal dulu, lalu jalankan `subprocess.run(["hdfs", "dfs", "-put", file_lokal, hdfs_path])` dari Python. **(b)** Gunakan library `hdfs` Python (`pip install hdfs`) dan `InsecureClient("http://localhost:9870")`. Cara (a) lebih mudah diimplementasikan.

**Q: Pembagian tugas yang disarankan?**

| Anggota | Tanggung Jawab |
|---------|----------------|
| Anggota 1 | Setup Docker (Hadoop & Kafka), buat topic, troubleshooting infrastruktur |
| Anggota 2 | `producer_api.py` — integrasi API eksternal |
| Anggota 3 | `producer_rss.py` + `consumer_to_hdfs.py` |
| Anggota 4 | `spark/analysis.ipynb` — 3 analisis wajib |
| Anggota 5 | `dashboard/app.py` + `index.html` |