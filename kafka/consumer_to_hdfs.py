"""
consumer_to_hdfs.py — Adiwidya Budi P (Anggota 3): Consumer Kafka → HDFS + Dashboard
Topik: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Alur data:
  Kafka (pangan-api + pangan-rss)
      → buffer 2 menit
      → HDFS /data/pangan/api/ dan /data/pangan/rss/
      → dashboard/data/live_api.json dan live_rss.json  ← untuk Flask dashboard
"""

import json
import logging
import os
import subprocess
import threading
import time

from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPICS         = ["pangan-api", "pangan-rss"]
CONSUMER_GROUP = "pangan-hdfs-consumer"
FLUSH_INTERVAL = 2 * 60                   # 2 menit

# Path lokal sementara sebelum upload ke HDFS (use /tmp for portability)
LOCAL_TMP_DIR  = os.path.join(os.getenv("HOME", "/tmp"), ".pangan_buffer")

# Path HDFS
HDFS_BASE = "/data/pangan"
HDFS_FOLDER = {
    "pangan-api": f"{HDFS_BASE}/api",
    "pangan-rss": f"{HDFS_BASE}/rss",
}

# Path dashboard lokal — Flask membaca dari sini
DASHBOARD_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dashboard", "data"
)

# File live per topic
DASHBOARD_FILE = {
    "pangan-api": os.path.join(DASHBOARD_DATA_DIR, "live_api.json"),
    "pangan-rss": os.path.join(DASHBOARD_DATA_DIR, "live_rss.json"),
}

# Maksimal entry yang disimpan di live JSON (hindari file terlalu besar)
MAX_LIVE_ENTRIES = 200

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("consumer_hdfs")


# ─────────────────────────────────────────────
# HDFS HELPERS
# ─────────────────────────────────────────────

def hdfs_mkdir(folder: str):
    result = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", folder],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log.warning(f"[HDFS] mkdir gagal: {result.stderr.strip()}")


def hdfs_put(local_path: str, hdfs_folder: str) -> bool:
    filename = os.path.basename(local_path)

    # Step 1: copy ke container namenode
    cp = subprocess.run(
        ["docker", "cp", local_path, f"namenode:/tmp/{filename}"],
        capture_output=True, text=True
    )
    if cp.returncode != 0:
        log.error(f"[HDFS] docker cp gagal: {cp.stderr.strip()}")
        return False

    # Step 2: pastikan folder ada
    hdfs_mkdir(hdfs_folder)

    # Step 3: put ke HDFS
    put = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs",
         "-put", "-f", f"/tmp/{filename}", hdfs_folder],
        capture_output=True, text=True
    )
    if put.returncode != 0:
        log.error(f"[HDFS] put gagal: {put.stderr.strip()}")
        return False

    log.info(f"[HDFS] ✓ {hdfs_folder}/{filename}")
    return True


# ─────────────────────────────────────────────
# DASHBOARD HELPER — KUNCI KONEKSI BACKEND↔FRONTEND
# ─────────────────────────────────────────────

def simpan_live_dashboard(buffer: list, topic: str):
    """
    Simpan salinan lokal terbaru ke dashboard/data/ agar Flask bisa membacanya.

    Format yang diharapkan frontend:
      live_api.json → list of: {komoditas, label, harga, satuan, wilayah,
                                 perubahan_pct, sumber, tanggal, timestamp_iso}
      live_rss.json → list of: {title, link, summary, komoditas, timestamp,
                                 source_feed, published}

    Entry diurutkan: terbaru di akhir (frontend mengambil dari belakang).
    """
    os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)
    path = DASHBOARD_FILE.get(topic)
    if not path:
        return

    # Baca data lama
    try:
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        if not isinstance(existing, list):
            existing = []
    except (FileNotFoundError, json.JSONDecodeError):
        existing = []

    # Filter buffer — hanya ambil field yang relevan untuk frontend
    filtered = []
    for item in buffer:
        if not isinstance(item, dict):
            continue
        if topic == "pangan-api":
            filtered.append({
                "komoditas"     : item.get("komoditas", ""),
                "label"         : item.get("label", ""),
                "harga"         : item.get("harga", 0),
                "satuan"        : item.get("satuan", "kg"),
                "wilayah"       : item.get("wilayah", "Nasional"),
                "harga_acuan"   : item.get("harga_acuan"),
                "perubahan_pct" : item.get("perubahan_pct"),
                "sumber"        : item.get("sumber", "simulator"),
                "tanggal"       : item.get("tanggal", ""),
                "jam"           : item.get("jam", ""),
                "timestamp_iso" : item.get("timestamp_iso", datetime.now().isoformat()),
            })
        elif topic == "pangan-rss":
            filtered.append({
                "title"       : item.get("title", ""),
                "link"        : item.get("link", "#"),
                "summary"     : item.get("summary", "")[:300],
                "komoditas"   : item.get("komoditas", "umum"),
                "timestamp"   : item.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "published"   : item.get("published", ""),
                "source_feed" : item.get("source_feed", ""),
            })

    # Gabungkan & batasi ukuran
    combined = existing + filtered
    combined = combined[-MAX_LIVE_ENTRIES:]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"[Dashboard] ✓ {os.path.basename(path)} diperbarui ({len(combined)} entry)")


# ─────────────────────────────────────────────
# FLUSH: HDFS + DASHBOARD (DIPANGGIL TIAP 2 MENIT)
# ─────────────────────────────────────────────

def flush_buffer(buffer: list, topic: str):
    """Simpan buffer ke HDFS dan perbarui file live dashboard."""
    if not buffer:
        return

    # ── 1. Simpan ke file lokal sementara ──
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename      = f"{timestamp_str}.json"
    local_dir     = os.path.join(LOCAL_TMP_DIR, topic.replace("-", "_"))
    local_path    = os.path.join(local_dir, filename)
    os.makedirs(local_dir, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(buffer, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"[Buffer] {topic}: {len(buffer)} event → {local_path}")

    # ── 2. Upload ke HDFS ──
    hdfs_folder = HDFS_FOLDER.get(topic, f"{HDFS_BASE}/unknown")
    success = hdfs_put(local_path, hdfs_folder)
    if success:
        try:
            os.remove(local_path)
        except OSError:
            pass

    # ── 3. Perbarui live JSON untuk Flask dashboard ──
    simpan_live_dashboard(buffer, topic)


# ─────────────────────────────────────────────
# CONSUMER WORKER (1 THREAD PER TOPIC)
# ─────────────────────────────────────────────

def consumer_worker(topic: str, stop_event: threading.Event):
    log.info(f"[Worker] Memulai thread untuk topic: {topic}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers   = [KAFKA_BROKER],
            auto_offset_reset   = "earliest",
            group_id            = CONSUMER_GROUP,
            enable_auto_commit  = True,
            value_deserializer  = lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms = 1000,
        )
    except KafkaError as e:
        log.error(f"[Worker] Gagal konek Kafka untuk {topic}: {e}")
        return

    buffer        = []
    last_flush_ts = time.time()

    try:
        while not stop_event.is_set():
            try:
                for msg in consumer:
                    buffer.append(msg.value)
                    log.info(
                        f"[{topic}] Diterima ({len(buffer)}): "
                        f"{str(msg.value)[:80]}..."
                    )

                    # Flush jika waktu sudah cukup
                    if time.time() - last_flush_ts >= FLUSH_INTERVAL:
                        log.info(f"[{topic}] Flush {len(buffer)} event...")
                        flush_buffer(buffer, topic)
                        buffer        = []
                        last_flush_ts = time.time()

                    if stop_event.is_set():
                        break

            except StopIteration:
                pass

            # Flush periodik meskipun tidak ada pesan baru
            if buffer and (time.time() - last_flush_ts >= FLUSH_INTERVAL):
                log.info(f"[{topic}] Flush periodik {len(buffer)} event...")
                flush_buffer(buffer, topic)
                buffer        = []
                last_flush_ts = time.time()

    except Exception as e:
        log.error(f"[Worker] Error pada topic {topic}: {e}", exc_info=True)

    finally:
        # Flush sisa buffer saat shutdown
        if buffer:
            log.info(f"[{topic}] Flush akhir {len(buffer)} event sebelum shutdown...")
            flush_buffer(buffer, topic)
        consumer.close()
        log.info(f"[Worker] Thread selesai: {topic}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan — consumer_to_hdfs.py                   ║")
    log.info("║  Kafka → HDFS + Dashboard Live Files                 ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker     : {KAFKA_BROKER}")
    log.info(f"Topics           : {TOPICS}")
    log.info(f"Consumer Group   : {CONSUMER_GROUP}")
    log.info(f"Flush Interval   : {FLUSH_INTERVAL // 60} menit")
    log.info(f"HDFS Base        : {HDFS_BASE}")
    log.info(f"Dashboard Dir    : {DASHBOARD_DATA_DIR}")

    # Buat folder HDFS awal
    for folder in HDFS_FOLDER.values():
        hdfs_mkdir(folder)
    hdfs_mkdir(f"{HDFS_BASE}/hasil")

    # Buat folder lokal
    os.makedirs(LOCAL_TMP_DIR,    exist_ok=True)
    os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)

    stop_event = threading.Event()

    threads = []
    for topic in TOPICS:
        t = threading.Thread(
            target = consumer_worker,
            args   = (topic, stop_event),
            name   = f"consumer-{topic}",
            daemon = True,
        )
        threads.append(t)
        t.start()
        log.info(f"[Main] Thread dimulai: consumer-{topic}")

    log.info(f"\n[Main] {len(threads)} thread berjalan. Ctrl+C untuk berhenti.\n")

    try:
        while True:
            alive = [t for t in threads if t.is_alive()]
            if not alive:
                log.warning("[Main] Semua thread berhenti.")
                break
            time.sleep(5)
    except KeyboardInterrupt:
        log.info("\n[Main] Ctrl+C — menghentikan semua consumer...")
        stop_event.set()

    for t in threads:
        t.join(timeout=30)

    log.info("[Main] Shutdown selesai.")


if __name__ == "__main__":
    main()
