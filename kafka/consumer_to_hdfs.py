"""
consumer_to_hdfs.py — Widi as Anggota 3: Consumer ke HDFS
Topik 8: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Fungsi:
  - Baca dari 2 Kafka topic: 'pangan-api' dan 'pangan-rss' secara paralel (threading)
  - Kumpulkan event dalam buffer selama FLUSH_INTERVAL (default: 2 menit)
  - Setiap flush: simpan buffer ke file lokal → copy ke HDFS via subprocess
  - Nama file menggunakan format timestamp: 2026-04-20_14-30.json

Strategi: Opsi A — save lokal dulu, lalu subprocess hdfs dfs -put
  (sesuai FAQ: "Cara menyimpan ke HDFS dari Python?")
"""

import json
import logging
import os
import subprocess
import threading
import time

from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

KAFKA_BROKER    = "127.0.0.1:9092"
TOPICS          = ["pangan-api", "pangan-rss"]
CONSUMER_GROUP  = "pangan-hdfs-consumer"     # group_id unik agar offset di-track Kafka
FLUSH_INTERVAL  = 2 * 60                     # 2 menit dalam detik (buffer sebelum simpan ke HDFS)
LOCAL_TMP_DIR   = "/tmp/pangan_buffer"       # direktori sementara di lokal
HDFS_BASE       = "/data/pangan"             # direktori dasar di HDFS

# Mapping topic → folder HDFS
HDFS_FOLDER = {
    "pangan-api": f"{HDFS_BASE}/api",
    "pangan-rss": f"{HDFS_BASE}/rss",
}

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
    """Buat direktori di HDFS jika belum ada."""
    result = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", folder],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        log.warning(f"[HDFS] mkdir gagal untuk {folder}: {result.stderr.strip()}")


def hdfs_put(local_path: str, hdfs_folder: str) -> bool:
    """
    Upload file lokal ke HDFS melalui namenode container.
    Return True jika berhasil.
    """
    filename = os.path.basename(local_path)

    # Step 1: copy file ke dalam container namenode
    cp_result = subprocess.run(
        ["docker", "cp", local_path, f"namenode:/tmp/{filename}"],
        capture_output=True, text=True
    )
    if cp_result.returncode != 0:
        log.error(f"[HDFS] docker cp gagal: {cp_result.stderr.strip()}")
        return False

    # Step 2: buat direktori HDFS (idempoten)
    hdfs_mkdir(hdfs_folder)

    # Step 3: put ke HDFS
    put_result = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f",
         f"/tmp/{filename}", hdfs_folder],
        capture_output=True, text=True
    )
    if put_result.returncode != 0:
        log.error(f"[HDFS] hdfs put gagal: {put_result.stderr.strip()}")
        return False

    log.info(f"[HDFS] ✓ Tersimpan: {hdfs_folder}/{filename}")
    return True


def flush_buffer_ke_hdfs(buffer: list, topic: str):
    """
    Simpan buffer event ke file JSON lokal, lalu upload ke HDFS.
    Nama file: YYYY-MM-DD_HH-MM.json (sesuai format rubrik)
    """
    if not buffer:
        return

    # Format nama file: 2026-04-20_14-30.json
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename      = f"{timestamp_str}.json"
    local_path    = os.path.join(LOCAL_TMP_DIR, topic.replace("-", "_"), filename)

    # Pastikan direktori lokal ada
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Tulis buffer ke file lokal
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(buffer, f, ensure_ascii=False, indent=2)

    log.info(f"[Buffer] {topic}: {len(buffer)} event → {local_path}")

    # Upload ke HDFS
    hdfs_folder = HDFS_FOLDER.get(topic, f"{HDFS_BASE}/unknown")
    success = hdfs_put(local_path, hdfs_folder)

    if success:
        # Hapus file lokal setelah berhasil upload
        try:
            os.remove(local_path)
        except OSError:
            pass


# ─────────────────────────────────────────────
# CONSUMER WORKER (PER TOPIC)
# ─────────────────────────────────────────────

def consumer_worker(topic: str, stop_event: threading.Event):
    """
    Worker thread untuk membaca satu Kafka topic.
    Buffer event selama FLUSH_INTERVAL, lalu flush ke HDFS.
    """
    log.info(f"[Worker] Thread dimulai untuk topic: {topic}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers  = [KAFKA_BROKER],
            auto_offset_reset  = "earliest",    # baca dari awal jika grup baru
            group_id           = CONSUMER_GROUP, # unik agar Kafka track offset
            enable_auto_commit = True,
            value_deserializer = lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms= 1000,           # timeout agar loop bisa cek stop_event
        )
    except KafkaError as e:
        log.error(f"[Worker] Gagal terhubung ke Kafka untuk topic {topic}: {e}")
        return

    buffer        = []
    last_flush_ts = time.time()

    try:
        while not stop_event.is_set():
            # Poll messages (non-blocking karena consumer_timeout_ms=1000)
            try:
                for msg in consumer:
                    buffer.append(msg.value)
                    log.info(f"[{topic}] Diterima: {json.dumps(msg.value)[:80]}...")

                    # Flush jika sudah FLUSH_INTERVAL detik
                    if time.time() - last_flush_ts >= FLUSH_INTERVAL:
                        log.info(f"[{topic}] Flush {len(buffer)} event ke HDFS...")
                        flush_buffer_ke_hdfs(buffer, topic)
                        buffer        = []
                        last_flush_ts = time.time()

                    if stop_event.is_set():
                        break

            except StopIteration:
                # consumer_timeout_ms habis, tidak ada pesan baru — cek flush
                pass

            # Flush berdasarkan waktu meski tidak ada pesan baru
            if buffer and (time.time() - last_flush_ts >= FLUSH_INTERVAL):
                log.info(f"[{topic}] Flush periodik {len(buffer)} event ke HDFS...")
                flush_buffer_ke_hdfs(buffer, topic)
                buffer        = []
                last_flush_ts = time.time()

    except Exception as e:
        log.error(f"[Worker] Error pada topic {topic}: {e}", exc_info=True)

    finally:
        # Flush sisa buffer sebelum shutdown
        if buffer:
            log.info(f"[{topic}] Flush akhir {len(buffer)} event sebelum shutdown...")
            flush_buffer_ke_hdfs(buffer, topic)
        consumer.close()
        log.info(f"[Worker] Thread selesai untuk topic: {topic}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan Monitor — consumer_to_hdfs.py           ║")
    log.info("║  Anggota 3: Consumer Kafka → HDFS                    ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker   : {KAFKA_BROKER}")
    log.info(f"Topics         : {TOPICS}")
    log.info(f"Consumer Group : {CONSUMER_GROUP}")
    log.info(f"Flush Interval : {FLUSH_INTERVAL // 60} menit")
    log.info(f"HDFS Base      : {HDFS_BASE}")

    # Buat direktori HDFS awal
    for folder in HDFS_FOLDER.values():
        hdfs_mkdir(folder)
    hdfs_mkdir(f"{HDFS_BASE}/hasil")   # untuk output Spark nanti

    # Buat direktori lokal tmp
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

    # Event untuk sinyal stop semua thread
    stop_event = threading.Event()

    # Buat satu thread per topic (paralel)
    threads = []
    for topic in TOPICS:
        t = threading.Thread(
            target=consumer_worker,
            args=(topic, stop_event),
            name=f"consumer-{topic}",
            daemon=True,
        )
        threads.append(t)
        t.start()
        log.info(f"[Main] Thread dimulai: consumer-{topic}")

    log.info(f"\n[Main] {len(threads)} consumer thread berjalan. Tekan Ctrl+C untuk berhenti.\n")

    try:
        # Tunggu semua thread
        while True:
            alive = [t for t in threads if t.is_alive()]
            if not alive:
                log.warning("[Main] Semua thread sudah berhenti.")
                break
            time.sleep(5)

    except KeyboardInterrupt:
        log.info("\n[Main] Ctrl+C diterima. Menghentikan semua consumer...")
        stop_event.set()

    # Tunggu thread selesai flush dan cleanup (max 30 detik)
    for t in threads:
        t.join(timeout=30)
        if t.is_alive():
            log.warning(f"[Main] Thread {t.name} tidak selesai dalam 30 detik.")

    log.info("[Main] Consumer shutdown selesai.")


if __name__ == "__main__":
    main()