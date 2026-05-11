"""
consumer_splitter.py — Smart Data Splitter untuk Dashboard
Topik 8: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Fungsi:
  - Baca data dari 2 Kafka topic: 'pangan-api' dan 'pangan-rss'
  - Split data berdasarkan topic dan keperluan:
    1. live_api.json      → Raw API data untuk tren harga (dashboard)
    2. live_rss.json      → News headlines + links (dashboard display)
    3. spark_input.json   → Combined data untuk Spark analysis
  - Write ke local files setiap FLUSH_INTERVAL (1 menit)

Strategi: Stream consumer dengan in-memory buffer per topic
  Buffer disimpan setiap flush ke local file (untuk dashboard pickup)
  Spark akan membaca dari HDFS (data persisten)
"""

import json
import logging
import os
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
CONSUMER_GROUP  = "pangan-splitter-group"
FLUSH_INTERVAL  = 60  # 1 menit - flush ke file lokal

# Output files untuk dashboard (local filesystem)
OUTPUT_DIR      = "../dashboard/data"
LIVE_API_FILE   = os.path.join(OUTPUT_DIR, "live_api.json")
LIVE_RSS_FILE   = os.path.join(OUTPUT_DIR, "live_rss.json")
SPARK_INPUT_FILE= os.path.join(OUTPUT_DIR, "spark_input.json")

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("consumer_splitter")


# ─────────────────────────────────────────────
# BUFFER & FILE OPERATIONS
# ─────────────────────────────────────────────

class DataSplitter:
    """Smart splitter untuk membagi data ke 3 output stream."""
    
    def __init__(self):
        self.buffer_api = []
        self.buffer_rss = []
        self.buffer_spark = []
        self.lock = threading.Lock()
        self.last_flush = datetime.now()

    def ensure_output_dir(self):
        """Pastikan output directory ada."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def add_message(self, topic: str, message: dict):
        """Tambah message ke buffer sesuai topic."""
        with self.lock:
            if topic == "pangan-api":
                self.buffer_api.append(message)
                # Juga masukkan ke spark buffer untuk analysis
                self.buffer_spark.append({
                    "type": "price",
                    "data": message,
                    "timestamp": datetime.now().isoformat()
                })
            elif topic == "pangan-rss":
                self.buffer_rss.append(message)
                # Juga masukkan ke spark buffer
                self.buffer_spark.append({
                    "type": "news",
                    "data": message,
                    "timestamp": datetime.now().isoformat()
                })

    def should_flush(self) -> bool:
        """Cek apakah sudah waktunya flush."""
        elapsed = (datetime.now() - self.last_flush).total_seconds()
        return elapsed >= FLUSH_INTERVAL

    def flush(self):
        """Flush all buffers ke local files."""
        with self.lock:
            if not self.buffer_api and not self.buffer_rss and not self.buffer_spark:
                return
            
            waktu = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Flush live_api.json
            if self.buffer_api:
                live_api_data = {
                    "topic": "pangan-api",
                    "timestamp": waktu,
                    "count": len(self.buffer_api),
                    "data": self.buffer_api[-10:]  # Keep recent 10 entries
                }
                try:
                    with open(LIVE_API_FILE, "w", encoding="utf-8") as f:
                        json.dump(live_api_data, f, ensure_ascii=False, indent=2)
                    log.info(f"✓ Flush live_api.json ({len(self.buffer_api)} entries)")
                except Exception as e:
                    log.error(f"✗ Gagal write live_api.json: {e}")

            # Flush live_rss.json
            if self.buffer_rss:
                live_rss_data = {
                    "topic": "pangan-rss",
                    "timestamp": waktu,
                    "count": len(self.buffer_rss),
                    "data": self.buffer_rss[-10:]  # Keep recent 10 news
                }
                try:
                    with open(LIVE_RSS_FILE, "w", encoding="utf-8") as f:
                        json.dump(live_rss_data, f, ensure_ascii=False, indent=2)
                    log.info(f"✓ Flush live_rss.json ({len(self.buffer_rss)} news)")
                except Exception as e:
                    log.error(f"✗ Gagal write live_rss.json: {e}")

            # Flush spark_input.json (all combined)
            if self.buffer_spark:
                spark_input_data = {
                    "timestamp": waktu,
                    "total_records": len(self.buffer_spark),
                    "data": self.buffer_spark
                }
                try:
                    with open(SPARK_INPUT_FILE, "w", encoding="utf-8") as f:
                        json.dump(spark_input_data, f, ensure_ascii=False, indent=2)
                    log.info(f"✓ Flush spark_input.json ({len(self.buffer_spark)} records)")
                except Exception as e:
                    log.error(f"✗ Gagal write spark_input.json: {e}")

            self.last_flush = datetime.now()

    def get_stats(self) -> dict:
        """Return buffer statistics."""
        with self.lock:
            return {
                "api_buffer": len(self.buffer_api),
                "rss_buffer": len(self.buffer_rss),
                "spark_buffer": len(self.buffer_spark),
            }


# ─────────────────────────────────────────────
# KAFKA CONSUMER
# ─────────────────────────────────────────────

def buat_consumer() -> KafkaConsumer:
    """Inisialisasi Kafka consumer."""
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers   = [KAFKA_BROKER],
                group_id            = CONSUMER_GROUP,
                value_deserializer  = lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset   = "latest",  # Ambil dari latest, not from beginning
                enable_auto_commit  = True,
                max_poll_records    = 100,
                session_timeout_ms  = 30_000,
            )
            log.info(f"[Kafka] Consumer terhubung ke {KAFKA_BROKER}")
            return consumer
        except KafkaError as e:
            log.warning(f"[Kafka] Percobaan {attempt}/5 gagal: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError("[Kafka] Tidak bisa terhubung setelah 5 percobaan.")


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

def jalankan_splitter():
    """Main loop: consume messages dan split ke 3 outputs."""
    log.info("╔═══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan Monitor — consumer_splitter.py           ║")
    log.info("║  Smart Data Splitter untuk Dashboard & Spark         ║")
    log.info("╚═══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker    : {KAFKA_BROKER}")
    log.info(f"Kafka Topics    : {', '.join(TOPICS)}")
    log.info(f"Consumer Group  : {CONSUMER_GROUP}")
    log.info(f"Flush Interval  : {FLUSH_INTERVAL} detik")
    log.info(f"Output Dir      : {OUTPUT_DIR}")

    splitter = DataSplitter()
    splitter.ensure_output_dir()
    
    consumer = buat_consumer()

    # Thread untuk periodic flush
    def flush_worker():
        while True:
            if splitter.should_flush():
                splitter.flush()
            time.sleep(5)  # Check every 5 seconds

    flush_thread = threading.Thread(target=flush_worker, daemon=True)
    flush_thread.start()

    msg_count = 0
    last_stats_time = datetime.now()

    try:
        log.info("✓ Listening for messages...")
        for message in consumer:
            msg_count += 1
            topic = message.topic
            value = message.value

            # Log incoming message
            title = value.get("title", "")[:50] or value.get("komoditas", "N/A")
            log.debug(f"[{topic}] {title}... (offset={message.offset})")

            # Add ke buffer
            splitter.add_message(topic, value)

            # Print stats setiap 30 detik
            now = datetime.now()
            if (now - last_stats_time).total_seconds() >= 30:
                stats = splitter.get_stats()
                log.info(
                    f"📊 Stats: API buffer={stats['api_buffer']}, "
                    f"RSS buffer={stats['rss_buffer']}, "
                    f"Spark buffer={stats['spark_buffer']}, "
                    f"Total messages={msg_count}"
                )
                last_stats_time = now

    except KeyboardInterrupt:
        log.info("\n[Shutdown] Consumer dihentikan oleh pengguna (Ctrl+C)")
    finally:
        # Final flush
        splitter.flush()
        consumer.close()
        log.info("[Shutdown] Kafka consumer ditutup.")
        log.info(f"Total messages processed: {msg_count}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    jalankan_splitter()
