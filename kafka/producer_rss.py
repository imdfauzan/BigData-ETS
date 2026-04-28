"""
producer_rss.py — Widi as Anggota 3: Producer RSS Feed
Topik 8: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Fungsi:
  - Polling RSS feed berita pangan setiap 5 menit
  - Parse feed menggunakan library feedparser
  - Hindari duplikat dengan menyimpan entry ID/link yang sudah dikirim
  - Kirim ke Kafka topic 'pangan-rss' dengan key berdasarkan hash URL

Kafka Topic  : pangan-rss
Key          : MD5 dari entry.link
Polling      : setiap 5 menit (300 detik)
"""

import json
import time
import hashlib
import logging
import feedparser

from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

KAFKA_BROKER   = "127.0.0.1:9092"
KAFKA_TOPIC    = "pangan-rss"
POLL_INTERVAL  = 5 * 60   # 5 menit dalam detik

# Daftar RSS feed berita komoditas pangan Indonesia
# Gunakan beberapa sumber agar data lebih kaya
RSS_FEEDS = [
    "https://www.kontan.co.id/rss/bisnis",            # bisnis/komoditas
    "https://ekonomi.bisnis.com/feed/rss",            # ekonomi bisnis
    "https://rss.detik.com/index.php/detikfinance",   # detikfinance
    "https://katadata.co.id/feed",                    # katadata
]

# Kata kunci komoditas yang dipantau (untuk filter berita relevan)
KOMODITAS_KEYWORDS = {
    "beras"        : ["beras", "padi", "gabah"],
    "jagung"       : ["jagung"],
    "kedelai"      : ["kedelai", "tempe", "tahu"],
    "gula"         : ["gula", "tebu"],
    "minyak_goreng": ["minyak goreng", "minyak sawit", "CPO"],
    "cabai"        : ["cabai", "cabe"],
    "bawang_merah" : ["bawang merah", "bawang"],
    "telur"        : ["telur ayam", "telur"],
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_rss")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def detect_komoditas(text: str) -> str:
    """
    Deteksi komoditas yang dibahas dalam teks berita.
    Return nama komoditas pertama yang ditemukan, atau 'umum' jika tidak ada.
    """
    text_lower = text.lower()
    for komoditas, keywords in KOMODITAS_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                return komoditas
    return "umum"


def parse_published(entry) -> str:
    """
    Ambil waktu publikasi dari entry feedparser.
    Fallback ke waktu sekarang jika tidak tersedia.
    """
    if hasattr(entry, "published"):
        return entry.published
    if hasattr(entry, "updated"):
        return entry.updated
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def buat_message(entry, feed_url: str) -> dict:
    """
    Bangun payload JSON dari satu entry RSS.
    Field mengikuti spesifikasi: title, link, summary, published, komoditas, timestamp.
    """
    title   = getattr(entry, "title",   "").strip()
    link    = getattr(entry, "link",    "").strip()
    summary = getattr(entry, "summary", "").strip()

    # Bersihkan HTML tag dari summary jika ada
    import re
    summary_clean = re.sub(r"<[^>]+>", "", summary).strip()

    komoditas = detect_komoditas(title + " " + summary_clean)

    return {
        # Identitas berita
        "title"       : title,
        "link"        : link,
        "summary"     : summary_clean[:500],   # batasi panjang
        "published"   : parse_published(entry),
        "source_feed" : feed_url,

        # Klasifikasi
        "komoditas"   : komoditas,

        # Metadata pipeline
        "timestamp"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "topic"       : KAFKA_TOPIC,
    }


# ─────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────

def buat_producer() -> KafkaProducer:
    """Inisialisasi Kafka producer dengan konfigurasi reliability."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers   = [KAFKA_BROKER],
                value_serializer    = lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer      = lambda k: k.encode("utf-8"),
                acks                = "all",             # tunggu semua replica konfirmasi
                enable_idempotence  = True,              # hindari duplikat di level Kafka
                retries             = 3,
                linger_ms           = 500,
                compression_type    = "gzip",
                max_block_ms        = 10_000,
            )
            log.info(f"[Kafka] Producer terhubung ke {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            log.warning(f"[Kafka] Percobaan {attempt}/5 gagal: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError("[Kafka] Tidak bisa terhubung setelah 5 percobaan.")


# ─────────────────────────────────────────────
# LOOP UTAMA
# ─────────────────────────────────────────────

def jalankan_producer():
    """
    Loop utama producer RSS.
    - Poll semua feed setiap POLL_INTERVAL detik
    - Deduplication menggunakan set ID entry yang sudah dikirim
    - Setiap entry baru dikirim ke Kafka dengan key = MD5(link)
    """
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan Monitor — producer_rss.py               ║")
    log.info("║  Anggota 3: Producer RSS Feed                        ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker  : {KAFKA_BROKER}")
    log.info(f"Kafka Topic   : {KAFKA_TOPIC}")
    log.info(f"Poll Interval : {POLL_INTERVAL // 60} menit")
    log.info(f"RSS Feeds     : {len(RSS_FEEDS)} sumber")

    producer = buat_producer()

    # Set untuk menyimpan link yang sudah dikirim (deduplication)
    # Ini adalah IN-MEMORY store; cukup untuk satu sesi runtime.
    # Untuk persistensi lintas restart, gunakan file atau Redis.
    sent_ids: set = set()

    polling_ke = 0

    try:
        while True:
            polling_ke += 1
            waktu_mulai = datetime.now()
            log.info(f"\n{'═'*55}")
            log.info(f"POLLING #{polling_ke} — {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}")
            log.info(f"{'═'*55}")

            total_baru  = 0
            total_duplikat = 0

            for feed_url in RSS_FEEDS:
                log.info(f"  Fetching: {feed_url}")
                try:
                    feed = feedparser.parse(feed_url)

                    if feed.bozo:
                        log.warning(f"  [Warn] Feed tidak valid: {feed_url} — {feed.bozo_exception}")

                    for entry in feed.entries:
                        # Gunakan link sebagai ID unik; fallback ke title+published
                        entry_id = getattr(entry, "link", None) or \
                                   hashlib.md5(
                                       (getattr(entry, "title", "") + getattr(entry, "published", "")).encode()
                                   ).hexdigest()

                        # Skip jika sudah pernah dikirim (deduplication)
                        if entry_id in sent_ids:
                            total_duplikat += 1
                            continue

                        # Bangun dan kirim message
                        msg = buat_message(entry, feed_url)
                        key = hashlib.md5(entry_id.encode()).hexdigest()

                        try:
                            future   = producer.send(KAFKA_TOPIC, key=key, value=msg)
                            metadata = future.get(timeout=10)
                            sent_ids.add(entry_id)
                            total_baru += 1
                            log.info(
                                f"  ✓ [{msg['komoditas']:15s}] {msg['title'][:60]}"
                                f" → partition={metadata.partition} offset={metadata.offset}"
                            )
                        except KafkaError as e:
                            log.error(f"  ✗ Gagal kirim: {e} | entry: {entry_id[:40]}")

                except Exception as e:
                    log.error(f"  [Error] Gagal fetch feed {feed_url}: {e}")
                    continue

            producer.flush()
            log.info(f"\nRingkasan polling #{polling_ke}:")
            log.info(f"  Entry baru dikirim : {total_baru}")
            log.info(f"  Duplikat diabaikan : {total_duplikat}")
            log.info(f"  Total ID tersimpan : {len(sent_ids)}")

            # Hitung waktu tunggu agar interval tetap akurat
            durasi = (datetime.now() - waktu_mulai).total_seconds()
            tunggu = max(0, POLL_INTERVAL - durasi)
            log.info(f"  Polling berikutnya dalam {tunggu/60:.1f} menit...")
            time.sleep(tunggu)

    except KeyboardInterrupt:
        log.info("\n[Shutdown] Producer RSS dihentikan oleh pengguna (Ctrl+C)")
    finally:
        producer.close()
        log.info("[Shutdown] Kafka producer ditutup.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    jalankan_producer()