"""
producer_rss.py — Producer RSS Feed Google News
Topik 8: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Fungsi:
  - Polling RSS feed Google News untuk "harga bahan pangan" setiap 5 menit
  - Parse RSS feed menggunakan feedparser
  - Extract: title, description, link, pubDate
  - Hindari duplikat dengan menyimpan link yang sudah dikirim
  - Kirim ke Kafka topic 'pangan-rss' sebagai JSON

Kafka Topic  : pangan-rss
Key          : MD5 dari entry.link (untuk deduplication)
Polling      : setiap 5 menit (300 detik)

Google News RSS URL (live & real-time):
https://news.google.com/rss/search?q=harga+bahan+pangan&hl=id&gl=ID&ceid=ID:id
"""

import json
import time
import hashlib
import logging
import feedparser
import requests

from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

KAFKA_BROKER   = "127.0.0.1:9092"
KAFKA_TOPIC    = "pangan-rss"
POLL_INTERVAL  = 5 * 60   # 5 menit dalam detik
REQUEST_TIMEOUT = 10       # timeout request HTTP

# Google News RSS URL - real-time news untuk "harga bahan pangan"
RSS_FEED_URL = "https://news.google.com/rss/search?q=harga+bahan+pangan&hl=id&gl=ID&ceid=ID:id"

# Kata kunci komoditas untuk tagging (opsional)
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
    if not text:
        return "umum"
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
    try:
        if hasattr(entry, "published"):
            return entry.published
        if hasattr(entry, "updated"):
            return entry.updated
    except:
        pass
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def buat_message(entry, feed_url: str) -> dict:
    """
    Bangun payload JSON dari satu entry RSS.
    Field: title, description, link, published, komoditas, timestamp.
    """
    import re
    
    title   = getattr(entry, "title",   "").strip()
    link    = getattr(entry, "link",    "").strip()
    
    # Ambil summary/description dan bersihkan HTML tags
    summary = getattr(entry, "summary", "").strip()
    summary_clean = re.sub(r"<[^>]+>", "", summary).strip()

    komoditas = detect_komoditas(title + " " + summary_clean)

    return {
        # Identitas berita
        "title"       : title,
        "description" : summary_clean[:500],   # batasi panjang
        "link"        : link,
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
                acks                = "all",
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
    Loop utama producer RSS Google News.
    - Poll feed setiap POLL_INTERVAL detik
    - Deduplication menggunakan set link yang sudah dikirim
    - Setiap entry baru dikirim ke Kafka dengan key = MD5(link)
    """
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan Monitor — producer_rss.py               ║")
    log.info("║  Google News RSS Feed Polling                        ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker    : {KAFKA_BROKER}")
    log.info(f"Kafka Topic     : {KAFKA_TOPIC}")
    log.info(f"Poll Interval   : {POLL_INTERVAL // 60} menit")
    log.info(f"RSS Feed URL    : {RSS_FEED_URL[:80]}...")

    producer = buat_producer()

    # Set untuk menyimpan link yang sudah dikirim (deduplication)
    sent_links: set = set()

    polling_ke = 0

    # Custom User-Agent untuk menghindari blocking
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        while True:
            polling_ke += 1
            waktu_mulai = datetime.now()
            log.info(f"\n{'═'*60}")
            log.info(f"POLLING #{polling_ke} — {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}")
            log.info(f"{'═'*60}")

            total_baru  = 0
            total_duplikat = 0

            try:
                # Fetch RSS feed dengan timeout
                response = requests.get(RSS_FEED_URL, headers=headers, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                
                # Parse RSS feed
                feed = feedparser.parse(response.content)

                if feed.bozo:
                    log.warning(f"⚠ Feed warning: {feed.bozo_exception}")

                log.info(f"  Ditemukan {len(feed.entries)} entries dalam feed")

                for entry in feed.entries:
                    # Gunakan link sebagai ID unik
                    entry_link = getattr(entry, "link", None) or ""
                    
                    if not entry_link:
                        log.warning(f"  ⚠ Skipped entry tanpa link")
                        continue

                    # Skip jika sudah pernah dikirim (deduplication)
                    if entry_link in sent_links:
                        total_duplikat += 1
                        continue

                    # Bangun dan kirim message
                    msg = buat_message(entry, RSS_FEED_URL)
                    key = hashlib.md5(entry_link.encode()).hexdigest()

                    try:
                        future   = producer.send(KAFKA_TOPIC, key=key, value=msg)
                        metadata = future.get(timeout=10)
                        sent_links.add(entry_link)
                        total_baru += 1
                        log.info(
                            f"  ✓ [{msg['komoditas']:15s}] {msg['title'][:50]}"
                            f" → offset={metadata.offset}"
                        )
                    except KafkaError as e:
                        log.error(f"  ✗ Gagal kirim ke Kafka: {e}")

            except requests.RequestException as e:
                log.error(f"  [Error] Gagal fetch RSS feed: {e}")
            except Exception as e:
                log.error(f"  [Error] Gagal parse feed: {e}")

            producer.flush()
            log.info(f"\nRingkasan polling #{polling_ke}:")
            log.info(f"  Entry baru dikirim : {total_baru}")
            log.info(f"  Duplikat diabaikan : {total_duplikat}")
            log.info(f"  Total link terindeks : {len(sent_links)}")

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
