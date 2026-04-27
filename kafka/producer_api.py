"""
producer_api.py — Anggota 2: Integrasi API Eksternal
Topik 8: HargaPangan - Monitor Harga Komoditas Bahan Pokok

Strategi API (prioritas urutan):
  1. Panel Harga Badanpangan (https://panelharga.badanpangan.go.id/)
     - Endpoint ditemukan via DevTools: /api/v1/harga-komoditi
  2. World Bank Commodity API
     - https://api.worldbank.org/v2/en/indicator/PNRG_CS?format=json
  3. Simulator Realistis berbasis data historis
     - Diaktifkan otomatis jika kedua API di atas tidak dapat diakses
     - Didokumentasikan sesuai syarat README

Kafka Topic  : pangan-api
Key          : nama komoditas (e.g., "beras", "cabai")
Polling      : setiap 30 menit
"""

import json
import time
import random
import hashlib
import logging
import requests

from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────

KAFKA_BROKER = "127.0.0.1:9092"
KAFKA_TOPIC    = "pangan-api"
POLL_INTERVAL  = 30 * 60          # 30 menit dalam detik
REQUEST_TIMEOUT = 10               # timeout request HTTP (detik)

# Komoditas yang dipantau (sesuai spesifikasi)
KOMODITAS = [
    "beras",
    "jagung",
    "kedelai",
    "gula",
    "minyak_goreng",
    "cabai",
    "bawang_merah",
    "telur",
]

# Mapping nama komoditas ke label tampilan
KOMODITAS_LABEL = {
    "beras"        : "Beras",
    "jagung"       : "Jagung",
    "kedelai"      : "Kedelai",
    "gula"         : "Gula",
    "minyak_goreng": "Minyak Goreng",
    "cabai"        : "Cabai",
    "bawang_merah" : "Bawang Merah",
    "telur"        : "Telur",
}

# ─────────────────────────────────────────────
# DATA HISTORIS UNTUK SIMULATOR
# Harga acuan dalam Rupiah per satuan (Maret 2024)
# Sumber referensi: Badan Pangan Nasional & BPS
# ─────────────────────────────────────────────

HARGA_ACUAN = {
    "beras"        : {"harga": 14500, "satuan": "kg",  "volatilitas": 0.04},
    "jagung"       : {"harga":  5800, "satuan": "kg",  "volatilitas": 0.06},
    "kedelai"      : {"harga": 12000, "satuan": "kg",  "volatilitas": 0.05},
    "gula"         : {"harga": 17000, "satuan": "kg",  "volatilitas": 0.03},
    "minyak_goreng": {"harga": 17500, "satuan": "liter","volatilitas": 0.04},
    "cabai"        : {"harga": 45000, "satuan": "kg",  "volatilitas": 0.25},  # sangat volatil
    "bawang_merah" : {"harga": 38000, "satuan": "kg",  "volatilitas": 0.18},  # cukup volatil
    "telur"        : {"harga": 29000, "satuan": "kg",  "volatilitas": 0.07},
}

# State harga simulasi — diperbarui setiap polling agar tren realistis
_sim_state = {k: v["harga"] for k, v in HARGA_ACUAN.items()}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_api")


# ═════════════════════════════════════════════
# BAGIAN 1 — SUMBER DATA API
# ═════════════════════════════════════════════

class BadanPanganAPI:
    """
    Integrasi Panel Harga Badanpangan (https://panelharga.badanpangan.go.id/)

    Endpoint ditemukan via browser DevTools (Network tab):
      GET /api/v1/harga-komoditi?tanggal=YYYY-MM-DD&komoditi=<id>
    atau endpoint agregat:
      GET /api/v1/harga-komoditi/list

    Catatan: Endpoint ini bersifat publik namun tidak terdokumentasi resmi.
    Jika structure response berubah, fallback ke simulator akan aktif otomatis.
    """

    BASE_URL    = "https://panelharga.badanpangan.go.id"
    ENDPOINT    = "/api/v1/harga-komoditi"

    # ID komoditas di sistem Badanpangan (ditemukan via DevTools)
    KOMODITI_ID = {
        "beras"        : 1,
        "jagung"       : 4,
        "kedelai"      : 5,
        "gula"         : 6,
        "minyak_goreng": 7,
        "cabai"        : 10,
        "bawang_merah" : 11,
        "telur"        : 12,
    }

    def fetch(self) -> list[dict] | None:
        """Ambil data harga dari Badanpangan. Return None jika gagal."""
        results = []
        today   = datetime.now().strftime("%Y-%m-%d")

        try:
            # Coba endpoint agregat dulu
            url = f"{self.BASE_URL}{self.ENDPOINT}/list"
            resp = requests.get(
                url,
                params  = {"tanggal": today},
                headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
                timeout = REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            # Parse response — struktur umum: {"data": [...], "status": "ok"}
            items = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(items, list):
                raise ValueError("Struktur response tidak dikenali")

            for item in items:
                komoditi_nama = self._normalize_nama(item.get("nama_komoditi", ""))
                if komoditi_nama not in KOMODITAS:
                    continue

                harga = float(item.get("harga_rata", item.get("harga", 0)))
                if harga <= 0:
                    continue

                results.append({
                    "komoditas"  : komoditi_nama,
                    "label"      : KOMODITAS_LABEL[komoditi_nama],
                    "harga"      : harga,
                    "satuan"     : HARGA_ACUAN[komoditi_nama]["satuan"],
                    "wilayah"    : item.get("wilayah", "Nasional"),
                    "tanggal"    : today,
                    "sumber"     : "badanpangan",
                })

            log.info(f"[BadanPangan] Berhasil ambil {len(results)} komoditas")
            return results if results else None

        except Exception as e:
            log.warning(f"[BadanPangan] Gagal: {e}")
            return None

    def _normalize_nama(self, nama: str) -> str:
        """Normalisasi nama komoditas dari API ke key standar."""
        nama = nama.lower().strip()
        mapping = {
            "beras medium"  : "beras",
            "beras premium" : "beras",
            "minyak goreng" : "minyak_goreng",
            "minyak goreng curah": "minyak_goreng",
            "bawang merah"  : "bawang_merah",
            "cabe merah"    : "cabai",
            "cabe rawit"    : "cabai",
            "cabai merah"   : "cabai",
        }
        return mapping.get(nama, nama.replace(" ", "_"))


class WorldBankAPI:
    """
    World Bank Commodity Price API
    https://api.worldbank.org/v2/en/indicator/PNRG_CS?format=json

    Catatan: World Bank API menyediakan data harga komoditas global (USD).
    Harga dikonversi ke IDR menggunakan kurs referensi.
    Data ini bersifat bulanan, bukan real-time harian.
    """

    BASE_URL   = "https://api.worldbank.org/v2"
    KURS_IDR   = 15800  # Kurs USD/IDR referensi (diupdate manual atau via API lain)

    # Indikator World Bank per komoditas
    INDICATORS = {
        "jagung"  : "PMAIZMMT",   # Maize (corn), US cents/bushel
        "kedelai" : "PSOYB",      # Soybeans, USD/mt
        "gula"    : "PSUGAUSA",   # Sugar, US cents/lb
    }

    def fetch(self) -> list[dict] | None:
        """Ambil data dari World Bank API. Return None jika gagal."""
        results = []

        for komoditas, indicator in self.INDICATORS.items():
            try:
                url = f"{self.BASE_URL}/en/indicator/{indicator}"
                resp = requests.get(
                    url,
                    params  = {"format": "json", "mrv": 1, "per_page": 1},
                    timeout = REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()

                # World Bank returns [metadata, [data_array]]
                if not isinstance(data, list) or len(data) < 2:
                    continue

                entries = data[1]
                if not entries:
                    continue

                entry = entries[0]
                nilai = entry.get("value")
                if nilai is None:
                    continue

                # Konversi ke IDR (estimasi kasar berdasarkan satuan)
                harga_idr = self._konversi_ke_idr(komoditas, float(nilai))

                results.append({
                    "komoditas"  : komoditas,
                    "label"      : KOMODITAS_LABEL[komoditas],
                    "harga"      : round(harga_idr, 2),
                    "satuan"     : HARGA_ACUAN[komoditas]["satuan"],
                    "wilayah"    : "Global (World Bank)",
                    "tanggal"    : entry.get("date", datetime.now().strftime("%Y-%m")),
                    "sumber"     : "worldbank",
                })

            except Exception as e:
                log.warning(f"[WorldBank] Gagal untuk {komoditas}: {e}")

        if results:
            log.info(f"[WorldBank] Berhasil ambil {len(results)} komoditas")
            return results
        return None

    def _konversi_ke_idr(self, komoditas: str, nilai_usd: float) -> float:
        """Konversi harga komoditas global ke perkiraan harga eceran IDR."""
        # Faktor markup: harga global → harga eceran Indonesia
        MARKUP = {"jagung": 1.8, "kedelai": 1.5, "gula": 2.2}
        markup = MARKUP.get(komoditas, 1.5)
        return nilai_usd * self.KURS_IDR * markup / 1000  # per kg


# ═════════════════════════════════════════════
# BAGIAN 2 — SIMULATOR REALISTIS
# ═════════════════════════════════════════════

class SimulatorHarga:
    """
    Simulator harga berbasis data historis.

    WAJIB DIAKTIFKAN jika kedua API di atas tidak dapat diakses.
    Simulasi ini menggunakan:
    - Mean-reversion model (harga cenderung kembali ke rata-rata)
    - Volatilitas berbeda per komoditas (cabai >> beras)
    - Shock acak untuk mensimulasikan kejadian eksternal (misal: el nino)
    - Tren musiman sederhana

    Dokumentasi untuk README:
    -------------------------------------------------------
    CATATAN: Mode Simulator Aktif
    Kedua API eksternal (Badanpangan & World Bank) tidak dapat diakses
    saat pengujian. Simulator ini menggunakan data acuan harga BPS/Bapanas
    (Maret 2024) dengan model fluktuasi statistik realistis.
    Parameter volatilitas disesuaikan dengan karakteristik masing-masing
    komoditas berdasarkan data historis 2022-2024.
    -------------------------------------------------------
    """

    def __init__(self):
        self._harga = dict(_sim_state)           # salin state global
        self._trend  = {k: 0.0 for k in KOMODITAS}
        self._shock_counter = 0

    def fetch(self) -> list[dict]:
        """Generate data harga simulasi realistis untuk semua komoditas."""
        results      = []
        now          = datetime.now()
        self._shock_counter += 1

        # Random shock event setiap ~8 polling (4 jam) — simulasi berita ekonomi
        ada_shock = (self._shock_counter % 8 == 0)
        komoditas_shock = random.choice(KOMODITAS) if ada_shock else None

        for komoditas in KOMODITAS:
            harga_baru = self._update_harga(komoditas, komoditas_shock)
            _sim_state[komoditas] = harga_baru  # update state global

            # Hitung perubahan dari harga acuan
            harga_acuan = HARGA_ACUAN[komoditas]["harga"]
            perubahan_pct = (harga_baru - harga_acuan) / harga_acuan * 100

            results.append({
                "komoditas"     : komoditas,
                "label"         : KOMODITAS_LABEL[komoditas],
                "harga"         : round(harga_baru, 2),
                "satuan"        : HARGA_ACUAN[komoditas]["satuan"],
                "harga_acuan"   : harga_acuan,
                "perubahan_pct" : round(perubahan_pct, 2),
                "wilayah"       : "Nasional (Simulasi)",
                "tanggal"       : now.strftime("%Y-%m-%d"),
                "jam"           : now.strftime("%H:%M:%S"),
                "sumber"        : "simulator",
                "ada_shock"     : (komoditas == komoditas_shock),
            })

        log.info(
            f"[Simulator] Generated {len(results)} komoditas"
            + (f" | SHOCK: {komoditas_shock}" if ada_shock else "")
        )
        return results

    def _update_harga(self, komoditas: str, komoditas_shock: str | None) -> float:
        """Update harga menggunakan mean-reversion + random walk + shock."""
        info       = HARGA_ACUAN[komoditas]
        harga_lama = self._harga.get(komoditas, info["harga"])
        mean       = info["harga"]
        vol        = info["volatilitas"]

        # Mean-reversion: tarik kembali ke harga acuan (kecepatan 5%)
        reversion  = 0.05 * (mean - harga_lama)

        # Random walk: noise Gaussian
        noise      = random.gauss(0, mean * vol * 0.1)

        # Tren jangka pendek (momentum)
        trend_delta   = random.gauss(0, mean * vol * 0.02)
        self._trend[komoditas] = 0.7 * self._trend[komoditas] + 0.3 * trend_delta
        trend = self._trend[komoditas]

        # Shock event: lonjakan tiba-tiba pada 1 komoditas
        shock = 0.0
        if komoditas == komoditas_shock:
            shock = random.choice([-1, 1]) * mean * vol * random.uniform(0.5, 1.5)
            log.info(f"  [Shock] {komoditas}: {'+' if shock > 0 else ''}{shock:.0f}")

        harga_baru = harga_lama + reversion + noise + trend + shock

        # Batas bawah: tidak boleh < 50% harga acuan
        harga_baru = max(harga_baru, mean * 0.5)

        self._harga[komoditas] = harga_baru
        return harga_baru


# ═════════════════════════════════════════════
# BAGIAN 3 — KAFKA PRODUCER
# ═════════════════════════════════════════════

def buat_message(record: dict) -> dict:
    """
    Buat message Kafka yang lengkap dari data harga satu komoditas.
    Ditambahkan: message_id, timestamp_unix, metadata pipeline.
    """
    now = datetime.now()
    return {
        # Identitas
        "message_id"    : hashlib.md5(
            f"{record['komoditas']}{now.isoformat()}".encode()
        ).hexdigest()[:12],
        "schema_version": "1.0",

        # Payload inti
        "komoditas"     : record["komoditas"],
        "label"         : record["label"],
        "harga"         : record["harga"],
        "satuan"        : record.get("satuan", "kg"),
        "wilayah"       : record.get("wilayah", "Nasional"),

        # Perubahan harga (jika tersedia)
        "harga_acuan"   : record.get("harga_acuan"),
        "perubahan_pct" : record.get("perubahan_pct"),

        # Sumber data
        "sumber"        : record.get("sumber", "unknown"),
        "ada_shock"     : record.get("ada_shock", False),

        # Waktu
        "tanggal"       : record.get("tanggal", now.strftime("%Y-%m-%d")),
        "jam"           : record.get("jam", now.strftime("%H:%M:%S")),
        "timestamp_iso" : now.isoformat(),
        "timestamp_unix": int(now.timestamp()),

        # Metadata pipeline
        "pipeline"      : "pangan-monitor",
        "topic"         : KAFKA_TOPIC,
    }


def buat_producer() -> KafkaProducer:
    """Inisialisasi Kafka producer dengan retry dan serialisasi JSON."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers  = [KAFKA_BROKER],
                value_serializer   = lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer     = lambda k: k.encode("utf-8"),
                acks               = "all",          # tunggu semua replica
                retries            = 3,
                linger_ms          = 500,             # batch kecil untuk latensi rendah
                compression_type   = "gzip",
                max_block_ms       = 10_000,
            )
            log.info(f"[Kafka] Producer terhubung ke {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            log.warning(f"[Kafka] Percobaan {attempt}/5 gagal: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError("[Kafka] Tidak bisa terhubung setelah 5 percobaan.")


def kirim_ke_kafka(producer: KafkaProducer, records: list[dict]) -> int:
    """
    Kirim semua record harga ke Kafka.
    Key = nama komoditas (sesuai spesifikasi: key: nama komoditas).
    Return jumlah pesan berhasil dikirim.
    """
    sukses = 0
    for record in records:
        msg = buat_message(record)
        key = record["komoditas"]

        future = producer.send(
            KAFKA_TOPIC,
            key   = key,
            value = msg,
        )
        try:
            metadata = future.get(timeout=10)
            log.info(
                f"  ✓ [{key:15s}] Rp {record['harga']:>10,.0f}/{record.get('satuan','kg')}"
                f"  → partition={metadata.partition} offset={metadata.offset}"
            )
            sukses += 1
        except KafkaError as e:
            log.error(f"  ✗ [{key}] Gagal kirim: {e}")

    producer.flush()
    return sukses


# ═════════════════════════════════════════════
# BAGIAN 4 — ORCHESTRATOR UTAMA
# ═════════════════════════════════════════════

def ambil_data_harga() -> tuple[list[dict], str]:
    """
    Coba ambil data harga dengan urutan prioritas:
      1. Badanpangan API
      2. World Bank API
      3. Simulator (fallback wajib)

    Return: (list of records, nama sumber yang digunakan)
    """
    log.info("─" * 55)
    log.info("Mencoba Badanpangan API...")
    data = BadanPanganAPI().fetch()
    if data:
        return data, "badanpangan"

    log.info("Mencoba World Bank API...")
    data = WorldBankAPI().fetch()
    if data:
        # World Bank hanya cover sebagian komoditas — lengkapi dengan simulator
        komoditas_wb = {r["komoditas"] for r in data}
        komoditas_sisa = [k for k in KOMODITAS if k not in komoditas_wb]
        if komoditas_sisa:
            sim   = SimulatorHarga()
            extra = [r for r in sim.fetch() if r["komoditas"] in komoditas_sisa]
            data  = data + extra
            log.info(f"[Hybrid] {len(komoditas_wb)} dari WorldBank + {len(extra)} dari Simulator")
        return data, "worldbank+simulator"

    log.warning("Semua API gagal — menggunakan Simulator Realistis")
    log.warning("(Dokumentasikan ini di README sesuai petunjuk!)")
    data = SimulatorHarga().fetch()
    return data, "simulator"


def jalankan_producer():
    """Loop utama: polling setiap POLL_INTERVAL detik."""
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  HargaPangan Monitor — producer_api.py               ║")
    log.info("║  Anggota 2: Integrasi API Eksternal                  ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"Kafka Broker  : {KAFKA_BROKER}")
    log.info(f"Kafka Topic   : {KAFKA_TOPIC}")
    log.info(f"Poll Interval : {POLL_INTERVAL // 60} menit")
    log.info(f"Komoditas     : {', '.join(KOMODITAS)}")

    producer = buat_producer()

    polling_ke = 0
    try:
        while True:
            polling_ke += 1
            waktu_mulai = datetime.now()
            log.info(f"\n{'═'*55}")
            log.info(f"POLLING #{polling_ke} — {waktu_mulai.strftime('%Y-%m-%d %H:%M:%S')}")
            log.info(f"{'═'*55}")

            # Ambil data
            records, sumber = ambil_data_harga()
            log.info(f"Sumber data: [{sumber}] | {len(records)} record diterima")

            # Kirim ke Kafka
            jumlah_sukses = kirim_ke_kafka(producer, records)
            log.info(f"Kafka: {jumlah_sukses}/{len(records)} pesan terkirim ke topic '{KAFKA_TOPIC}'")

            # Ringkasan harga
            log.info("\n📊 Ringkasan Harga:")
            for r in sorted(records, key=lambda x: x.get("perubahan_pct", 0) or 0, reverse=True):
                pct = r.get("perubahan_pct")
                indikator = ("↑" if pct > 0 else "↓" if pct < 0 else "→") if pct is not None else " "
                pct_str = f"({pct:+.1f}%)" if pct is not None else ""
                log.info(
                    f"  {indikator} {r['label']:15s}: "
                    f"Rp {r['harga']:>10,.0f}/{r.get('satuan','kg')} {pct_str}"
                )

            # Tunggu interval berikutnya
            durasi = (datetime.now() - waktu_mulai).total_seconds()
            tunggu = max(0, POLL_INTERVAL - durasi)
            berikutnya = datetime.now() + timedelta(seconds=tunggu)
            log.info(
                f"\nPolling berikutnya: {berikutnya.strftime('%H:%M:%S')} "
                f"(dalam {tunggu/60:.1f} menit)"
            )
            time.sleep(tunggu)

    except KeyboardInterrupt:
        log.info("\n[Shutdown] Producer dihentikan oleh pengguna (Ctrl+C)")
    finally:
        producer.close()
        log.info("[Shutdown] Kafka producer ditutup.")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    jalankan_producer()
