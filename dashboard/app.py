import json
import os
from datetime import datetime
from flask import Flask, jsonify, render_template

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
SPARK_RESULTS_PATH = os.path.join(DATA_DIR, "spark_results.json")
LIVE_API_PATH = os.path.join(DATA_DIR, "live_api.json")
LIVE_RSS_PATH = os.path.join(DATA_DIR, "live_rss.json")


def load_json_file(path, handle_nan=False):
    """Membaca file JSON dengan error handling."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
            if handle_nan:
                content = content.replace("NaN", "null")
            return json.loads(content)
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"[ERROR] Gagal memuat {path}: {e}")
        return None


@app.route("/")
def index():
    """Render halaman utama dashboard."""
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    """
    Endpoint API untuk mengirim data lengkap ke frontend.
    Menggabungkan Spark results, live harga, dan berita terbaru.
    """
    # Load data dari berbagai sumber
    spark_data = load_json_file(SPARK_RESULTS_PATH, handle_nan=True) or {}
    live_api_raw = load_json_file(LIVE_API_PATH) or []
    live_rss_raw = load_json_file(LIVE_RSS_PATH) or []

    # Handle both dict and list formats for live data
    if isinstance(live_api_raw, dict):
        live_harga = live_api_raw.get("data", [])
    else:
        live_harga = live_api_raw if isinstance(live_api_raw, list) else []
    
    if isinstance(live_rss_raw, dict):
        berita = live_rss_raw.get("data", [])
    else:
        berita = live_rss_raw if isinstance(live_rss_raw, list) else []

    # Siapkan response yang sesuai dengan struktur dashboard
    response = {
        "server_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        
        # Live data
        "live_harga": live_harga[:20] if isinstance(live_harga, list) else [],
        "berita": berita[:20] if isinstance(berita, list) else [],
        
        # Spark analysis results
        "spark": spark_data.get("analyses", {}) if isinstance(spark_data, dict) else {},
        
        # Status pipeline
        "status": {
            "spark_ok": bool(spark_data),
            "live_ok": len(live_harga) > 0,
            "news_ok": len(berita) > 0,
        }
    }

    return jsonify(response)


@app.route("/api/live")
def api_live():
    """Endpoint API untuk data live terbaru dari consumer."""
    live_api = load_json_file(LIVE_API_PATH) or {"topic": "pangan-api", "data": []}
    live_rss = load_json_file(LIVE_RSS_PATH) or {"topic": "pangan-rss", "data": []}

    return jsonify({
        "server_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "live_api": live_api,
        "live_rss": live_rss,
    })


@app.route("/api/health")
def health_check():
    """Endpoint health check sederhana."""
    return jsonify({
        "status": "ok",
        "service": "HargaPangan Dashboard",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })


if __name__ == "__main__":
    print("=" * 50)
    print(" HargaPangan Dashboard - Kelompok 6")
    print(" Akses aplikasi di: http://localhost:5000")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
