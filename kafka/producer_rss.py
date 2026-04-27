from kafka import KafkaProducer
import json
import time
import random
import hashlib
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

komoditas = [
    "beras","jagung","kedelai","gula",
    "minyak_goreng","cabai","bawang_merah","telur"
]

templates = [
    "Harga {k} naik akibat distribusi terganggu",
    "Harga {k} turun karena stok melimpah",
    "Permintaan {k} meningkat drastis",
    "Krisis pasokan {k} di beberapa daerah",
    "Pemerintah intervensi harga {k}"
]

print("=== PRODUCER RSS STARTED ===")

while True:
    k = random.choice(komoditas)
    title = random.choice(templates).format(k=k)

    link = f"http://news/{hash(title)}"
    key = hashlib.md5(link.encode()).hexdigest()

    data = {
        "title": title,
        "komoditas": k,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("pangan-rss", key=key.encode(), value=data).get()
    print("Sent RSS:", data)

    time.sleep(5)