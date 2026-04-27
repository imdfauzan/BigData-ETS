from kafka import KafkaConsumer
import json
import os
from datetime import datetime

consumer = KafkaConsumer(
    'pangan-api',
    'pangan-rss',
    bootstrap_servers=['127.0.0.1:9092'],
    auto_offset_reset='earliest',
    group_id=None,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("=== CONSUMER STARTED ===")

def save_to_hdfs(data, folder):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"data_{timestamp}.json"
    local_path = f"/tmp/{filename}"

    # save lokal dulu
    with open(local_path, "w") as f:
        json.dump(data, f)

    print("Uploading:", filename)

    # copy ke container namenode
    os.system(f"docker cp {local_path} namenode:/tmp/{filename}")

    # simpan ke HDFS
    os.system(f"docker exec namenode hdfs dfs -mkdir -p {folder}")
    os.system(f"docker exec namenode hdfs dfs -put -f /tmp/{filename} {folder}")

    print("Saved to HDFS:", folder)
    print("-"*50)

for msg in consumer:
    print("Received:", msg.value)

    if msg.topic == "pangan-api":
        save_to_hdfs(msg.value, "/data/pangan/api")

    elif msg.topic == "pangan-rss":
        save_to_hdfs(msg.value, "/data/pangan/rss")