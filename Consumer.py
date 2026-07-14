"""
Consumer: baca event transaksi dari Kafka, landing ke file JSON lines
(simulasi raw layer: raw/bank_transactions_YYYYMMDD.jsonl).
"""

import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bank_transactions"
GROUP_ID = "bank-mini-consumer"
RAW_DIR = "raw"


def get_raw_file_path():
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    os.makedirs(RAW_DIR, exist_ok=True)
    return os.path.join(RAW_DIR, f"bank_transactions_{today}.jsonl")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print(f"Mulai consume dari topic '{TOPIC}'. Ctrl+C untuk stop.")
    count = 0
    try:
        for message in consumer:
            txn = message.value
            file_path = get_raw_file_path()
            with open(file_path, "a") as f:
                f.write(json.dumps(txn) + "\n")
            count += 1
            print(f"[{count}] partition={message.partition} Landed: {txn['transaksi_id']}")
    except KeyboardInterrupt:
        print(f"\nStop consuming. Total landed: {count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
