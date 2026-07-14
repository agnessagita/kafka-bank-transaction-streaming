"""
Producer: generate event transaksi bank, kirim ke Kafka.
Key = rekening_id, supaya transaksi dari rekening yang sama selalu
masuk partition yang sama (urutan terjamin per rekening).
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "bank_transactions"

JENIS_TRANSAKSI = ["debit", "kredit", "transfer", "tarik_tunai", "setor_tunai"]
CABANG_ID = ["CBG001", "CBG002", "CBG003", "CBG004"]


def generate_transaksi():
    return {
        "transaksi_id": str(uuid.uuid4()),
        "rekening_id": f"REK{random.randint(1000, 1050)}",
        "nasabah_id": f"NAS{random.randint(100, 130)}",
        "cabang_id": random.choice(CABANG_ID),
        "jenis_transaksi": random.choice(JENIS_TRANSAKSI),
        "jumlah": round(random.uniform(50_000, 25_000_000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Mulai produce ke topic '{TOPIC}'. Ctrl+C untuk stop.")
    try:
        while True:
            txn = generate_transaksi()
            producer.send(TOPIC, key=txn["rekening_id"], value=txn)
            print(f"Sent: {txn['transaksi_id']} | {txn['jenis_transaksi']} | Rp{txn['jumlah']:,.0f}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStop producing.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
