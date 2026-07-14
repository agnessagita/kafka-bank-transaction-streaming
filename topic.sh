#!/bin/bash
# Jalankan setelah `docker compose up -d` dan broker sudah ready.

docker exec -it kafka-bank-mini /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic bank_transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "--- Describe topic ---"
docker exec -it kafka-bank-mini /opt/kafka/bin/kafka-topics.sh \
  --describe --topic bank_transactions --bootstrap-server localhost:9092
