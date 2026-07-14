# Mini Project Kafka: Bank Transaction Stream (Dari Awal, Step-by-Step)

Tutorial ini bangun ulang project dari nol, lokal di WSL pakai Docker.
Tiap step dijelasin konsepnya, bukan cuma perintahnya.

---

## Bagian 0: Konsep Dasar Kafka (Wajib Paham Dulu)

Sebelum ngoding, ini istilah yang bakal muncul terus:

| Istilah | Penjelasan |
|---|---|
| **Broker** | Server Kafka yang nyimpen & nge-serve data. Mini project ini pakai 1 broker (single-node). |
| **Topic** | "Nama kategori" buat data, mirip nama tabel. Contoh: `bank_transactions`. |
| **Partition** | Topic dibagi jadi beberapa partition biar bisa diproses paralel. 1 partition = 1 log file terurut (append-only). |
| **Offset** | Nomor urut pesan di dalam satu partition. Kayak primary key auto-increment per partition. |
| **Producer** | Aplikasi yang **kirim** data ke topic. |
| **Consumer** | Aplikasi yang **baca** data dari topic. |
| **Consumer Group** | Sekumpulan consumer yang share kerjaan baca satu topic. Kafka bagi partition ke consumer dalam satu group, jadi gak ada duplikasi kerjaan antar consumer di group yang sama. |
| **Key** | Setiap pesan bisa punya key. Kafka pakai key buat nentuin pesan itu masuk partition mana (hash-based). Berguna kalau kamu mau semua transaksi dari 1 rekening selalu di partition yang sama & urut. |

**Analogi:** topic itu kayak grup WhatsApp, partition itu kayak beberapa
thread paralel di grup itu biar gak numpuk di satu tempat, offset itu
nomor urut chat, consumer group itu tim yang baca chat bareng-bareng
tapi gak baca pesan yang sama dua kali.

Kenapa ini penting: urutan event penting (misal update saldo harus
urut). Kafka jamin urutan **di dalam satu partition**, makanya
pemilihan key itu krusial.

---

## Bagian 1: Jalankan Kafka Broker

### 1.1 Cek Docker

```bash
docker --version
docker compose version
```

Kalau belum ada, install (butuh akses sudo):

```bash
sudo apt update
sudo apt install docker.io -y
sudo service docker start
sudo usermod -aG docker $USER
newgrp docker
docker run hello-world   # test
```

### 1.2 `docker-compose.yml`

Pakai image `apache/kafka` dengan **KRaft mode** — mode baru Kafka
yang gak butuh Zookeeper lagi (broker handle koordinasi sendiri).
Lebih simpel buat local dev.

Kenapa `PLAINTEXT` (bukan `SASL_SSL` kayak di cluster kantor)? Karena
ini broker sendiri di laptop, gak ada yang perlu diautentikasi.
`SASL_SSL` + Kerberos baru relevan pas connect ke cluster shared.

Jalankan:

```bash
docker compose up -d
docker logs kafka-bank-mini --tail 20
```

Tunggu sampai log nunjukin baris `Kafka Server started`.

### 1.3 Buat Topic Manual (biar paham partition)

```bash
bash create_topic.sh
```

Script ini pakai `--partitions 3 --replication-factor 1`:
- **3 partition**: topic dibagi 3, Kafka nentuin pesan masuk partition
  mana berdasarkan hash dari key (`rekening_id`).
- **replication-factor 1**: gak ada replikasi karena cuma 1 broker
  (di cluster produksi biasanya 3).

Cek hasilnya dengan `--describe` — output bakal nunjukin 3 partition
(0, 1, 2), masing-masing punya leader broker.

---

## Bagian 2: Setup Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Pakai `kafka-python` (pure Python, gak butuh compile C library) —
sengaja dipilih biar gak kena masalah system package yang butuh root,
mirip kendala Kerberos di cluster kantor.

---

## Bagian 3: Producer — Kirim Data Transaksi

Lihat `producer.py`. Poin penting:

### Serializer
```python
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)
```
Kafka cuma ngerti bytes, jadi dict Python harus diubah ke JSON string
lalu di-encode ke bytes.

### Key = rekening_id
```python
producer.send("bank_transactions", key=txn["rekening_id"], value=txn)
```
Supaya semua transaksi dari satu rekening yang sama selalu jatuh ke
partition yang sama, jadi urutannya terjamin per rekening.

### Flush saat berhenti
```python
finally:
    producer.flush()   # pastikan semua pesan ke-kirim sebelum exit
    producer.close()
```
`producer.send()` itu **async** — cuma masukin ke buffer internal.
`flush()` maksa semua yang di buffer beneran terkirim.

---

## Bagian 4: Consumer — Baca & Landing Data

Lihat `consumer.py`. Poin penting:

### Group ID & offset
```python
consumer = KafkaConsumer(
    "bank_transactions",
    group_id="bank-mini-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    ...
)
```
- `group_id`: kalau 2 consumer punya `group_id` sama, Kafka bagi
  partition ke mereka berdua — dasar horizontal scaling.
- `auto_offset_reset="earliest"`: consumer group baru mulai baca dari
  pesan paling awal.
- `enable_auto_commit=True`: offset yang udah dibaca otomatis
  di-commit berkala, jadi consumer restart lanjut dari offset
  terakhir, bukan ulang dari awal.

### Loop baca (blocking)
```python
for message in consumer:
    ...
```
Mirip `tail -f`, nunggu terus sampai ada pesan baru. `message.value`
udah otomatis ke-decode balik jadi dict Python.

File `.jsonl` (JSON Lines) dipilih karena append-friendly — satu
baris satu record, gampang di-load ke Spark (`spark.read.json()`)
buat lanjut ke staging layer.

---

## Bagian 5: Jalankan End-to-End

Buka 2 terminal (masih di venv yang sama):

```bash
# Terminal 1
python producer.py

# Terminal 2
python consumer.py
```

Cek hasilnya:
```bash
cat raw/bank_transactions_*.jsonl | wc -l   # jumlah record
tail -5 raw/bank_transactions_*.jsonl        # contoh record terakhir
```

---

## Bagian 6: Spark Structured Streaming (Alternatif Consumer)

Lihat `spark_streaming_consumer.py`. Perbedaan kunci dari consumer
manual:

- **Schema eksplisit wajib** — streaming source gak bisa infer schema
  otomatis kayak batch read biasa.
- **`readStream`/`writeStream`** — bukan `read`/`write` biasa.
- **Checkpoint** — `option("checkpointLocation", ...)` nyimpen progress
  offset Kafka yang udah diproses, setara konsep consumer group offset
  tapi versi Spark, memastikan exactly-once & bisa resume kalau job
  restart.
- **Trigger interval** — `trigger(processingTime="10 seconds")`,
  Spark proses data dalam micro-batch tiap 10 detik, bukan
  event-by-event kayak consumer manual.

Jalankan (stop dulu `consumer.py` manual biar gak bentrok):
```bash
pip install pyspark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
  spark_streaming_consumer.py
```

Cek hasil (butuh `pandas` + `pyarrow`):
```bash
pip install pandas pyarrow
python3 -c "
import pandas as pd
df = pd.read_parquet('spark_output/bank_transactions_parquet/')
print('Total record:', len(df))
print(df['jenis_transaksi'].value_counts())
"
```

---

## Bagian 7: Eksperimen Tambahan (Opsional tapi Disarankan)

### 7.1 Lihat distribusi partition

```bash
docker exec -it kafka-bank-mini /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bank_transactions \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --max-messages 20
```

Perhatikan: `rekening_id` yang sama harusnya selalu muncul di
`partition` yang sama.

### 7.2 Cek consumer group lag

```bash
docker exec -it kafka-bank-mini /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group bank-mini-consumer
```

Kolom `LAG` nunjukin berapa banyak pesan yang belum dibaca consumer —
metrik ini yang biasa dipantau di production buat tau consumer
ketinggalan apa nggak.

### 7.3 Test resilience: matiin consumer, lalu nyalain lagi

Stop `consumer.py` (Ctrl+C), tunggu producer kirim beberapa pesan
lagi, lalu jalankan `consumer.py` lagi. Karena `group_id` sama & auto
offset commit, consumer bakal lanjut dari offset terakhir — bukan
ulang dari awal, bukan juga skip pesan yang numpuk selama consumer
mati. Bandingkan `LAG` sebelum & sesudah consumer mati untuk melihat
backlog yang tertahan aman di broker.

---

## Troubleshooting Umum

| Masalah | Penyebab | Solusi |
|---|---|---|
| `NoBrokersAvailable` | Broker belum ready / port salah | Cek `docker logs kafka-bank-mini`, tunggu sampai "started" |
| Consumer gak nangkep data lama | `auto_offset_reset="latest"` (default) tapi topic udah ada isinya sebelum consumer nyala | Set `earliest`, atau ganti `group_id` baru |
| Port 9092 bentrok | Ada service lain pakai port itu | Ganti port di `docker-compose.yml` & `bootstrap_servers` |
| Docker gak jalan di WSL kantor | Restriction IT / WSL2 integration belum aktif | `sudo apt install docker.io`, `sudo service docker start` |
| `SyntaxError: unterminated string literal` saat bikin file `.py` | Copy-paste ke terminal kepotong di tengah | Timpa ulang file dengan `cat > file.py << 'EOF' ... EOF`, pastikan full block ter-paste |
| PySpark error versi Java | PySpark 4.x butuh Java 17+ | `sudo apt install openjdk-17-jdk -y` |
| `spark-submit` gagal download package | Versi connector Kafka gak cocok sama versi PySpark/Scala | Cek `pyspark.__version__`, sesuaikan `spark-sql-kafka-0-10_2.13:<versi>` |

## Next Steps

1. Ganti consumer manual jadi **Spark Structured Streaming** penuh
   (sudah dibahas di Bagian 6) biar konsisten dengan pipeline batch.
2. Tambah **schema validation** sebelum landing — reject event yang
   field-nya invalid, mirip pendekatan DQ module.
3. Setelah lancar, migrasi `bootstrap_servers` & tambahin
   `security_protocol="SASL_SSL"` + config GSSAPI buat connect ke
   cluster CDP kantor.
