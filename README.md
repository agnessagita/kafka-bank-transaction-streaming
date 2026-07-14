# Bank Transaction Streaming Pipeline — Kafka Mini Project

Mini project untuk mempelajari dan membuktikan pemahaman Apache Kafka sebagai extension dari Bank Transaction Data Pipeline (batch, star schema, SCD Type 0/1/2) yang sudah dibangun sebelumnya. Project ini fokus pada bagian real-time streaming ingestion, dijalankan lokal (Docker + WSL) sebelum nantinya di migrasikan ke cluster CDP produksi.

##Latar Belakang

Pipeline batch yang sudah ada memproses data secara terjadwal (nightly ETL), yang berarti ada delay antara transaksi terjadi dan data siap dianalisis. Kafka digunakan untuk menutup gap tersebut — menangkap event transaksi begitu terjadi, membuka jalan untuk use case seperti fraud detection real-time, dashboard operasional live, dan CDC (Change Data Capture) yang membuat warehouse selalu sinkron dengan source system.

Project ini awalnya diarahkan ke cluster CDP kantor, namun sempat terblokir oleh kebutuhan autentikasi Kerberos/SASL_SSL (butuh package sistem krb5-devel, python3-devel yang memerlukan akses root di edge node). Sebagai jalan keluar sambil menunggu akses tersebut, project mini ini dibangun sepenuhnya lokal menggunakan PLAINTEXT (tanpa autentikasi), sehingga logic dan konsepnya tetap sama persis — tinggal ganti connection config saat migrasi ke cluster asli.

##Arsitektur

┌─────────────┐      ┌──────────────────────┐      ┌─────────────────┐
│  Producer    │─────▶│  Kafka Topic          │─────▶│  Consumer(s)     │
│  (Python)    │      │  bank_transactions     │      │                  │
│  1 event/detik│      │  3 partitions          │      ├──────────────────┤
└─────────────┘      │  key = rekening_id     │      │ A. Python manual │
                      └──────────────────────┘      │    → JSON lines  │
                                                       │                  │
                                                       │ B. Spark          │
                                                       │    Structured     │
                                                       │    Streaming      │
                                                       │    → Parquet      │
                                                       └──────────────────┘

###Dua jalur consumer dibangun untuk dibandingkan:


Consumer A (Python/kafka-python): landing manual ke file JSON lines, simulasi raw layer sederhana. Consumer B (Spark Structured Streaming): baca Kafka sebagai
DataFrame streaming, parse JSON dengan schema eksplisit, tulis ke Parquet dengan checkpoint — konsisten dengan tooling pipeline batch yang sudah ada (spark3-submit, pattern SCD).


##Tech Stack

Komponen                    Tools
Message broker              Apache Kafka 3.7.0 (KRaft mode, tanpa Zookeeper), single-node via Docker
Producer/Consumer manual    Python 3.11, kafka-python 2.0.2
Stream processing           PySpark 4.1.2, spark-sql-kafka-0 10_2.13
Environment                 WSL2 (Ubuntu 24.04), Docker Engine
Storage output              JSON Lines (raw layer), Parquet (staging layer)

##Struktur Folder

kafka-bank-transaction-streaming/
├── README.md                      # dokumentasi ini
├── TUTORIAL.md                    # panduan step-by-step dari nol + konsep Kafka
├── docker-compose.yml             # Kafka broker (KRaft mode, single-node)
├── create_topic.sh                # buat topic bank_transactions, 3 partition
├── requirements.txt               # dependency Python (kafka-python)
├── producer.py                    # generate & kirim event transaksi
├── consumer.py                    # consumer manual → JSON lines
├── spark_streaming_consumer.py    # consumer Spark Structured Streaming → Parquet
├── .gitignore
├── raw/                           # (generated) output consumer manual
└── spark_output/                  # (generated) output Spark + checkpoint

##Skema Data

Event transaksi mengikuti skema fact_transaksi dari project pipeline batch (disederhanakan):

json{
  "transaksi_id": "uuid",
  "rekening_id": "REK1000-1050",
  "nasabah_id": "NAS100-130",
  "cabang_id": "CBG001-004",
  "jenis_transaksi": "debit | kredit | transfer | tarik_tunai | setor_tunai",
  "jumlah": "double, Rp50.000 - Rp25.000.000",
  "timestamp": "ISO8601 UTC"
}

Key pesan Kafka menggunakan rekening_id — memastikan semua transaksi dari satu rekening yang sama selalu masuk ke partition yang sama, sehingga urutan transaksi per rekening terjamin.

##Setup & Menjalankan

#### 1. Jalankan Kafka broker
docker compose up -d
docker logs kafka-bank-mini --tail 20   # tunggu sampai "Kafka Server started"

#### 2. Buat topic dengan 3 partition
bash create_topic.sh

#### 3. Setup Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

#### 4a. Consumer manual (2 terminal terpisah)
python producer.py
python consumer.py

#### 4b. Atau, Spark Structured Streaming (butuh PySpark + Java 17+)
pip install pyspark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \ spark_streaming_consumer.py

Detail penjelasan tiap konsep (broker, partition, offset, consumer group, dll) ada di TUTORIAL.md.

##Hasil Testing & Validasi

1. End-to-end ingestion (Consumer manual)
   - 329 transaksi berhasil di-generate producer dan landing ke raw/bank_transactions_YYYYMMDD.jsonl.
   - Distribusi jenis transaksi: kredit (72), debit (69), tarik_tunai (66), setor_tunai (66), transfer (56) — merata sesuai random distribution di producer.
   - 0 duplikat dari 329 transaksi_id (diverifikasi dengan jq).


2. Konsistensi partition-key
   - Semua transaksi dengan rekening_id yang sama (contoh: REK1003, transaksi) konsisten masuk ke partition yang sama, membuktikan key-based partitioning bekerja sesuai ekspektasi.


3. Consumer group lag monitoring

PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          58              60              2
1          62              63              1
2          36              38              2

Lag kecil (1-2) menunjukkan consumer berjalan sehat, hampir selalu
real-time mengejar producer.

4. Resilience test (consumer down & recovery)

Skenario: consumer dimatikan (Ctrl+C) selama ~15 detik sementara
producer tetap berjalan.

Tahap                      Hasil
Saat consumer mati         Consumer group status: "no active members"
Lag menumpuk               45 + 48 + 29 = 122 pesan tertahan aman di broker
Consumer dinyalakan lagi   Berhasil drain seluruh backlog tanpa restart dari
awal
Total setelah recovery     329 record, 0 data hilang, 0 duplikat

Insight kunci: Kafka broker adalah durable log storage, bukan
sekadar pipa lewat — pesan tetap tersimpan aman selama consumer mati,
dan enable_auto_commit=True + group_id yang konsisten membuat
consumer otomatis resume dari offset terakhir (bukan re-read dari
awal, bukan juga skip pesan yang tertunda).

5. Spark Structured Streaming
   - Berhasil membaca topic yang sama, parsing JSON dengan schema eksplisit (schema tidak bisa di-infer otomatis dari streaming source, beda dengan batch read biasa).
   - Trigger interval 10 detik (processingTime="10 seconds"), output bertambah otomatis (329 → 333 record teramati antar pengecekan).
   - Checkpoint folder (_checkpoint/offsets, _checkpoint/commits) aktif tercatat, menjadi dasar exactly-once processing & kemampuan resume jika job Spark di-restart.


##Konsep Kafka yang Dipelajari & Dibuktikan
- Broker, topic, partition, offset — struktur dasar penyimpanan data terurut per partition.
- Producer key → partition assignment — hash-based routing, penting untuk menjaga urutan event per entitas (rekening).
- Consumer group — mekanisme pembagian partition antar consumer dalam satu group, dasar horizontal scaling.
- Durability & at-least-once delivery — data tidak hilang selama consumer down, terbukti lewat resilience test.
- Streaming vs batch trade-off — kapan Kafka relevan digunakan dibanding pipeline batch terjadwal.
- Spark Structured Streaming integration — cara menyambungkan Kafka ke tooling Spark yang sudah dikuasai dari pipeline batch.
