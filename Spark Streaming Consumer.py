"""
Spark Structured Streaming consumer: baca dari Kafka topic
'bank_transactions', tulis ke Parquet (simulasi staging layer),
dengan checkpoint biar exactly-once & bisa resume kalau restart.

Jalankan dengan spark-submit (bukan `python`), karena butuh connector
Kafka dari Maven:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
        spark_streaming_consumer.py

Sesuaikan versi connector (_2.13:<versi>) dengan versi PySpark yang
terinstall (`python3 -c "import pyspark; print(pyspark.__version__)"`).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "bank_transactions"
OUTPUT_PATH = "spark_output/bank_transactions_parquet"
CHECKPOINT_PATH = "spark_output/_checkpoint"

spark = (
    SparkSession.builder
    .appName("BankTransactionStreaming")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schema harus didefinisikan eksplisit, gak bisa infer schema dari
# streaming source (beda dari batch read biasa)
schema = StructType([
    StructField("transaksi_id", StringType()),
    StructField("rekening_id", StringType()),
    StructField("nasabah_id", StringType()),
    StructField("cabang_id", StringType()),
    StructField("jenis_transaksi", StringType()),
    StructField("jumlah", DoubleType()),
    StructField("timestamp", StringType()),
])

# Baca stream mentah dari Kafka. Value-nya masih berupa bytes,
# harus di-cast ke string dulu baru di-parse JSON-nya.
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Tulis ke Parquet, trigger tiap 10 detik. Checkpoint nyimpen progress
# offset Kafka yang udah diproses, jadi kalau job restart, dia lanjut
# gak ulang dari awal (sama konsepnya kayak consumer group offset,
# tapi versi Spark).
query = (
    parsed_stream.writeStream
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start()
)

print(f"Streaming query started. Writing to {OUTPUT_PATH}")
query.awaitTermination()
