"""
Smart City Traffic — Spark Structured Streaming Job

Reads from Kafka topic 'traffic-raw', applies a 5-minute tumbling window
on EVENT TIME (sensor timestamp), computes a Congestion Index, and:
  - Writes all windowed results  → PostgreSQL: processed_traffic
  - Writes critical alerts       → PostgreSQL: critical_traffic
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, sum as _sum, count,
    window, when, lit, to_timestamp, greatest
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

# ── Logging ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = "kafka:29092"          # inside Docker network
KAFKA_TOPIC    = "traffic-raw"
PG_URL = "jdbc:postgresql://host.docker.internal:5432/traffic_db"
PG_PROPS       = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

WINDOW_DURATION  = "5 minutes"
WATERMARK_DELAY  = "10 minutes"         # tolerate up to 10 min late data
CHECKPOINT_DIR   = "/tmp/spark-checkpoints"

# ── Kafka message schema ───────────────────────────────────────────────
SCHEMA = StructType([
    StructField("sensor_id",     StringType(),  True),
    StructField("timestamp",     StringType(),  True),   # ISO-8601 string
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed",     DoubleType(),  True),
    StructField("is_critical",   BooleanType(), True),
])

# ── Congestion Index formula ───────────────────────────────────────────
# CI = max(0, (1 - avg_speed / 80) * 100)
# Ranges 0 (free flow at 80+ km/h) → 100 (complete standstill)
# Justified: 80 km/h is the typical free-flow speed on Colombo arterials
def congestion_index(avg_speed_col):
    return greatest(lit(0.0), (lit(1.0) - avg_speed_col / lit(80.0)) * lit(100.0))

# ── Write batch to PostgreSQL ──────────────────────────────────────────
def write_to_postgres(batch_df, batch_id, table):
    count = batch_df.count()
    if count == 0:
        return
    log.info("Batch %d → writing %d rows to %s", batch_id, count, table)
    batch_df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)

# ── Main ───────────────────────────────────────────────────────────────
def main():
    log.info("Initialising Spark session...")

    spark = SparkSession.builder \
        .appName("SmartCityTrafficStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready.")

    # ── 1. Read raw stream from Kafka ──────────────────────────────────
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # ── 2. Parse JSON and extract event time ───────────────────────────
    parsed = raw \
        .select(from_json(col("value").cast("string"), SCHEMA).alias("d")) \
        .select("d.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))
        # ↑ This uses EVENT TIME (sensor clock), not Spark's processing time.
        # Watermarking below tells Spark to wait up to 10 min for late data.

    # ── 3. Apply watermark + 5-minute tumbling window ─────────────────
    windowed = parsed \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),
            col("sensor_id")
        ) \
        .agg(
            avg("avg_speed").alias("avg_speed"),
            _sum("vehicle_count").alias("vehicle_count"),
            count("*").alias("reading_count")
        ) \
        .withColumn("congestion_index", congestion_index(col("avg_speed"))) \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end",   col("window.end")) \
        .drop("window")

    # ── 4. Write windowed results → processed_traffic ─────────────────
    q1 = windowed \
        .select("window_start", "window_end", "sensor_id",
                "avg_speed", "vehicle_count", "congestion_index") \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "processed_traffic")) \
        .option("checkpointLocation", CHECKPOINT_DIR + "/processed") \
        .start()

    # ── 5. Filter critical events and write immediately ────────────────
    critical = parsed \
        .withWatermark("event_time", WATERMARK_DELAY) \
        .filter(col("avg_speed") < 10.0) \
        .select(
            col("sensor_id"),
            col("event_time"),
            col("avg_speed"),
            col("vehicle_count"),
            lit("CRITICAL").alias("alert_level")
        )

    q2 = critical \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, "critical_traffic")) \
        .option("checkpointLocation", CHECKPOINT_DIR + "/critical") \
        .start()

    log.info("Both streaming queries running. Waiting for termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()