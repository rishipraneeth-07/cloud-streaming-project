
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data" / "streaming" / "orders"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "checkpoints" / "orders"

KAFKA_SERVERS = "localhost:9092"
KAFKA_TOPIC = "orders"

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_time", StringType(), True),
])


def main():
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("CloudStreamingOrders")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed_df = (
            kafka_df
            .selectExpr("CAST(value AS STRING) AS json_value")
            .select(from_json(col("json_value"), order_schema).alias("order"))
            .select("order.*")
        )

        valid_orders = parsed_df.filter(
            col("order_id").isNotNull()
            & col("quantity").isNotNull()
            & col("unit_price").isNotNull()
            & (col("quantity") > 0)
            & (col("unit_price") >= 0)
        )

        processed_orders = valid_orders.withColumn(
            "line_total",
            col("quantity") * col("unit_price"),
        )

        query = (
            processed_orders.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(OUTPUT_PATH))
            .option("checkpointLocation", str(CHECKPOINT_PATH))
            .trigger(processingTime="5 seconds")
            .start()
        )

        print(f"Reading Kafka topic: {KAFKA_TOPIC}")
        print(f"Parquet output: {OUTPUT_PATH}")
        print("Streaming is running. Press Ctrl+C to stop.")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
