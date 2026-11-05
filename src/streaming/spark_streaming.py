
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

from scripts.upload_streaming_to_minio import (
    create_minio_client,
    upload_batch_file,
)

from src.common.pipeline_tracker import track_pipeline_run


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

    with track_pipeline_run(
        "orders_streaming_processing",
        "STREAMING",
    ) as run:

        minio_client = create_minio_client()

        spark = (
            SparkSession.builder
            .appName("CloudStreamingOrders")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
            )
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
                .select(
                    from_json(
                        col("json_value"),
                        order_schema,
                    ).alias("order")
                )
                .select("order.*")
            )

            def process_batch(batch_df, batch_id):
                total_count = batch_df.count()

                valid_orders = batch_df.filter(
                    col("order_id").isNotNull()
                    & col("quantity").isNotNull()
                    & col("unit_price").isNotNull()
                    & (col("quantity") > 0)
                    & (col("unit_price") >= 0)
                )

                valid_count = valid_orders.count()

                run["records_processed"] += valid_count
                run["records_failed"] += total_count - valid_count

                if valid_count == 0:
                    print(
                        f"Batch {batch_id}: "
                        f"received={total_count}, processed=0, "
                        f"invalid={total_count}"
                    )
                    return

                processed_orders = valid_orders.withColumn(
                    "line_total",
                    col("quantity") * col("unit_price"),
                )

                batch_output_dir = (
                    OUTPUT_PATH / f"batch-{batch_id}"
                )

                (
                    processed_orders
                    .coalesce(1)
                    .write
                    .mode("overwrite")
                    .parquet(str(batch_output_dir))
                )

                parquet_files = list(
                    batch_output_dir.glob("part-*.parquet")
                )

                if len(parquet_files) != 1:
                    raise RuntimeError(
                        f"Expected one Parquet file for batch {batch_id}, "
                        f"found {len(parquet_files)}"
                    )

                object_key = upload_batch_file(
                    minio_client,
                    parquet_files[0],
                    batch_id,
                )

                print(
                    f"Batch {batch_id}: "
                    f"received={total_count}, "
                    f"processed={valid_count}, "
                    f"invalid={total_count - valid_count}"
                )
                print(f"MinIO upload complete: {object_key}")

            query = (
                parsed_df.writeStream
                .foreachBatch(process_batch)
                .option(
                    "checkpointLocation",
                    str(CHECKPOINT_PATH),
                )
                .trigger(processingTime="5 seconds")
                .start()
            )

            print(f"Reading Kafka topic: {KAFKA_TOPIC}")
            print(f"Parquet output: {OUTPUT_PATH}")
            print("MinIO automatic upload: enabled")
            print("Streaming is running. Press Ctrl+C to stop.")

            query.awaitTermination()

        finally:
            spark.stop()


if __name__ == "__main__":
    main()

