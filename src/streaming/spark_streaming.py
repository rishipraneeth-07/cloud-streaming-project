
from pathlib import Path
import os
import sys
import time

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
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.upload_streaming_to_minio import (  # noqa: E402
    create_minio_client,
    upload_batch_file,
)
from src.common.pipeline_tracker import (  # noqa: E402
    track_pipeline_run,
    update_pipeline_progress,
)
from src.streaming.quarantine import (  # noqa: E402
    failure_reason,
    persist_quarantine,
)

OUTPUT_PATH = Path(os.getenv(
    "STREAMING_OUTPUT_PATH",
    str(PROJECT_ROOT / "data" / "streaming" / "orders"),
))
CHECKPOINT_PATH = Path(os.getenv(
    "STREAMING_CHECKPOINT_PATH",
    str(PROJECT_ROOT / "data" / "checkpoints" / "orders"),
))

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
STARTING_OFFSETS = os.getenv("STREAMING_STARTING_OFFSETS", "earliest")
QUARANTINE_BUCKET = os.getenv("STREAMING_QUARANTINE_BUCKET", "hot-data")
QUARANTINE_PREFIX = os.getenv("STREAMING_QUARANTINE_PREFIX", "quarantine/orders")


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
            .config("spark.local.dir", os.getenv("SPARK_LOCAL_DIRS", "/tmp/spark-local"))
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
            )
            .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/spark-ivy"))
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        try:
            stop_after_records = int(
                os.getenv("STREAMING_STOP_AFTER_RECORDS", "0")
            )
            kafka_df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", STARTING_OFFSETS)
                .load()
            )

            parsed_df = (
                kafka_df
                .selectExpr(
                    "CAST(value AS STRING) AS json_value",
                    "topic",
                    "partition",
                    "offset",
                    "timestamp AS source_timestamp",
                )
                .select(
                    "json_value", "topic", "partition", "offset", "source_timestamp",
                    from_json(
                        col("json_value"),
                        order_schema,
                    ).alias("order")
                )
                .select("json_value", "topic", "partition", "offset", "source_timestamp", "order.*")
            )

            def process_batch(batch_df, batch_id):
                total_count = batch_df.count()

                valid_orders = batch_df.filter(
                    col("order_id").isNotNull()
                    & col("quantity").isNotNull()
                    & col("unit_price").isNotNull()
                    & col("product").isNotNull()
                    & (col("product") != "")
                    & col("order_status").isNotNull()
                    & (col("order_status") != "")
                    & col("event_time").isNotNull()
                    & (col("event_time") != "")
                    & (col("quantity") > 0)
                    & (col("unit_price") >= 0)
                )

                valid_count = valid_orders.count()

                for row in batch_df.filter(
                    col("order_id").isNull()
                    | col("quantity").isNull()
                    | col("unit_price").isNull()
                    | col("product").isNull()
                    | (col("product") == "")
                    | col("order_status").isNull()
                    | (col("order_status") == "")
                    | col("event_time").isNull()
                    | (col("event_time") == "")
                    | (col("quantity") <= 0)
                    | (col("unit_price") < 0)
                ).toLocalIterator():
                    record = row.asDict(recursive=True)
                    reason = failure_reason(record) or "schema_or_business_rule_violation"
                    key, created = persist_quarantine(
                        minio_client,
                        QUARANTINE_BUCKET,
                        QUARANTINE_PREFIX,
                        record,
                        reason,
                    )
                    print(
                        f"Quarantined {'new' if created else 'existing'} record: "
                        f"{QUARANTINE_BUCKET}/{key} ({reason})"
                    )

                run["records_processed"] += valid_count
                run["records_failed"] += total_count - valid_count

                if valid_count == 0:
                    _commit_kafka_offsets(batch_df)
                    update_pipeline_progress(
                        run["id"],
                        run["records_processed"],
                        run["records_failed"],
                    )
                    run["stop_requested"] = (
                        stop_after_records > 0
                        and run["records_processed"] + run["records_failed"]
                        >= stop_after_records
                    )
                    print(
                        f"Batch {batch_id}: "
                        f"received={total_count}, processed=0, "
                        f"invalid={total_count}"
                    )
                    return

                processed_orders = valid_orders.select(
                    "order_id",
                    "product",
                    "quantity",
                    "unit_price",
                    "order_status",
                    "event_time",
                ).withColumn(
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

                # Commit the real Kafka group only after quarantine writes and
                # the durable Parquet upload have both succeeded.
                _commit_kafka_offsets(batch_df)

                print(
                    f"Batch {batch_id}: "
                    f"received={total_count}, "
                    f"processed={valid_count}, "
                    f"invalid={total_count - valid_count}"
                )
                print(f"MinIO upload complete: {object_key}")

                update_pipeline_progress(
                    run["id"],
                    run["records_processed"],
                    run["records_failed"],
                )

                run["stop_requested"] = (
                    stop_after_records > 0
                    and run["records_processed"] + run["records_failed"]
                    >= stop_after_records
                )

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

            run_seconds = int(os.getenv("STREAMING_RUN_SECONDS", "0"))
            if run_seconds <= 0 and stop_after_records <= 0:
                query.awaitTermination()
            else:
                deadline = (
                    time.monotonic() + run_seconds
                    if run_seconds > 0
                    else None
                )
                while query.isActive:
                    if run.get("stop_requested"):
                        query.stop()
                        break

                    if deadline is not None and time.monotonic() >= deadline:
                        query.stop()
                        raise TimeoutError(
                            "Streaming run exceeded STREAMING_RUN_SECONDS "
                            "before its record target was reached"
                        )

                    query.awaitTermination(timeout=1)

        finally:
            spark.stop()


def _commit_kafka_offsets(batch_df):
    """Commit next offsets to a real Kafka consumer group after batch success."""
    from kafka import KafkaConsumer
    from kafka.structs import OffsetAndMetadata, TopicPartition

    group_id = os.getenv("KAFKA_CONSUMER_GROUP", "orders-structured-streaming")
    maximums = (
        batch_df.groupBy("topic", "partition")
        .agg({"offset": "max"})
        .collect()
    )
    offsets = {}
    for row in maximums:
        max_offset = row["max(offset)"]
        if max_offset is not None:
            offsets[TopicPartition(row["topic"], int(row["partition"]))] = OffsetAndMetadata(
                int(max_offset) + 1, ""
            )
    if not offsets:
        return
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )
    try:
        consumer.assign(list(offsets))
        consumer.commit(offsets=offsets)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

