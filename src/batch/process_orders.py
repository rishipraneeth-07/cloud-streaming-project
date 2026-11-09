import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from minio import Minio


ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.common.pipeline_tracker import track_pipeline_run, update_pipeline_progress
from src.tiering.metadata import register_object


def create_minio_client():
    import os

    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY.")

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def upload_parquet_outputs(client, run_id, dataset_name, output_path):
    bucket = "hot-data"
    if not client.bucket_exists(bucket):
        raise RuntimeError(
            f"MinIO bucket does not exist: {bucket}. "
            "Run scripts/setup_tier_buckets.py first."
        )

    parquet_files = sorted(Path(output_path).glob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No Parquet files found in {output_path}")

    uploaded = []
    for parquet_file in parquet_files:
        object_name = (
            f"batch/{dataset_name}/run-{run_id}/{parquet_file.name}"
        )
        client.fput_object(
            bucket,
            object_name,
            str(parquet_file),
            content_type="application/octet-stream",
        )
        remote = client.stat_object(bucket, object_name)
        local_size = parquet_file.stat().st_size
        if remote.size != local_size:
            raise RuntimeError(
                f"MinIO size verification failed for {bucket}/{object_name}"
            )

        metadata_key = f"{bucket}/{object_name}"
        register_object(
            object_key=metadata_key,
            original_created_at=datetime.now(timezone.utc).isoformat(),
            timestamp_source="pipeline_creation",
        )
        uploaded.append(metadata_key)

    return uploaded



def main():
    with track_pipeline_run(
        "orders_batch_processing",
        "BATCH",
    ) as run:

        spark = (
            SparkSession.builder
            .appName("OrdersBatchProcessing")
            .master("local[*]")
            .getOrCreate()
        )

        try:
            input_path = ROOT_DIR / "data/sample/orders.csv"

            orders_df = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(input_path))
            )

            required_columns = [
                "order_id",
                "product",
                "quantity",
                "unit_price",
                "order_status",
            ]

            missing_columns = [
                column for column in required_columns
                if column not in orders_df.columns
            ]

            if missing_columns:
                raise ValueError(
                    f"Missing required columns: {missing_columns}"
                )

            original_count = orders_df.count()

            valid_condition = (
                col("order_id").isNotNull()
                & col("product").isNotNull()
                & col("quantity").isNotNull()
                & col("unit_price").isNotNull()
                & col("order_status").isNotNull()
                & (col("quantity") > 0)
                & (col("unit_price") >= 0)
            )

            orders_df = orders_df.filter(valid_condition)
            valid_count = orders_df.count()

            run["records_processed"] = valid_count
            run["records_failed"] = original_count - valid_count

            update_pipeline_progress(
                run["id"],
                run["records_processed"],
                run["records_failed"],
            )

            print("Original records:", original_count)
            print("Valid records:", valid_count)
            print("Invalid records:", original_count - valid_count)

            print("Original schema:")
            orders_df.printSchema()

            orders_df = orders_df.withColumn(
                "line_total",
                col("quantity") * col("unit_price"),
            )

            print("Transformed data:")
            orders_df.show(truncate=False)

            completed_orders_df = orders_df.filter(
                col("order_status") == "COMPLETED"
            )

            product_summary_df = (
                completed_orders_df
                .groupBy("product")
                .agg(
                    sum("line_total").alias("total_revenue"),
                    count("*").alias("order_lines"),
                )
            )

            print("Product-wise revenue summary:")
            product_summary_df.show(truncate=False)

            orders_output_path = ROOT_DIR / "data/processed/orders"
            summary_output_path = ROOT_DIR / "data/processed/product_summary"

            orders_df.write \
                .mode("overwrite") \
                .parquet(str(orders_output_path))

            orders_created_at = datetime.now(timezone.utc).isoformat()

            print("Orders saved to:", orders_output_path)

            product_summary_df.write \
                .mode("overwrite") \
                .parquet(str(summary_output_path))

            summary_created_at = datetime.now(timezone.utc).isoformat()

            print("Product summary saved to:", summary_output_path)

            minio_client = create_minio_client()
            for dataset_name, output_path in [
                ("orders", orders_output_path),
                ("product_summary", summary_output_path),
            ]:
                uploaded_objects = upload_parquet_outputs(
                    minio_client,
                    run["id"],
                    dataset_name,
                    output_path,
                )
                for object_key in uploaded_objects:
                    print("Uploaded and verified:", object_key)

            saved_orders_df = spark.read.parquet(
                str(orders_output_path)
            )

            saved_summary_df = spark.read.parquet(
                str(summary_output_path)
            )

            print(
                "Orders read back from Parquet:",
                saved_orders_df.count(),
            )

            print(
                "Product summaries read back:",
                saved_summary_df.count(),
            )

            saved_orders_df.printSchema()

            print("Verified product summary:")
            saved_summary_df.orderBy("product").show(truncate=False)

        finally:
            spark.stop()


if __name__ == "__main__":
    main()
