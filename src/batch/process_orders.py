
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count


ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from src.tiering.metadata import register_object


spark = (
    SparkSession.builder
    .appName("OrdersBatchProcessing")
    .master("local[*]")
    .getOrCreate()
)

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
    spark.stop()
    raise ValueError(f"Missing required columns: {missing_columns}")

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

print("Total orders:", orders_df.count())

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

# Register metadata for the batch outputs in MinIO.
# These timestamps represent the pipeline output generation time.
bucket_name = "hot-data"

for dataset_name, output_path, created_at in [
    ("orders", orders_output_path, orders_created_at),
    ("product_summary", summary_output_path, summary_created_at),
]:
    parquet_files = list(output_path.glob("*.parquet"))

    if not parquet_files:
        raise RuntimeError(
            f"No Parquet files found in {output_path}"
        )

    for parquet_file in parquet_files:
        object_key = (
            f"{bucket_name}/batch/{dataset_name}/{parquet_file.name}"
        )

        register_object(
            object_key=object_key,
            original_created_at=created_at,
            timestamp_source="pipeline_creation",
        )

        print("Registered metadata:", object_key)

saved_orders_df = spark.read.parquet(str(orders_output_path))
saved_summary_df = spark.read.parquet(str(summary_output_path))

print("Orders read back from Parquet:", saved_orders_df.count())
print("Product summaries read back:", saved_summary_df.count())

saved_orders_df.printSchema()

print("Verified product summary:")
saved_summary_df.orderBy("product").show(truncate=False)

spark.stop()

