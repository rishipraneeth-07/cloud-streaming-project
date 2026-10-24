
from pathlib import Path

from pyspark.sql import SparkSession

project_root = Path(__file__).resolve().parents[1]
output_path = project_root / "data" / "streaming" / "orders"

spark = (
    SparkSession.builder
    .appName("VerifyStreamingOutput")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(str(output_path))

    print("Total processed records:", df.count())
    print("\nSchema:")
    df.printSchema()

    print("\nSample processed orders:")
    df.select(
        "order_id",
        "product",
        "quantity",
        "unit_price",
        "order_status",
        "line_total",
    ).show(20, truncate=False)

finally:
    spark.stop()
