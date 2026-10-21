from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,count

spark = (
    SparkSession.builder
    .appName("OrdersBatchProcessing")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/sample/orders.csv"

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

required_columns = [
    "order_id",
    "product",
    "quantity",
    "unit_price",
    "order_status"
]

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
    col("quantity") * col("unit_price")
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
        count("*").alias("order_lines")
    )
)

print("Product-wise revenue summary:")
product_summary_df.show(truncate=False)

orders_output_path = "data/processed/orders"
summary_output_path = "data/processed/product_summary"

orders_df.write \
    .mode("overwrite") \
    .parquet(orders_output_path)

product_summary_df.write \
    .mode("overwrite") \
    .parquet(summary_output_path)

print("Orders saved to:", orders_output_path)
print("Product summary saved to:", summary_output_path)

saved_orders_df = spark.read.parquet(orders_output_path)
saved_summary_df = spark.read.parquet(summary_output_path)

print("Orders read back from Parquet:", saved_orders_df.count())
print("Product summaries read back:", saved_summary_df.count())

saved_orders_df.printSchema()

print("Verified product summary:")
saved_summary_df.orderBy("product").show(truncate=False)

spark.stop()
