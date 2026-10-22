import os

from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

buckets = ["hot-data", "warm-data", "cold-data"]

for bucket in buckets:
    if client.bucket_exists(bucket):
        print(f"Bucket already exists: {bucket}")
    else:
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")

