import os
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False
)

if client.bucket_exists("hot-data"):
    print("Connected to MinIO successfully")
    print("Bucket 'hot-data' exists")
else:
    print("Connected to MinIO successfully")
    print("Bucket 'hot-data' does not exist")
