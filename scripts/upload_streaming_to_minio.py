
import os
from pathlib import Path

from minio import Minio

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "data" / "streaming" / "orders"
BUCKET_NAME = "hot-data"
OBJECT_PREFIX = "streaming/orders"

endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")

if not access_key or not secret_key:
    raise RuntimeError(
        "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY before running this script."
    )

client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False,
)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

parquet_files = list(SOURCE_DIR.rglob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(f"No Parquet files found under {SOURCE_DIR}")

for file_path in parquet_files:
    relative_path = file_path.relative_to(SOURCE_DIR)
    object_name = f"{OBJECT_PREFIX}/{relative_path.as_posix()}"

    client.fput_object(
        BUCKET_NAME,
        object_name,
        str(file_path),
        content_type="application/octet-stream",
    )

    print(f"Uploaded: {BUCKET_NAME}/{object_name}")

print(f"Finished uploading {len(parquet_files)} Parquet files.")

