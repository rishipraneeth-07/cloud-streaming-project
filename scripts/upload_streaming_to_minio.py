
import os
import sys
from pathlib import Path

from minio import Minio

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tiering.metadata import register_object

SOURCE_DIR = PROJECT_ROOT / "data" / "streaming" / "orders"
BUCKET_NAME = "hot-data"
OBJECT_PREFIX = "streaming/orders"

endpoint = (
    os.getenv("MINIO_ENDPOINT", "localhost:9000")
    .replace("http://", "")
    .replace("https://", "")
)
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
    raise RuntimeError(f"MinIO bucket does not exist: {BUCKET_NAME}")

if not SOURCE_DIR.is_dir():
    raise FileNotFoundError(f"Streaming output directory not found: {SOURCE_DIR}")

parquet_files = list(SOURCE_DIR.rglob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(f"No Parquet files found under {SOURCE_DIR}")

uploaded = 0
registered = 0

for file_path in parquet_files:
    relative_path = file_path.relative_to(SOURCE_DIR)
    object_name = f"{OBJECT_PREFIX}/{relative_path.as_posix()}"
    metadata_key = f"{BUCKET_NAME}/{object_name}"

    client.fput_object(
        BUCKET_NAME,
        object_name,
        str(file_path),
        content_type="application/octet-stream",
    )
    uploaded += 1

    try:
        client.stat_object(BUCKET_NAME, object_name)

        # The local creation time is not currently recorded by the
        # streaming pipeline, so do not claim the upload time is original.
        register_object(
            object_key=metadata_key,
            timestamp_source="minio_last_modified",
        )
        registered += 1

    except Exception:
        print(
            f"WARNING: Upload succeeded, but metadata registration "
            f"needs recovery: {metadata_key}"
        )
        raise

    print(f"Uploaded and registered: {metadata_key}")

print(f"\nFinished uploading {uploaded} Parquet files.")
print(f"Metadata registration completed for {registered} objects.")

