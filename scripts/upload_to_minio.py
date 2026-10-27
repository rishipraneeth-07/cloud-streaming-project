
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from minio import Minio
from minio.error import S3Error

from src.tiering.metadata import register_object


client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000")
    .replace("http://", "")
    .replace("https://", ""),
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

bucket_name = "hot-data"

datasets = {
    "orders": ROOT_DIR / "data/processed/orders",
    "product_summary": ROOT_DIR / "data/processed/product_summary",
}

if not client.bucket_exists(bucket_name):
    raise RuntimeError(f"MinIO bucket does not exist: {bucket_name}")

for dataset_name, directory in datasets.items():
    if not directory.is_dir():
        print(f"Skipping missing directory: {directory}")
        continue

    parquet_files = list(directory.glob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in {directory}")
        continue

    for file_path in parquet_files:
        # Unique object name avoids overwriting existing data.
        object_name = (
            f"batch/{dataset_name}/"
            f"{uuid.uuid4().hex}/{file_path.name}"
        )
        metadata_key = f"{bucket_name}/{object_name}"
        created_at = datetime.now(timezone.utc).isoformat()

        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
        )

        try:
            client.stat_object(bucket_name, object_name)

            register_object(
                object_key=metadata_key,
                original_created_at=created_at,
                timestamp_source="pipeline_creation",
            )

        except Exception:
            # Keep the uploaded object if metadata registration fails.
            # It can be reconciled later without risking data loss.
            print(
                f"WARNING: Upload succeeded but metadata registration "
                f"needs recovery: {metadata_key}"
            )
            raise

        print(f"Uploaded and registered: {metadata_key}")

