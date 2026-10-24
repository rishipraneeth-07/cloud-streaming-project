
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio


ROOT_DIR = Path(__file__).resolve().parents[1]
METADATA_FILE = ROOT_DIR / "config" / "object_metadata.json"

BUCKETS = ["hot-data", "warm-data", "cold-data"]


def create_minio_client():
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables"
        )

    return Minio(
        "localhost:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def main():
    client = create_minio_client()

    if METADATA_FILE.exists():
        with open(METADATA_FILE, "r", encoding="utf-8") as file:
            metadata = json.load(file)
    else:
        metadata = {}

    added = 0
    skipped = 0

    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            print(f"Skipping missing bucket: {bucket}")
            continue

        for obj in client.list_objects(bucket, recursive=True):
            key = obj.object_name

            if key in metadata:
                skipped += 1
                continue

            # Use MinIO's object modification time as the initial
            # timestamp when no original creation time is available.
            modified_at = obj.last_modified

            if modified_at is None:
                print(f"SKIP {bucket}/{key}: modification time unavailable")
                continue

            if modified_at.tzinfo is None:
                modified_at = modified_at.replace(tzinfo=timezone.utc)

            metadata[key] = {
                "original_created_at": modified_at.astimezone(
                    timezone.utc
                ).isoformat(),
                "timestamp_source": "minio_last_modified",
                "timestamp_is_original_creation_time": False,
            }

            added += 1
            print(f"ADDED metadata for {bucket}/{key}")

    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(METADATA_FILE, "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)
        file.write("\n")

    print(f"\nMetadata initialization complete.")
    print(f"New records: {added}")
    print(f"Existing records preserved: {skipped}")
    print(f"Metadata file: {METADATA_FILE}")


if __name__ == "__main__":
    main()
