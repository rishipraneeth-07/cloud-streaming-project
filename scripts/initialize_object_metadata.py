
import argparse
import os
import sys
from datetime import timezone
from pathlib import Path

from minio import Minio


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.tiering.metadata import register_object  # noqa: E402
from src.tiering.metadata_repository import get_metadata  # noqa: E402


BUCKETS = ["hot-data", "warm-data", "cold-data"]


def create_minio_client():
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables"
        )

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Initialize object metadata in PostgreSQL"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write new metadata records to PostgreSQL",
    )
    args = parser.parse_args()

    client = create_minio_client()

    discovered = 0
    new_records = []
    existing_records = 0
    skipped_records = 0

    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            print(f"Skipping missing bucket: {bucket}")
            continue

        for obj in client.list_objects(bucket, recursive=True):
            discovered += 1
            object_key = f"{bucket}/{obj.object_name}"

            if obj.last_modified is None:
                print(f"SKIP {object_key}: modification time unavailable")
                skipped_records += 1
                continue

            modified_at = obj.last_modified

            if modified_at.tzinfo is None:
                modified_at = modified_at.replace(tzinfo=timezone.utc)

            existing = get_metadata(object_key)

            if existing is not None:
                existing_records += 1
                continue

            timestamp = modified_at.astimezone(timezone.utc).isoformat()
            new_records.append((object_key, timestamp))

    print("\nMetadata initialization preview")
    print(f"Objects discovered: {discovered}")
    print(f"New records: {len(new_records)}")
    print(f"Existing records preserved: {existing_records}")
    print(f"Skipped records: {skipped_records}")

    if not args.apply:
        print("\nDRY RUN: PostgreSQL was not modified.")
        print("Run with --apply to register the new records.")
        for object_key, _ in new_records:
            print(f"WOULD REGISTER: {object_key}")
        return

    registered = 0

    for object_key, timestamp in new_records:
        result = register_object(
            object_key,
            timestamp,
            timestamp_source="minio_last_modified",
        )

        if result is None:
            raise RuntimeError(
                f"Metadata registration returned no record: {object_key}"
            )

        registered += 1
        print(f"REGISTERED: {object_key}")

    print("\nPostgreSQL metadata initialization complete.")
    print(f"New unconfirmed records registered: {registered}")
    print("Existing records were preserved.")
    print("No MinIO objects were copied, moved, or deleted.")


if __name__ == "__main__":
    main()

