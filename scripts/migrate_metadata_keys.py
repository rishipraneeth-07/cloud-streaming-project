
import json
import os
import tempfile
from pathlib import Path

from minio import Minio

ROOT_DIR = Path(__file__).resolve().parents[1]
METADATA_FILE = ROOT_DIR / "config" / "object_metadata.json"
BACKUP_FILE = ROOT_DIR / "config" / "object_metadata.json.bak"

BUCKETS = ("hot-data", "warm-data", "cold-data")


def create_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def save_json_atomically(path, data):
    temporary_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
        ) as file:
            temporary_path = Path(file.name)
            json.dump(data, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        temporary_path.replace(path)

    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def main():
    if not METADATA_FILE.exists():
        raise FileNotFoundError(METADATA_FILE)

    if not BACKUP_FILE.exists():
        raise FileNotFoundError(
            f"Backup not found: {BACKUP_FILE}"
        )

    with open(METADATA_FILE, encoding="utf-8") as file:
        metadata = json.load(file)

    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a JSON object")

    client = create_minio_client()
    object_locations = {}

    for bucket in BUCKETS:
        if not client.bucket_exists(bucket):
            continue

        for obj in client.list_objects(bucket, recursive=True):
            if obj.object_name in object_locations:
                raise RuntimeError(
                    "Ambiguous object key exists in multiple buckets: "
                    f"{obj.object_name}"
                )

            object_locations[obj.object_name] = bucket

    updated_metadata = {}

    for logical_key, record in metadata.items():
        if not isinstance(record, dict):
            raise ValueError(
                f"Invalid metadata record: {logical_key}"
            )

        bucket = object_locations.get(logical_key)

        if bucket is None:
            raise RuntimeError(
                f"No matching MinIO object found for metadata key: "
                f"{logical_key}"
            )

        qualified_key = f"{bucket}/{logical_key}"

        if qualified_key in metadata:
            raise FileExistsError(
                f"Qualified key already exists in original metadata: "
                f"{qualified_key}"
            )

        if qualified_key in updated_metadata:
            raise FileExistsError(
                f"Duplicate destination metadata key: {qualified_key}"
            )

        updated_metadata[qualified_key] = record.copy()

        print(f"{logical_key}")
        print(f"  -> {qualified_key}")

    if len(updated_metadata) != len(metadata):
        raise RuntimeError("Metadata record count changed unexpectedly")

    save_json_atomically(METADATA_FILE, updated_metadata)

    print(f"\nUpdated {len(updated_metadata)} metadata records.")
    print(f"Backup retained at: {BACKUP_FILE}")


if __name__ == "__main__":
    main()
