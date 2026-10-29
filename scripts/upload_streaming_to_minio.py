
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from minio import Minio
from minio.error import S3Error

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tiering.metadata import register_object

SOURCE_DIR = PROJECT_ROOT / "data" / "streaming" / "orders"
SPARK_METADATA_DIR = SOURCE_DIR / "_spark_metadata"
BUCKET_NAME = "hot-data"
OBJECT_PREFIX = "streaming/orders"


def load_committed_files(metadata_dir, source_dir):
    metadata_dir = Path(metadata_dir).resolve()
    source_dir = Path(source_dir).resolve()

    if not metadata_dir.is_dir():
        raise FileNotFoundError(
            f"Spark metadata directory not found: {metadata_dir}"
        )

    metadata_entries = sorted(
        path
        for path in metadata_dir.iterdir()
        if path.is_file() and path.name.isdigit()
    )

    if not metadata_entries:
        raise RuntimeError("No committed Spark metadata entries were found.")

    committed_files = {}

    for metadata_path in metadata_entries:
        lines = metadata_path.read_text(encoding="utf-8").splitlines()

        if not lines or lines[0] != "v1":
            raise ValueError(
                f"Unsupported Spark metadata format: {metadata_path}"
            )

        for line in lines[1:]:
            if not line.strip():
                continue

            record = json.loads(line)

            if record.get("action") != "add":
                continue

            if "path" not in record or "size" not in record:
                raise ValueError(
                    f"Missing path or size in {metadata_path}"
                )

            parsed_path = urlparse(record["path"])

            if parsed_path.scheme != "file" or parsed_path.netloc:
                raise ValueError(
                    f"Unexpected Spark output path: {record['path']}"
                )

            local_path = Path(unquote(parsed_path.path)).resolve()

            if not local_path.is_relative_to(source_dir):
                raise ValueError(
                    f"Spark output path is outside the expected directory: "
                    f"{local_path}"
                )

            committed_files[local_path] = record

    return committed_files


def create_minio_client():
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

    return client


def upload_committed_files(client, committed_files, source_dir):
    source_dir = Path(source_dir).resolve()

    uploaded = 0
    already_present = 0
    registered = 0
    skipped = 0

    for file_path, record in sorted(
        committed_files.items(),
        key=lambda item: str(item[0]),
    ):
        file_path = Path(file_path)

        if not file_path.is_file():
            print(f"SKIP: Committed file is missing locally: {file_path}")
            skipped += 1
            continue

        actual_size = file_path.stat().st_size

        if actual_size != record["size"]:
            raise RuntimeError(
                f"Size mismatch for {file_path}: "
                f"local={actual_size}, spark={record['size']}"
            )

        relative_path = file_path.relative_to(source_dir)
        object_name = f"{OBJECT_PREFIX}/{relative_path.as_posix()}"
        metadata_key = f"{BUCKET_NAME}/{object_name}"

        try:
            client.stat_object(BUCKET_NAME, object_name)
            already_present += 1
            print(f"Already present; not overwritten: {metadata_key}")

        except S3Error as error:
            if error.code not in ("NoSuchKey", "NoSuchObject", "NotFound"):
                raise

            client.fput_object(
                BUCKET_NAME,
                object_name,
                str(file_path),
                content_type="application/octet-stream",
            )

            # Verify that the uploaded object is visible in MinIO.
            client.stat_object(BUCKET_NAME, object_name)
            uploaded += 1
            print(f"Uploaded: {metadata_key}")

        modification_time_ms = record.get("modificationTime")

        if (
            isinstance(modification_time_ms, bool)
            or not isinstance(modification_time_ms, (int, float))
        ):
            register_object(
                object_key=metadata_key,
                timestamp_source="spark_modification_time_unconfirmed",
            )
        else:
            timestamp = datetime.fromtimestamp(
                modification_time_ms / 1000,
                tz=timezone.utc,
            ).isoformat()

            register_object(
                object_key=metadata_key,
                original_created_at=timestamp,
                timestamp_source="spark_modification_time_unconfirmed",
            )

        registered += 1

    summary = {
        "uploaded": uploaded,
        "already_present": already_present,
        "registered": registered,
        "skipped": skipped,
    }

    print("\nStreaming upload summary")
    print(f"New uploads: {uploaded}")
    print(f"Objects already present: {already_present}")
    print(f"Metadata registration attempted: {registered}")
    print(f"Missing local files skipped: {skipped}")
    print("Spark modification timestamps remain unconfirmed.")

    return summary


def main():
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(
            f"Streaming output directory not found: {SOURCE_DIR}"
        )

    committed_files = load_committed_files(
        SPARK_METADATA_DIR,
        SOURCE_DIR,
    )

    client = create_minio_client()

    upload_committed_files(
        client,
        committed_files,
        SOURCE_DIR,
    )


if __name__ == "__main__":
    main()

