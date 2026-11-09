import argparse
import hashlib
import os

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error


SOURCE_BUCKET = "hot-data"
DESTINATION_BUCKET = "warm-data"
SOURCE_PREFIX = "batch/orders/"


def sha256_object(client, bucket, key):
    response = client.get_object(bucket, key)
    digest = hashlib.sha256()
    try:
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        response.close()
        response.release_conn()


def main():
    parser = argparse.ArgumentParser(
        description="Safely migrate batch order objects from hot to warm storage."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Perform migration. Without this flag, only preview objects.",
    )
    args = parser.parse_args()

    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    if not access_key or not secret_key:
        raise RuntimeError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY.")

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    if not client.bucket_exists(SOURCE_BUCKET):
        raise RuntimeError(f"Source bucket does not exist: {SOURCE_BUCKET}")
    if not client.bucket_exists(DESTINATION_BUCKET):
        raise RuntimeError(
            f"Destination bucket does not exist: {DESTINATION_BUCKET}"
        )

    objects = list(
        client.list_objects(
            SOURCE_BUCKET,
            prefix=SOURCE_PREFIX,
            recursive=True,
        )
    )

    if not objects:
        print("No matching objects found.")
        return

    for obj in objects:
        key = obj.object_name
        print(f"Object: {SOURCE_BUCKET}/{key} ({obj.size} bytes)")

        if not args.apply:
            print("DRY RUN: no object was copied or deleted.")
            continue

        try:
            client.stat_object(DESTINATION_BUCKET, key)
        except S3Error as error:
            if error.code not in ("NoSuchKey", "NoSuchObject", "NotFound"):
                raise
        else:
            print(
                f"SKIP: destination already exists; source preserved: "
                f"{DESTINATION_BUCKET}/{key}"
            )
            continue

        source_hash = sha256_object(client, SOURCE_BUCKET, key)
        client.copy_object(
            DESTINATION_BUCKET,
            key,
            CopySource(SOURCE_BUCKET, key),
        )

        destination = client.stat_object(DESTINATION_BUCKET, key)
        source = client.stat_object(SOURCE_BUCKET, key)

        if source.size != destination.size:
            raise RuntimeError(
                f"Size verification failed for {key}; source preserved."
            )

        destination_hash = sha256_object(client, DESTINATION_BUCKET, key)
        if source_hash != destination_hash:
            raise RuntimeError(
                f"SHA-256 verification failed for {key}; source preserved."
            )

        client.remove_object(SOURCE_BUCKET, key)
        print(f"MIGRATED and SHA-256 verified: {key}")


if __name__ == "__main__":
    main()
