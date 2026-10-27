
import hashlib
import os
from datetime import datetime, timezone

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from src.tiering.metadata import (
    complete_metadata_migration,
    load_object_metadata,
    preserve_object_metadata,
)
from src.tiering.policy import load_tiering_rules, get_target_tier


def create_minio_client():
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set"
        )

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def calculate_age_days(original_created_at):
    created_at = datetime.fromisoformat(original_created_at)

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    age_days = (
        datetime.now(timezone.utc)
        - created_at.astimezone(timezone.utc)
    ).total_seconds() / 86400

    if age_days < 0:
        raise ValueError("Creation timestamp is in the future")

    return age_days


def calculate_object_sha256(client, bucket_name, object_name):
    response = client.get_object(bucket_name, object_name)

    try:
        sha256 = hashlib.sha256()

        for chunk in response.stream(1024 * 1024):
            sha256.update(chunk)

        return sha256.hexdigest()
    finally:
        response.close()
        response.release_conn()


def destination_exists(client, bucket_name, object_name):
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except S3Error as error:
        if error.code in (
            "NoSuchKey",
            "NoSuchObject",
            "NoSuchBucket",
            "NotFound",
        ):
            return False
        raise


def migrate_object(client, source_bucket, object_name, target_bucket):
    if source_bucket == target_bucket:
        raise ValueError("Source and destination buckets must differ")

    source_key = f"{source_bucket}/{object_name}"
    destination_key = f"{target_bucket}/{object_name}"

    source_stat = client.stat_object(source_bucket, object_name)

    if destination_exists(client, target_bucket, object_name):
        raise FileExistsError(
            f"Destination already exists: {destination_key}"
        )

    client.copy_object(
        target_bucket,
        object_name,
        CopySource(source_bucket, object_name),
    )

    # Track which stage has been reached so cleanup never deletes
    # the destination after source deletion has started.
    metadata_preservation_started = False
    source_removal_started = False

    try:
        destination_stat = client.stat_object(
            target_bucket,
            object_name,
        )

        if source_stat.size != destination_stat.size:
            raise RuntimeError(
                "Migration verification failed: object sizes do not match"
            )

        source_hash = calculate_object_sha256(
            client,
            source_bucket,
            object_name,
        )
        destination_hash = calculate_object_sha256(
            client,
            target_bucket,
            object_name,
        )

        if source_hash != destination_hash:
            raise RuntimeError(
                "Migration verification failed: SHA-256 checksums "
                "do not match"
            )

        # Preserve metadata before attempting source deletion.
        # If this stage fails, retain both object copies rather than
        # risk deleting data while metadata state is uncertain.
        metadata_preservation_started = True

        preserve_object_metadata(source_key, destination_key)

        # Once source deletion starts, never automatically delete
        # the destination in an exception handler.
        source_removal_started = True
        client.remove_object(source_bucket, object_name)

        # Remove the old metadata key only after source deletion succeeds.
        complete_metadata_migration(source_key, destination_key)

    except Exception:
        # Cleanup is permitted only before metadata preservation starts
        # and before source deletion is attempted.
        if not metadata_preservation_started and not source_removal_started:
            try:
                client.remove_object(target_bucket, object_name)
            except Exception as cleanup_error:
                print(
                    "WARNING: Destination cleanup failed. "
                    f"Both copies may remain: {cleanup_error}"
                )

        raise

    print(
        f"MIGRATED {source_key} -> {destination_key}"
    )


def preview_tiering():
    client = create_minio_client()
    config = load_tiering_rules()
    rules = config["tiers"]
    dry_run = config.get("dry_run", True)

    tier_to_bucket = {
        tier: details["bucket"]
        for tier, details in rules.items()
    }

    metadata = load_object_metadata()

    for source_tier in ("hot", "warm", "cold"):
        source_bucket = tier_to_bucket[source_tier]

        if not client.bucket_exists(source_bucket):
            print(f"Skipping missing bucket: {source_bucket}")
            continue

        for obj in client.list_objects(source_bucket, recursive=True):
            object_name = obj.object_name
            source_key = f"{source_bucket}/{object_name}"
            record = metadata.get(source_key)

            if record is None:
                print(
                    f"SKIP {source_key}: metadata record is missing"
                )
                continue

            if record.get("timestamp_is_original_creation_time") is not True:
                print(
                    f"SKIP {source_key}: original creation time "
                    "is not confirmed"
                )
                continue

            try:
                age_days = calculate_age_days(
                    record["original_created_at"]
                )
                target_tier = get_target_tier(age_days, rules)
                target_bucket = tier_to_bucket[target_tier]
            except (KeyError, TypeError, ValueError) as error:
                print(f"SKIP {source_key}: {error}")
                continue

            if target_bucket == source_bucket:
                print(
                    f"KEEP {source_key}: age={age_days:.2f} days, "
                    f"tier={target_tier.upper()}"
                )
                continue

            if dry_run:
                print(
                    f"DRY RUN: WOULD MOVE {source_key} "
                    f"-> {target_bucket}/{object_name}: "
                    f"age={age_days:.2f} days"
                )
                continue

            try:
                migrate_object(
                    client,
                    source_bucket,
                    object_name,
                    target_bucket,
                )
            except Exception as error:
                print(f"MIGRATION FAILED for {source_key}: {error}")


if __name__ == "__main__":
    preview_tiering()

