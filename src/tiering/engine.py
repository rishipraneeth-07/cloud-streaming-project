
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
from src.tiering.metadata_repository import (
    create_migration_record,
    update_migration_status,
)
from src.tiering.policy import load_tiering_rules, get_target_tier


def create_minio_client():
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set"
        )

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def calculate_age_days(original_created_at):
    """Calculate object age using its trusted original creation timestamp."""
    created_at = datetime.fromisoformat(original_created_at)

    if created_at.tzinfo is None:
        raise ValueError("Creation timestamp must include a timezone")

    age_days = (
        datetime.now(timezone.utc)
        - created_at.astimezone(timezone.utc)
    ).total_seconds() / 86400

    if age_days < 0:
        raise ValueError("Creation timestamp is in the future")

    return age_days


def calculate_object_sha256(client, bucket_name, object_name):
    """Calculate SHA-256 by streaming an object from MinIO."""
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
    """Check whether a destination object exists."""
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
    """Migrate one object with integrity and metadata verification."""
    if source_bucket == target_bucket:
        raise ValueError("Source and destination buckets must differ")

    if not object_name:
        raise ValueError("Object name cannot be empty")

    source_key = f"{source_bucket}/{object_name}"
    destination_key = f"{target_bucket}/{object_name}"

    source_stat = client.stat_object(source_bucket, object_name)

    if destination_exists(client, target_bucket, object_name):
        raise FileExistsError(
            f"Destination object already exists: {destination_key}"
        )

    migration_id = create_migration_record(
        source_bucket,
        object_name,
        target_bucket,
        object_name,
    )

    destination_created = False
    source_deleted = False

    try:
        client.copy_object(
            target_bucket,
            object_name,
            CopySource(source_bucket, object_name),
        )
        destination_created = True

        destination_stat = client.stat_object(
            target_bucket,
            object_name,
        )

        if destination_stat.size != source_stat.size:
            raise RuntimeError("Object size verification failed")

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
            raise RuntimeError("SHA-256 verification failed")

        preserve_object_metadata(source_key, destination_key)

        # Remove the source only after object and metadata verification.
        client.remove_object(source_bucket, object_name)
        source_deleted = True

        complete_metadata_migration(source_key, destination_key)

        update_migration_status(migration_id, "COMPLETED")

        print(
            f"MIGRATED: {source_key} -> {destination_key} "
            f"(SHA-256 verified)"
        )

        return True

    except Exception as error:
        # Keep the destination if the source has already been deleted.
        # It may be needed to recover a metadata/database failure.
        if destination_created and not source_deleted:
            try:
                client.remove_object(target_bucket, object_name)
                print(f"Removed incomplete destination: {destination_key}")
            except Exception as cleanup_error:
                print(
                    f"WARNING: Could not remove incomplete destination "
                    f"{destination_key}: {cleanup_error}"
                )

        try:
            update_migration_status(
                migration_id,
                "FAILED",
                str(error)[:2000],
            )
        except Exception as status_error:
            print(
                f"WARNING: Could not update migration history: {status_error}"
            )

        raise


def preview_tiering():
    """Preview or execute migrations according to the configured dry-run flag."""
    rules = load_tiering_rules()
    client = create_minio_client()
    metadata = load_object_metadata()

    dry_run = rules.get("dry_run", True)

    if not isinstance(dry_run, bool):
        raise ValueError("dry_run must be a boolean in tiering rules")

    print("\nStorage tiering execution")
    print(f"Dry run: {dry_run}")
    print(f"Metadata records loaded: {len(metadata)}")

    eligible = 0
    skipped = 0
    migrated = 0

    for object_key, record in metadata.items():
        if not isinstance(record, dict):
            print(f"SKIP {object_key}: invalid metadata")
            skipped += 1
            continue

        if record.get("timestamp_is_original_creation_time") is not True:
            print(f"SKIP {object_key}: creation timestamp is unconfirmed")
            skipped += 1
            continue

        original_created_at = record.get("original_created_at")

        if not original_created_at:
            print(f"SKIP {object_key}: creation timestamp is missing")
            skipped += 1
            continue

        try:
            age_days = calculate_age_days(original_created_at)
        except (TypeError, ValueError) as error:
            print(f"SKIP {object_key}: invalid creation timestamp: {error}")
            skipped += 1
            continue

        parts = object_key.split("/", 1)

        if len(parts) != 2:
            print(f"SKIP {object_key}: invalid bucket-prefixed object key")
            skipped += 1
            continue

        source_bucket, object_name = parts

        try:
            target_bucket = get_target_tier(
                source_bucket,
                age_days,
                rules,
            )
        except Exception as error:
            print(f"SKIP {object_key}: tier policy error: {error}")
            skipped += 1
            continue

        if not target_bucket or target_bucket == source_bucket:
            skipped += 1
            continue

        eligible += 1

        if dry_run:
            print(
                f"WOULD MIGRATE: {object_key} -> "
                f"{target_bucket}/{object_name} "
                f"(age: {age_days:.1f} days)"
            )
            continue

        try:
            if migrate_object(
                client,
                source_bucket,
                object_name,
                target_bucket,
            ):
                migrated += 1
        except Exception as error:
            print(f"FAILED: {object_key}: {error}")

    print("\nTiering summary")
    print(f"Eligible objects: {eligible}")
    print(f"Skipped objects: {skipped}")
    print(f"Successfully migrated: {migrated}")

    if dry_run:
        print("DRY RUN: No MinIO objects were modified.")

