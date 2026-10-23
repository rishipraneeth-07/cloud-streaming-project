
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio
from minio.commonconfig import CopySource

from src.tiering.policy import get_target_tier, load_tiering_rules


ROOT_DIR = Path(__file__).resolve().parents[2]
METADATA_FILE = ROOT_DIR / "config" / "object_metadata.json"


def load_object_metadata():
    if not METADATA_FILE.exists():
        return {}

    with open(METADATA_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_object_metadata(metadata):
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(METADATA_FILE, "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)


def create_minio_client():
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY before running the engine"
        )

    return Minio(
        "localhost:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )


def calculate_age_days(original_created_at):
    created_at = datetime.fromisoformat(original_created_at)

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    age_seconds = (
        datetime.now(timezone.utc)
        - created_at.astimezone(timezone.utc)
    ).total_seconds()

    if age_seconds < 0:
        raise ValueError("Object creation time cannot be in the future")

    return age_seconds / 86400


def migrate_object(client, source_bucket, object_name, target_bucket):
    source = client.stat_object(source_bucket, object_name)

    client.copy_object(
        target_bucket,
        object_name,
        CopySource(source_bucket, object_name),
    )

    destination = client.stat_object(target_bucket, object_name)

    if destination.size != source.size:
        raise RuntimeError(
            f"Size verification failed for {object_name}; "
            "source object was preserved"
        )

    client.remove_object(source_bucket, object_name)

    print(
        f"MIGRATED {source_bucket}/{object_name} "
        f"-> {target_bucket}/{object_name}"
    )


def preview_tiering():
    client = create_minio_client()
    config = load_tiering_rules()
    rules = config["tiers"]
    dry_run = config.get("dry_run", True)
    metadata = load_object_metadata()

    tier_to_bucket = {
        tier: details["bucket"]
        for tier, details in rules.items()
    }

    for source_tier in ("hot", "warm", "cold"):
        source_bucket = tier_to_bucket[source_tier]

        if not client.bucket_exists(source_bucket):
            print(f"Skipping missing bucket: {source_bucket}")
            continue

        for obj in client.list_objects(source_bucket, recursive=True):
            key = obj.object_name
            record = metadata.get(key)

            if record is None:
                print(
                    f"SKIP {source_bucket}/{key}: "
                    "original creation time is unknown"
                )
                continue

            age_days = calculate_age_days(record["original_created_at"])
            target_tier = get_target_tier(age_days, rules)
            target_bucket = tier_to_bucket[target_tier]

            if target_bucket == source_bucket:
                print(
                    f"KEEP {source_bucket}/{key}: "
                    f"age={age_days:.2f} days, "
                    f"tier={target_tier.upper()}"
                )
                continue

            if dry_run:
                print(
                    f"DRY RUN: WOULD MOVE {source_bucket}/{key} "
                    f"-> {target_bucket}: age={age_days:.2f} days"
                )
                continue

            print(
                f"MIGRATION DISABLED FOR {source_bucket}/{key}: "
                "metadata-safe migration is not implemented yet"
            )


if __name__ == "__main__":
    preview_tiering()

