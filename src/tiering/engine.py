
import json
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio

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
    access_key = __import__("os").environ.get("MINIO_ACCESS_KEY")
    secret_key = __import__("os").environ.get("MINIO_SECRET_KEY")

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
        datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)
    ).total_seconds()

    if age_seconds < 0:
        raise ValueError("Object creation time cannot be in the future")

    return age_seconds / 86400


def preview_tiering():
    client = create_minio_client()
    rules = load_tiering_rules()
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
                    f"SKIP {key}: original creation time is unknown; "
                    "record it before making a tiering decision"
                )
                continue

            age_days = calculate_age_days(record["original_created_at"])
            target_tier = get_target_tier(age_days, rules)
            target_bucket = tier_to_bucket[target_tier]

            if target_bucket == source_bucket:
                print(
                    f"KEEP {key}: age={age_days:.2f} days, "
                    f"tier={target_tier.upper()}"
                )
            else:
                print(
                    f"WOULD MOVE {key} -> {target_bucket}: "
                    f"age={age_days:.2f} days, "
                    f"target={target_tier.upper()}"
                )


if __name__ == "__main__":
    preview_tiering()
