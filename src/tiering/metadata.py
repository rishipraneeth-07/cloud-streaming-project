
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
METADATA_FILE = ROOT_DIR / "config" / "object_metadata.json"


def load_object_metadata():
    if not METADATA_FILE.exists():
        return {}

    with open(METADATA_FILE, "r", encoding="utf-8") as file:
        metadata = json.load(file)

    if not isinstance(metadata, dict):
        raise ValueError("Object metadata must be a JSON object")

    return metadata


def save_object_metadata(metadata):
    if not isinstance(metadata, dict):
        raise TypeError("Metadata must be a dictionary")

    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = METADATA_FILE.with_suffix(".json.tmp")

    with open(temporary_file, "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)
        file.write("\n")

    temporary_file.replace(METADATA_FILE)


def register_object(
    object_key,
    original_created_at=None,
    timestamp_source="unknown",
):
    if not isinstance(object_key, str) or not object_key:
        raise ValueError("Object key must be a non-empty string")

    metadata = load_object_metadata()
    existing = metadata.get(object_key, {})

    if existing.get("timestamp_is_original_creation_time") is True:
        return existing

    if original_created_at is None:
        timestamp = datetime.now(timezone.utc).isoformat()
        confirmed = False
        source = "registration_time_estimate"
    else:
        parsed = datetime.fromisoformat(original_created_at)

        if parsed.tzinfo is None:
            raise ValueError("Timestamp must include a timezone")

        timestamp = parsed.astimezone(timezone.utc).isoformat()
        confirmed = timestamp_source == "pipeline_creation"
        source = timestamp_source

    record = {
        "original_created_at": timestamp,
        "timestamp_source": source,
        "timestamp_is_original_creation_time": confirmed,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }

    metadata[object_key] = record
    save_object_metadata(metadata)

    return record


def preserve_object_metadata(source_key, destination_key):
    if not source_key or not destination_key:
        raise ValueError("Source and destination keys are required")

    if source_key == destination_key:
        raise ValueError("Source and destination metadata keys must differ")

    metadata = load_object_metadata()

    if source_key not in metadata:
        raise KeyError(f"No metadata exists for {source_key}")

    if destination_key in metadata:
        raise FileExistsError(
            f"Metadata already exists for {destination_key}"
        )

    record = metadata[source_key].copy()
    record["last_moved_at"] = datetime.now(timezone.utc).isoformat()

    metadata[destination_key] = record
    save_object_metadata(metadata)

    return record


def complete_metadata_migration(source_key, destination_key):
    """Remove the source metadata record after the object move is complete."""
    if not source_key or not destination_key:
        raise ValueError("Source and destination metadata keys are required")

    metadata = load_object_metadata()

    if source_key not in metadata:
        raise KeyError(f"No metadata exists for {source_key}")

    if destination_key not in metadata:
        raise KeyError(f"No metadata exists for {destination_key}")

    if metadata[source_key] != {
        key: value
        for key, value in metadata[destination_key].items()
        if key != "last_moved_at"
    }:
        raise ValueError("Source and destination metadata do not match")

    del metadata[source_key]
    save_object_metadata(metadata)

