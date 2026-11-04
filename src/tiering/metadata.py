
from datetime import datetime, timezone

from src.tiering.metadata_repository import (
    delete_metadata,
    get_metadata,
    list_metadata,
    save_metadata,
)


def load_object_metadata():
    """Load all object metadata from PostgreSQL."""
    return list_metadata()


def save_object_metadata(metadata):
    """Save metadata records to PostgreSQL."""
    if not isinstance(metadata, dict):
        raise TypeError("Metadata must be a dictionary")

    for object_key, record in metadata.items():
        save_metadata(object_key, record)


def register_object(
    object_key,
    original_created_at=None,
    timestamp_source="unknown",
):
    """Register an object without overwriting an existing confirmed timestamp."""
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    existing = get_metadata(object_key) or {}

    if existing.get("timestamp_is_original_creation_time") is True:
        return existing

    if original_created_at is None:
        timestamp = datetime.now(timezone.utc).isoformat()
        confirmed = False
        source = "registration_time_estimate"
    else:
        if not isinstance(original_created_at, str):
            raise TypeError("Creation timestamp must be a string")

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

    save_metadata(object_key, record)
    return get_metadata(object_key)


def preserve_object_metadata(source_key, destination_key):
    """Create destination metadata before the source metadata is removed."""
    if not source_key or not destination_key:
        raise ValueError("Source and destination keys are required")

    if source_key == destination_key:
        raise ValueError("Source and destination metadata keys must differ")

    source_record = get_metadata(source_key)

    if source_record is None:
        raise KeyError(f"No metadata exists for {source_key}")

    if get_metadata(destination_key) is not None:
        raise FileExistsError(
            f"Metadata already exists for {destination_key}"
        )

    record = source_record.copy()
    record["last_moved_at"] = datetime.now(timezone.utc).isoformat()

    save_metadata(destination_key, record)

    saved_record = get_metadata(destination_key)

    if saved_record is None:
        raise RuntimeError(
            f"Destination metadata was not saved for {destination_key}"
        )

    if any(
        saved_record.get(key) != value
        for key, value in record.items()
    ):
        raise RuntimeError(
            f"Destination metadata verification failed for {destination_key}"
        )

    return saved_record


def complete_metadata_migration(source_key, destination_key):
    """Remove source metadata after destination metadata is verified."""
    if not source_key or not destination_key:
        raise ValueError("Source and destination metadata keys are required")

    source_record = get_metadata(source_key)
    destination_record = get_metadata(destination_key)

    if source_record is None:
        raise KeyError(f"No metadata exists for {source_key}")

    if destination_record is None:
        raise KeyError(f"No metadata exists for {destination_key}")

    expected_source_record = {
        key: value
        for key, value in destination_record.items()
        if key != "last_moved_at"
    }

    if source_record != expected_source_record:
        raise ValueError("Source and destination metadata do not match")

    if not delete_metadata(source_key):
        raise RuntimeError(
            f"Failed to delete source metadata for {source_key}"
        )

