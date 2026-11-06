
from datetime import datetime, timezone

from src.tiering.metadata_repository import (
    delete_metadata,
    get_metadata,
    list_metadata,
    save_metadata,
)


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def _normalize_timestamp(timestamp):
    """Validate a timezone-aware ISO timestamp and convert it to UTC."""
    if not isinstance(timestamp, str):
        raise TypeError("Timestamp must be a string")

    parsed = datetime.fromisoformat(timestamp)

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")

    return parsed.astimezone(timezone.utc).isoformat()


def load_object_metadata():
    """Load all object metadata from PostgreSQL."""
    return list_metadata()


def save_object_metadata(metadata):
    """Save metadata records to PostgreSQL."""
    if not isinstance(metadata, dict):
        raise TypeError("Metadata must be a dictionary")

    for object_key, record in metadata.items():
        if not isinstance(object_key, str) or not object_key.strip():
            raise ValueError("Object key must be a non-empty string")

        if not isinstance(record, dict):
            raise TypeError(f"Metadata for {object_key} must be a dictionary")

        save_metadata(object_key, record)


def register_object(
    object_key,
    original_created_at=None,
    timestamp_source="unknown",
):
    """
    Register object metadata.

    Only pipeline_creation timestamps are considered confirmed.
    Existing confirmed timestamps are never overwritten.
    Untrusted timestamps cannot replace existing records.
    """
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    if not isinstance(timestamp_source, str) or not timestamp_source.strip():
        raise ValueError("Timestamp source must be a non-empty string")

    existing = get_metadata(object_key)

    if existing is not None:
        if existing.get("timestamp_is_original_creation_time") is True:
            return existing

        if timestamp_source != "pipeline_creation":
            return existing

    if original_created_at is None:
        timestamp = _utc_now()
        confirmed = False
        source = "registration_time_estimate"
    else:
        timestamp = _normalize_timestamp(original_created_at)
        confirmed = timestamp_source == "pipeline_creation"
        source = timestamp_source

    record = {
        "original_created_at": timestamp,
        "timestamp_source": source,
        "timestamp_is_original_creation_time": confirmed,
        "registered_at": _utc_now(),
    }

    save_metadata(object_key, record)

    saved_record = get_metadata(object_key)

    if saved_record is None:
        raise RuntimeError(
            f"Metadata registration failed for {object_key}"
        )

    if any(saved_record.get(key) != value for key, value in record.items()):
        raise RuntimeError(
            f"Metadata verification failed for {object_key}"
        )

    return saved_record


def preserve_object_metadata(source_key, destination_key):
    """
    Copy source metadata to destination and verify it before source removal.
    """
    if not isinstance(source_key, str) or not source_key.strip():
        raise ValueError("Source key must be a non-empty string")

    if not isinstance(destination_key, str) or not destination_key.strip():
        raise ValueError("Destination key must be a non-empty string")

    if source_key == destination_key:
        raise ValueError("Source and destination metadata keys must differ")

    source_record = get_metadata(source_key)

    if source_record is None:
        raise KeyError(f"No metadata exists for {source_key}")

    existing_destination = get_metadata(destination_key)

    if existing_destination is not None:
        source_comparable = {
            key: value
            for key, value in source_record.items()
            if key != "last_moved_at"
        }

        destination_comparable = {
            key: value
            for key, value in existing_destination.items()
            if key != "last_moved_at"
        }

        if source_comparable == destination_comparable:
            return existing_destination

        raise FileExistsError(
            f"Conflicting metadata already exists for {destination_key}"
        )

    record = source_record.copy()
    record["last_moved_at"] = _utc_now()

    save_metadata(destination_key, record)

    saved_record = get_metadata(destination_key)

    if saved_record is None:
        raise RuntimeError(
            f"Destination metadata was not saved for {destination_key}"
        )

    if any(saved_record.get(key) != value for key, value in record.items()):
        raise RuntimeError(
            f"Destination metadata verification failed for {destination_key}"
        )

    return saved_record


def complete_metadata_migration(source_key, destination_key):
    """
    Remove source metadata only after destination metadata is verified.
    """
    source_record = get_metadata(source_key)
    destination_record = get_metadata(destination_key)

    if source_record is None:
        if destination_record is not None:
            return True
        raise KeyError(f"Metadata missing for both {source_key} and {destination_key}")

    if destination_record is None:
        raise RuntimeError(
            f"Cannot remove source metadata: destination metadata is missing for {destination_key}"
        )

    source_comparable = {
        key: value
        for key, value in source_record.items()
        if key != "last_moved_at"
    }

    destination_comparable = {
        key: value
        for key, value in destination_record.items()
        if key != "last_moved_at"
    }

    if source_comparable != destination_comparable:
        raise RuntimeError(
            f"Metadata mismatch between {source_key} and {destination_key}"
        )

    delete_metadata(source_key)

    if get_metadata(source_key) is not None:
        raise RuntimeError(
            f"Source metadata deletion could not be verified for {source_key}"
        )

    return True

