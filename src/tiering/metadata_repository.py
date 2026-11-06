\
import os
from datetime import datetime
from pathlib import Path

import psycopg


ROOT_DIR = Path(__file__).resolve().parents[2]


def load_env_file():
    """Load missing environment variables from the project's .env file."""
    required = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")

    if all(os.environ.get(key) for key in required):
        return

    env_file = ROOT_DIR / ".env"

    if not env_file.exists():
        return

    try:
        lines = env_file.read_text(encoding="utf-8").splitlines()
    except PermissionError:
        return

    for line in lines:
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def get_connection():
    """Create a PostgreSQL connection using environment configuration."""
    load_env_file()

    required = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    missing = [key for key in required if not os.environ.get(key)]

    if missing:
        raise RuntimeError(
            f"Missing required database environment variables: {', '.join(missing)}"
        )

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _to_iso(value):
    """Convert a database timestamp to an ISO 8601 string."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    return value


def get_metadata(object_key):
    """Retrieve an object's metadata from PostgreSQL."""
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    original_created_at,
                    timestamp_source,
                    timestamp_is_original_creation_time,
                    registered_at,
                    last_moved_at
                FROM object_metadata
                WHERE object_key = %s
                """,
                (object_key,),
            )
            row = cursor.fetchone()

    if row is None:
        return None

    record = {
        "original_created_at": _to_iso(row[0]),
        "timestamp_source": row[1],
        "timestamp_is_original_creation_time": row[2],
        "registered_at": _to_iso(row[3]),
    }

    if row[4] is not None:
        record["last_moved_at"] = _to_iso(row[4])

    return record


def list_metadata():
    """Retrieve all object metadata keyed by object key."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    object_key,
                    original_created_at,
                    timestamp_source,
                    timestamp_is_original_creation_time,
                    registered_at,
                    last_moved_at
                FROM object_metadata
                ORDER BY object_key
                """
            )
            rows = cursor.fetchall()

    metadata = {}

    for row in rows:
        record = {
            "original_created_at": _to_iso(row[1]),
            "timestamp_source": row[2],
            "timestamp_is_original_creation_time": row[3],
            "registered_at": _to_iso(row[4]),
        }

        if row[5] is not None:
            record["last_moved_at"] = _to_iso(row[5])

        metadata[row[0]] = record

    return metadata


def save_metadata(object_key, record):
    """
    Insert or update metadata without replacing trusted timestamps.

    An unconfirmed timestamp can only be upgraded using a
    confirmed pipeline_creation timestamp.
    """
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    if not isinstance(record, dict):
        raise TypeError("Metadata record must be a dictionary")

    timestamp_source = record.get("timestamp_source", "unknown")
    confirmed = record.get("timestamp_is_original_creation_time", False)

    if not isinstance(timestamp_source, str) or not timestamp_source.strip():
        raise ValueError("Timestamp source must be a non-empty string")

    if not isinstance(confirmed, bool):
        raise TypeError("Timestamp confirmation must be a boolean")

    if confirmed and timestamp_source != "pipeline_creation":
        raise ValueError(
            "Only pipeline_creation timestamps can be confirmed"
        )

    created_at = record.get("original_created_at")

    if not created_at:
        raise ValueError("Original creation timestamp is required")

    registered_at = record.get("registered_at")
    last_moved_at = record.get("last_moved_at")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO object_metadata (
                    object_key,
                    original_created_at,
                    timestamp_source,
                    timestamp_is_original_creation_time,
                    registered_at,
                    last_moved_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s::timestamptz, CURRENT_TIMESTAMP),
                    %s
                )
                ON CONFLICT (object_key) DO UPDATE SET

                    original_created_at = CASE
                        WHEN object_metadata.timestamp_is_original_creation_time
                            THEN object_metadata.original_created_at
                        WHEN EXCLUDED.timestamp_is_original_creation_time
                            AND EXCLUDED.timestamp_source = 'pipeline_creation'
                            THEN EXCLUDED.original_created_at
                        ELSE object_metadata.original_created_at
                    END,

                    timestamp_source = CASE
                        WHEN object_metadata.timestamp_is_original_creation_time
                            THEN object_metadata.timestamp_source
                        WHEN EXCLUDED.timestamp_is_original_creation_time
                            AND EXCLUDED.timestamp_source = 'pipeline_creation'
                            THEN EXCLUDED.timestamp_source
                        ELSE object_metadata.timestamp_source
                    END,

                    timestamp_is_original_creation_time =
                        object_metadata.timestamp_is_original_creation_time
                        OR (
                            EXCLUDED.timestamp_is_original_creation_time
                            AND EXCLUDED.timestamp_source = 'pipeline_creation'
                        ),

                    registered_at = CASE
                        WHEN object_metadata.timestamp_is_original_creation_time
                            THEN object_metadata.registered_at
                        WHEN EXCLUDED.timestamp_is_original_creation_time
                            AND EXCLUDED.timestamp_source = 'pipeline_creation'
                            THEN EXCLUDED.registered_at
                        ELSE object_metadata.registered_at
                    END,

                    last_moved_at = COALESCE(
                        EXCLUDED.last_moved_at,
                        object_metadata.last_moved_at
                    )
                """,
                (
                    object_key,
                    created_at,
                    timestamp_source,
                    confirmed,
                    registered_at,
                    last_moved_at,
                ),
            )


def delete_metadata(object_key):
    """Delete one metadata record by its object key."""
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM object_metadata WHERE object_key = %s",
                (object_key,),
            )
            return cursor.rowcount == 1


def create_migration_record(
    source_bucket,
    source_key,
    destination_bucket,
    destination_key,
):
    """Create a migration history record with PENDING status."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration_history (
                    source_bucket,
                    source_key,
                    destination_bucket,
                    destination_key,
                    status
                )
                VALUES (%s, %s, %s, %s, 'PENDING')
                RETURNING id
                """,
                (
                    source_bucket,
                    source_key,
                    destination_bucket,
                    destination_key,
                ),
            )
            return cursor.fetchone()[0]


def update_migration_status(
    migration_id,
    status,
    error_message=None,
):
    """Update migration status and completion timestamp."""
    if status not in ("COMPLETED", "FAILED"):
        raise ValueError("Invalid migration status")

    if not isinstance(migration_id, int) or migration_id <= 0:
        raise ValueError("Migration ID must be a positive integer")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE migration_history
                SET
                    status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE id = %s
                """,
                (status, error_message, migration_id),
            )

            if cursor.rowcount != 1:
                raise KeyError(
                    f"Migration record not found: {migration_id}"
                )


def list_migration_history():
    """Retrieve migration history ordered by ID."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    source_bucket,
                    source_key,
                    destination_bucket,
                    destination_key,
                    status,
                    started_at,
                    completed_at,
                    error_message
                FROM migration_history
                ORDER BY id
                """
            )

            return cursor.fetchall()

