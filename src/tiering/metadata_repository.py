
import os
from datetime import datetime
from pathlib import Path

import psycopg


ROOT_DIR = Path(__file__).resolve().parents[2]


def load_env_file():
    """Load missing environment variables from the project's .env file."""
    env_file = ROOT_DIR / ".env"

    if not env_file.exists():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
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

    return {
        "original_created_at": _to_iso(row[0]),
        "timestamp_source": row[1],
        "timestamp_is_original_creation_time": row[2],
        "registered_at": _to_iso(row[3]),
        **({"last_moved_at": _to_iso(row[4])} if row[4] is not None else {}),
    }


def save_metadata(object_key, record):
    """Insert or update an object's metadata in PostgreSQL."""
    if not isinstance(object_key, str) or not object_key.strip():
        raise ValueError("Object key must be a non-empty string")

    if not isinstance(record, dict):
        raise TypeError("Metadata record must be a dictionary")

    timestamp_source = record.get("timestamp_source", "unknown")
    confirmed = record.get("timestamp_is_original_creation_time", False)

    if not isinstance(timestamp_source, str) or not timestamp_source:
        raise ValueError("Timestamp source must be a non-empty string")

    if not isinstance(confirmed, bool):
        raise TypeError("Timestamp confirmation must be a boolean")

    created_at = record.get("original_created_at")
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
        ELSE EXCLUDED.original_created_at
    END,
    timestamp_source = CASE
        WHEN object_metadata.timestamp_is_original_creation_time
        THEN object_metadata.timestamp_source
        ELSE EXCLUDED.timestamp_source
    END,
    timestamp_is_original_creation_time =
        object_metadata.timestamp_is_original_creation_time
        OR EXCLUDED.timestamp_is_original_creation_time,
    registered_at = CASE
        WHEN object_metadata.timestamp_is_original_creation_time
        THEN object_metadata.registered_at
        ELSE EXCLUDED.registered_at
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
