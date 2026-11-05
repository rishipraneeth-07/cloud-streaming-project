
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tiering.metadata_repository import (
    get_metadata,
    list_metadata,
    save_metadata,
)

JSON_FILE = PROJECT_ROOT / "config" / "object_metadata.json"


def main():
    if not JSON_FILE.is_file():
        raise FileNotFoundError(f"Metadata file not found: {JSON_FILE}")

    with JSON_FILE.open("r", encoding="utf-8") as file:
        json_records = json.load(file)

    if not isinstance(json_records, dict):
        raise ValueError("JSON metadata must be a dictionary")

    # Validate the entire input before writing anything.
    for object_key, record in json_records.items():
        if not isinstance(object_key, str) or not object_key.strip():
            raise ValueError("Found an invalid object key")

        if not isinstance(record, dict):
            raise ValueError(f"Invalid metadata record for {object_key}")

        confirmed = record.get(
            "timestamp_is_original_creation_time", False
        )
        if not isinstance(confirmed, bool):
            raise ValueError(
                f"Invalid timestamp confirmation for {object_key}"
            )

        timestamp = record.get("original_created_at")
        if timestamp is not None:
            if not isinstance(timestamp, str):
                raise ValueError(
                    f"Invalid timestamp value for {object_key}"
                )

            parsed = __import__("datetime").datetime.fromisoformat(timestamp)
            if parsed.tzinfo is None:
                raise ValueError(
                    f"Timestamp lacks timezone for {object_key}"
                )

    existing = list_metadata()
    imported = 0
    already_present = 0

    # Preserve existing PostgreSQL records; never overwrite them blindly.
    with __import__("contextlib").nullcontext():
        for object_key, record in json_records.items():
            current = get_metadata(object_key)

            if current is not None:
                if current != record:
                    raise RuntimeError(
                        "Existing PostgreSQL metadata differs from JSON "
                        f"for {object_key}; review this record manually."
                    )
                already_present += 1
                continue

            save_metadata(object_key, record)
            imported += 1

    final_records = list_metadata()
    missing = set(json_records) - set(final_records)

    if missing:
        raise RuntimeError(
            f"Import verification failed for {len(missing)} records"
        )

    print(f"JSON records: {len(json_records)}")
    print(f"Imported records: {imported}")
    print(f"Already matching: {already_present}")
    print(f"PostgreSQL records after import: {len(final_records)}")
    print("JSON source file was not modified.")


if __name__ == "__main__":
    main()
