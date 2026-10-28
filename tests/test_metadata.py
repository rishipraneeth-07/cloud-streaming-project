
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from src.tiering import metadata as metadata_module


class TestObjectMetadata(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.metadata_file = Path(self.temp_dir.name) / "object_metadata.json"

        self.metadata_patch = patch.object(
            metadata_module,
            "METADATA_FILE",
            self.metadata_file,
        )
        self.metadata_patch.start()
        self.addCleanup(self.metadata_patch.stop)
        self.addCleanup(self.temp_dir.cleanup)

    def test_load_missing_metadata_returns_empty_dictionary(self):
        self.assertEqual(metadata_module.load_object_metadata(), {})

    def test_register_object_with_unknown_timestamp(self):
        record = metadata_module.register_object(
            "batch/example.parquet"
        )

        self.assertFalse(
            record["timestamp_is_original_creation_time"]
        )
        self.assertEqual(
            record["timestamp_source"],
            "registration_time_estimate",
        )

    def test_register_object_with_pipeline_creation_timestamp(self):
        timestamp = "2026-10-20T10:00:00+00:00"

        record = metadata_module.register_object(
            "batch/example.parquet",
            original_created_at=timestamp,
            timestamp_source="pipeline_creation",
        )

        self.assertTrue(
            record["timestamp_is_original_creation_time"]
        )
        self.assertEqual(
            datetime.fromisoformat(record["original_created_at"]),
            datetime.fromisoformat(timestamp),
        )

    def test_non_pipeline_timestamp_is_not_confirmed(self):
        record = metadata_module.register_object(
            "streaming/example.parquet",
            original_created_at="2026-10-28T10:00:00+00:00",
            timestamp_source="minio_last_modified",
        )

        self.assertFalse(
            record["timestamp_is_original_creation_time"]
        )
        self.assertEqual(
            record["timestamp_source"],
            "minio_last_modified",
        )

    def test_registration_does_not_overwrite_confirmed_timestamp(self):
        original_timestamp = "2026-10-20T10:00:00+00:00"

        original_record = metadata_module.register_object(
            "batch/example.parquet",
            original_created_at=original_timestamp,
            timestamp_source="pipeline_creation",
        )

        updated_record = metadata_module.register_object(
            "batch/example.parquet"
        )

        self.assertEqual(
            updated_record["original_created_at"],
            original_record["original_created_at"],
        )
        self.assertTrue(
            updated_record["timestamp_is_original_creation_time"]
        )

    def test_preserve_metadata_for_destination_key(self):
        timestamp = "2026-10-20T10:00:00+00:00"

        source_record = metadata_module.register_object(
            "batch/example.parquet",
            original_created_at=timestamp,
            timestamp_source="pipeline_creation",
        )

        destination_record = metadata_module.preserve_object_metadata(
            "batch/example.parquet",
            "archive/example.parquet",
        )

        self.assertEqual(
            destination_record["original_created_at"],
            source_record["original_created_at"],
        )
        self.assertTrue(
            destination_record["timestamp_is_original_creation_time"]
        )
        self.assertIn(
            "archive/example.parquet",
            metadata_module.load_object_metadata(),
        )

    def test_preserving_missing_source_metadata_raises_error(self):
        with self.assertRaises(KeyError):
            metadata_module.preserve_object_metadata(
                "missing.parquet",
                "archive/missing.parquet",
            )

    def test_naive_timestamp_is_rejected(self):
        with self.assertRaises(ValueError):
            metadata_module.register_object(
                "batch/example.parquet",
                original_created_at="2026-10-20T10:00:00",
                timestamp_source="pipeline_creation",
            )


if __name__ == "__main__":
    unittest.main()

