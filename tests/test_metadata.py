import unittest
from unittest.mock import patch

from src.tiering import metadata


class TestObjectMetadata(unittest.TestCase):

    def setUp(self):
        self.store = {}

        self.get_patch = patch.object(
            metadata,
            "get_metadata",
            side_effect=lambda key: self.store.get(key),
        )
        self.list_patch = patch.object(
            metadata,
            "list_metadata",
            side_effect=lambda: self.store.copy(),
        )
        self.save_patch = patch.object(
            metadata,
            "save_metadata",
            side_effect=lambda key, record: self.store.update(
                {key: record.copy()}
            ),
        )
        self.delete_patch = patch.object(
            metadata,
            "delete_metadata",
            side_effect=lambda key: self.store.pop(key, None) is not None,
        )

        self.get_patch.start()
        self.list_patch.start()
        self.save_patch.start()
        self.delete_patch.start()

    def tearDown(self):
        self.get_patch.stop()
        self.list_patch.stop()
        self.save_patch.stop()
        self.delete_patch.stop()

    def test_load_missing_metadata_returns_empty_dictionary(self):
        self.assertEqual(metadata.load_object_metadata(), {})

    def test_register_object_with_pipeline_creation_timestamp(self):
        result = metadata.register_object(
            "orders/test.parquet",
            "2026-10-23T00:00:00+00:00",
            "pipeline_creation",
        )

        self.assertEqual(
            result["original_created_at"],
            "2026-10-23T00:00:00+00:00",
        )
        self.assertEqual(result["timestamp_source"], "pipeline_creation")
        self.assertTrue(result["timestamp_is_original_creation_time"])

    def test_register_object_with_unknown_timestamp(self):
        result = metadata.register_object("orders/test.parquet")

        self.assertEqual(
            result["timestamp_source"],
            "registration_time_estimate",
        )
        self.assertFalse(result["timestamp_is_original_creation_time"])
        self.assertIsNotNone(result["original_created_at"])

    def test_naive_timestamp_is_rejected(self):
        with self.assertRaises(ValueError):
            metadata.register_object(
                "orders/test.parquet",
                "2026-10-23T00:00:00",
                "pipeline_creation",
            )

    def test_non_pipeline_timestamp_is_not_confirmed(self):
        result = metadata.register_object(
            "orders/test.parquet",
            "2026-10-23T00:00:00+00:00",
            "spark_modification_time_unconfirmed",
        )

        self.assertFalse(result["timestamp_is_original_creation_time"])
        self.assertEqual(
            result["timestamp_source"],
            "spark_modification_time_unconfirmed",
        )

    def test_registration_does_not_overwrite_confirmed_timestamp(self):
        original = {
            "original_created_at": "2026-10-23T00:00:00+00:00",
            "timestamp_source": "pipeline_creation",
            "timestamp_is_original_creation_time": True,
            "registered_at": "2026-10-23T01:00:00+00:00",
        }
        self.store["orders/test.parquet"] = original.copy()

        result = metadata.register_object(
            "orders/test.parquet",
            "2026-10-24T00:00:00+00:00",
            "unknown",
        )

        self.assertEqual(result, original)

    def test_preserve_metadata_for_destination_key(self):
        source_key = "hot-data/orders/test.parquet"
        destination_key = "warm-data/orders/test.parquet"

        source_record = {
            "original_created_at": "2026-10-23T00:00:00+00:00",
            "timestamp_source": "pipeline_creation",
            "timestamp_is_original_creation_time": True,
            "registered_at": "2026-10-23T01:00:00+00:00",
        }
        self.store[source_key] = source_record.copy()

        result = metadata.preserve_object_metadata(
            source_key,
            destination_key,
        )

        self.assertEqual(
            result["original_created_at"],
            source_record["original_created_at"],
        )
        self.assertEqual(result["timestamp_source"], "pipeline_creation")
        self.assertIn("last_moved_at", result)
        self.assertIn(source_key, self.store)
        self.assertIn(destination_key, self.store)

    def test_preserving_missing_source_metadata_raises_error(self):
        with self.assertRaises(KeyError):
            metadata.preserve_object_metadata(
                "hot-data/missing.parquet",
                "warm-data/missing.parquet",
            )


if __name__ == "__main__":
    unittest.main()
