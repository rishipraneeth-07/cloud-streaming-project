
import unittest
import uuid

from src.tiering.metadata_repository import (
    delete_metadata,
    get_metadata,
    save_metadata,
)


class TestMetadataRepository(unittest.TestCase):
    def setUp(self):
        self.key = f"test/{uuid.uuid4()}.parquet"

    def tearDown(self):
        delete_metadata(self.key)

    def test_save_and_retrieve_metadata(self):
        record = {
            "original_created_at": "2026-10-29T10:00:00+00:00",
            "timestamp_source": "pipeline_creation",
            "timestamp_is_original_creation_time": True,
            "registered_at": "2026-10-29T10:05:00+00:00",
        }

        save_metadata(self.key, record)
        result = get_metadata(self.key)

        self.assertIsNotNone(result)
        self.assertEqual(
            result["timestamp_source"],
            "pipeline_creation",
        )
        self.assertTrue(
            result["timestamp_is_original_creation_time"]
        )

    def test_missing_metadata_returns_none(self):
        self.assertIsNone(get_metadata(self.key))

    def test_empty_object_key_is_rejected(self):
        with self.assertRaises(ValueError):
            get_metadata("")

        with self.assertRaises(ValueError):
            save_metadata("", {})

        with self.assertRaises(ValueError):
            delete_metadata("")

    def test_non_dictionary_record_is_rejected(self):
        with self.assertRaises(TypeError):
            save_metadata(self.key, "invalid")

    def test_confirmed_timestamp_is_not_overwritten(self):
        original_record = {
            "original_created_at": "2026-10-29T10:00:00+00:00",
            "timestamp_source": "pipeline_creation",
            "timestamp_is_original_creation_time": True,
            "registered_at": "2026-10-29T10:05:00+00:00",
        }

        unconfirmed_record = {
            "original_created_at": "2026-10-30T12:00:00+00:00",
            "timestamp_source": "registration_time_estimate",
            "timestamp_is_original_creation_time": False,
            "registered_at": "2026-10-30T12:05:00+00:00",
        }

        save_metadata(self.key, original_record)
        save_metadata(self.key, unconfirmed_record)

        result = get_metadata(self.key)

        self.assertEqual(
            result["original_created_at"],
            "2026-10-29T10:00:00+00:00",
        )
        self.assertEqual(
            result["timestamp_source"],
            "pipeline_creation",
        )
        self.assertTrue(
            result["timestamp_is_original_creation_time"]
        )


if __name__ == "__main__":
    unittest.main()

