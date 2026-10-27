from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from datetime import datetime, timedelta, timezone

from src.tiering.engine import calculate_age_days


class TestTieringEngine(unittest.TestCase):

    def test_recent_object_age(self):
        created_at = datetime.now(timezone.utc) - timedelta(days=2)
        age = calculate_age_days(created_at.isoformat())

        self.assertGreaterEqual(age, 2)
        self.assertLess(age, 2.01)

    def test_old_object_age(self):
        created_at = datetime.now(timezone.utc) - timedelta(days=35)
        age = calculate_age_days(created_at.isoformat())

        self.assertGreaterEqual(age, 35)
        self.assertLess(age, 35.01)

    def test_future_timestamp_raises_error(self):
        created_at = datetime.now(timezone.utc) + timedelta(days=1)

        with self.assertRaises(ValueError):
            calculate_age_days(created_at.isoformat())

    def test_timestamp_confirmation_values(self):
        confirmed_record = {
            "original_created_at": "2026-10-20T00:00:00+00:00",
            "timestamp_is_original_creation_time": True,
        }
        estimated_record = {
            "original_created_at": "2026-10-20T00:00:00+00:00",
            "timestamp_is_original_creation_time": False,
        }
        unknown_record = {
            "original_created_at": "2026-10-20T00:00:00+00:00",
        }

        self.assertIs(
            confirmed_record.get("timestamp_is_original_creation_time"),
            True,
        )
        self.assertIs(
            estimated_record.get("timestamp_is_original_creation_time"),
            False,
        )
        self.assertIsNot(
            unknown_record.get("timestamp_is_original_creation_time"),
            True,
        )

    @patch("src.tiering.engine.load_object_metadata")
    @patch("src.tiering.engine.load_tiering_rules")
    @patch("src.tiering.engine.create_minio_client")
    def test_preview_skips_unconfirmed_timestamp(
        self,
        mock_create_client,
        mock_load_rules,
        mock_load_metadata,
    ):
        from src.tiering.engine import preview_tiering

        client = mock_create_client.return_value
        client.bucket_exists.return_value = True

        obj = SimpleNamespace(object_name="example.parquet")
        client.list_objects.side_effect = (
            lambda bucket, recursive=True:
            [obj] if bucket == "hot-data" else []
        )

        mock_load_rules.return_value = {
            "tiers": {
                "hot": {"bucket": "hot-data", "max_age_days": 7},
                "warm": {"bucket": "warm-data", "max_age_days": 30},
                "cold": {"bucket": "cold-data", "max_age_days": None},
            },
            "dry_run": True,
        }

        mock_load_metadata.return_value = {
            "example.parquet": {
                "original_created_at": "2026-10-20T00:00:00+00:00",
                "timestamp_is_original_creation_time": False,
            }
        }

        output = StringIO()
        with redirect_stdout(output):
            preview_tiering()

        self.assertIn(
            "original creation time is not confirmed",
            output.getvalue(),
        )
        self.assertNotIn("DRY RUN", output.getvalue())


if __name__ == "__main__":
    unittest.main()


