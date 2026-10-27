
import unittest
from unittest.mock import Mock, patch

from src.tiering.engine import preview_tiering


class TestTieringEngine(unittest.TestCase):

    def setUp(self):
        self.client = Mock()
        self.rules = {
            "hot": {"bucket": "hot-data", "max_age_days": 7},
            "warm": {"bucket": "warm-data", "max_age_days": 30},
            "cold": {"bucket": "cold-data"},
        }

    @patch("src.tiering.engine.load_object_metadata")
    @patch("src.tiering.engine.load_tiering_rules")
    @patch("src.tiering.engine.create_minio_client")
    def test_unconfirmed_timestamp_is_skipped(
        self,
        mock_create_client,
        mock_load_rules,
        mock_load_metadata,
    ):
        mock_create_client.return_value = self.client
        mock_load_rules.return_value = {
            "tiers": self.rules,
            "dry_run": True,
        }
        mock_load_metadata.return_value = {
            "hot-data/orders.parquet": {
                "original_created_at": "2026-01-01T00:00:00+00:00",
                "timestamp_is_original_creation_time": False,
            }
        }

        self.client.bucket_exists.return_value = True
        self.client.list_objects.return_value = [
            Mock(object_name="orders.parquet")
        ]

        with patch("builtins.print") as mock_print:
            preview_tiering()

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )
        self.assertIn("original creation time is not confirmed", output)

    @patch("src.tiering.engine.load_object_metadata")
    @patch("src.tiering.engine.load_tiering_rules")
    @patch("src.tiering.engine.create_minio_client")
    def test_missing_metadata_is_skipped(
        self,
        mock_create_client,
        mock_load_rules,
        mock_load_metadata,
    ):
        mock_create_client.return_value = self.client
        mock_load_rules.return_value = {
            "tiers": self.rules,
            "dry_run": True,
        }
        mock_load_metadata.return_value = {}

        self.client.bucket_exists.return_value = True
        self.client.list_objects.return_value = [
            Mock(object_name="orders.parquet")
        ]

        with patch("builtins.print") as mock_print:
            preview_tiering()

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )
        self.assertIn("metadata record is missing", output)

    @patch("src.tiering.engine.load_object_metadata")
    @patch("src.tiering.engine.load_tiering_rules")
    @patch("src.tiering.engine.create_minio_client")
    def test_missing_bucket_is_skipped(
        self,
        mock_create_client,
        mock_load_rules,
        mock_load_metadata,
    ):
        mock_create_client.return_value = self.client
        mock_load_rules.return_value = {
            "tiers": self.rules,
            "dry_run": True,
        }
        mock_load_metadata.return_value = {}

        self.client.bucket_exists.return_value = False

        with patch("builtins.print") as mock_print:
            preview_tiering()

        output = "\n".join(
            str(call.args[0]) for call in mock_print.call_args_list
        )
        self.assertIn("Skipping missing bucket", output)
        self.client.list_objects.assert_not_called()


if __name__ == "__main__":
    unittest.main()


