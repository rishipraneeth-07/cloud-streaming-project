import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.batch.process_orders import upload_parquet_outputs


class TestBatchMinioUpload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.output_dir = Path(self.temp_dir.name)
        self.parquet_file = self.output_dir / "part-00000.snappy.parquet"
        self.parquet_file.write_bytes(b"parquet-data")
        self.client = Mock()
        self.client.bucket_exists.return_value = True
        self.client.stat_object.return_value = Mock(
            size=self.parquet_file.stat().st_size
        )

    @patch("src.batch.process_orders.register_object")
    def test_uploads_verifies_and_registers_parquet(self, register_object):
        keys = upload_parquet_outputs(
            self.client,
            run_id=47,
            dataset_name="orders",
            output_path=self.output_dir,
        )

        self.assertEqual(len(keys), 1)
        self.assertTrue(keys[0].startswith("hot-data/batch/orders/run-47/"))
        self.client.fput_object.assert_called_once()
        self.client.stat_object.assert_called_once_with(
            "hot-data", keys[0].removeprefix("hot-data/")
        )
        register_object.assert_called_once()
        self.assertEqual(register_object.call_args.kwargs["object_key"], keys[0])
        self.assertEqual(
            register_object.call_args.kwargs["timestamp_source"],
            "pipeline_creation",
        )

    @patch("src.batch.process_orders.register_object")
    def test_size_mismatch_fails_before_metadata_registration(self, register_object):
        self.client.stat_object.return_value = Mock(size=1)

        with self.assertRaisesRegex(RuntimeError, "size verification failed"):
            upload_parquet_outputs(
                self.client,
                run_id=47,
                dataset_name="orders",
                output_path=self.output_dir,
            )

        register_object.assert_not_called()

    def test_missing_hot_bucket_fails_without_upload(self):
        self.client.bucket_exists.return_value = False

        with self.assertRaisesRegex(RuntimeError, "bucket does not exist"):
            upload_parquet_outputs(
                self.client,
                run_id=47,
                dataset_name="orders",
                output_path=self.output_dir,
            )

        self.client.fput_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()
