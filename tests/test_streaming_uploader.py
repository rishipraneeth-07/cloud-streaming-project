
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from minio.error import S3Error

from scripts.upload_streaming_to_minio import (
    load_committed_files,
    upload_committed_files,
)


class TestLoadCommittedFiles(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source_dir = self.root / "orders"
        self.metadata_dir = self.source_dir / "_spark_metadata"

        self.source_dir.mkdir()
        self.metadata_dir.mkdir()

        self.addCleanup(self.temp_dir.cleanup)

    def write_metadata(self, batch_id, records, version="v1"):
        metadata_path = self.metadata_dir / str(batch_id)

        with metadata_path.open("w", encoding="utf-8") as file:
            file.write(version + "\n")

            for record in records:
                file.write(json.dumps(record) + "\n")

    def make_record(self, file_path, action="add", size=100):
        return {
            "path": file_path.resolve().as_uri(),
            "size": size,
            "modificationTime": 1793197209480,
            "action": action,
        }

    def test_loads_committed_add_file(self):
        parquet_file = self.source_dir / "part-00000.parquet"
        parquet_file.write_bytes(b"x" * 100)

        self.write_metadata(
            0,
            [self.make_record(parquet_file)],
        )

        result = load_committed_files(
            self.metadata_dir,
            self.source_dir,
        )

        self.assertEqual(len(result), 1)
        self.assertIn(parquet_file.resolve(), result)
        self.assertEqual(result[parquet_file.resolve()]["size"], 100)

    def test_ignores_non_add_actions(self):
        parquet_file = self.source_dir / "part-00000.parquet"

        self.write_metadata(
            0,
            [self.make_record(parquet_file, action="delete")],
        )

        result = load_committed_files(
            self.metadata_dir,
            self.source_dir,
        )

        self.assertEqual(result, {})

    def test_rejects_path_outside_source_directory(self):
        outside_file = self.root / "outside.parquet"

        self.write_metadata(
            0,
            [self.make_record(outside_file)],
        )

        with self.assertRaises(ValueError):
            load_committed_files(
                self.metadata_dir,
                self.source_dir,
            )

    def test_rejects_unsupported_metadata_version(self):
        self.write_metadata(0, [], version="v2")

        with self.assertRaises(ValueError):
            load_committed_files(
                self.metadata_dir,
                self.source_dir,
            )

    def test_rejects_missing_metadata_directory(self):
        missing_dir = self.root / "missing"

        with self.assertRaises(FileNotFoundError):
            load_committed_files(
                missing_dir,
                self.source_dir,
            )

    def test_rejects_missing_path_or_size(self):
        self.write_metadata(
            0,
            [{"action": "add", "path": "file:///tmp/test.parquet"}],
        )

        with self.assertRaises(ValueError):
            load_committed_files(
                self.metadata_dir,
                self.source_dir,
            )

    def test_rejects_no_metadata_entries(self):
        with self.assertRaises(RuntimeError):
            load_committed_files(
                self.metadata_dir,
                self.source_dir,
            )

    def test_loads_multiple_committed_batches(self):
        first_file = self.source_dir / "part-00000-a.parquet"
        second_file = self.source_dir / "part-00000-b.parquet"

        self.write_metadata(
            0,
            [self.make_record(first_file, size=10)],
        )
        self.write_metadata(
            1,
            [self.make_record(second_file, size=20)],
        )

        result = load_committed_files(
            self.metadata_dir,
            self.source_dir,
        )

        self.assertEqual(len(result), 2)
        self.assertIn(first_file.resolve(), result)
        self.assertIn(second_file.resolve(), result)

    def test_later_entry_replaces_duplicate_file_record(self):
        parquet_file = self.source_dir / "part-00000.parquet"

        self.write_metadata(
            0,
            [self.make_record(parquet_file, size=10)],
        )
        self.write_metadata(
            1,
            [self.make_record(parquet_file, size=20)],
        )

        result = load_committed_files(
            self.metadata_dir,
            self.source_dir,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[parquet_file.resolve()]["size"], 20)


class TestUploadCommittedFiles(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "orders"
        self.source_dir.mkdir()
        self.addCleanup(self.temp_dir.cleanup)

        self.file_path = self.source_dir / "part-00000.parquet"
        self.file_path.write_bytes(b"x" * 100)

        self.record = {
            "size": 100,
            "modificationTime": 1793197209480,
            "action": "add",
        }

        self.client = Mock()

    def test_existing_object_is_not_overwritten(self):
        self.client.stat_object.return_value = Mock()

        with patch(
            "scripts.upload_streaming_to_minio.register_object"
        ) as register:
            summary = upload_committed_files(
                self.client,
                {self.file_path: self.record},
                self.source_dir,
            )

        self.client.fput_object.assert_not_called()
        register.assert_called_once()

        self.assertEqual(summary["uploaded"], 0)
        self.assertEqual(summary["already_present"], 1)

    def test_missing_local_file_is_skipped(self):
        missing_file = self.source_dir / "missing.parquet"

        with patch(
            "scripts.upload_streaming_to_minio.register_object"
        ) as register:
            summary = upload_committed_files(
                self.client,
                {missing_file: self.record},
                self.source_dir,
            )

        self.client.stat_object.assert_not_called()
        self.client.fput_object.assert_not_called()
        register.assert_not_called()
        self.assertEqual(summary["skipped"], 1)

    def test_size_mismatch_stops_before_upload(self):
        self.record["size"] = 999

        with self.assertRaises(RuntimeError):
            upload_committed_files(
                self.client,
                {self.file_path: self.record},
                self.source_dir,
            )

        self.client.stat_object.assert_not_called()
        self.client.fput_object.assert_not_called()

    
    def test_missing_object_is_uploaded_and_verified(self):
        missing_error = S3Error(
            None,
            "NoSuchKey",
            "Object not found",
            "resource",
            "request-id",
            "host-id",
        )

        self.client.stat_object.side_effect = [
            missing_error,
            Mock(),
        ]

        with patch(
            "scripts.upload_streaming_to_minio.register_object"
        ) as register:
            summary = upload_committed_files(
                self.client,
                {self.file_path: self.record},
                self.source_dir,
            )

        self.client.fput_object.assert_called_once()
        self.assertEqual(self.client.stat_object.call_count, 2)
        register.assert_called_once()

        self.assertEqual(summary["uploaded"], 1)
        self.assertEqual(summary["already_present"], 0)

        kwargs = register.call_args.kwargs
        self.assertEqual(
            kwargs["timestamp_source"],
            "spark_modification_time_unconfirmed",
        )


    def test_unexpected_minio_error_is_propagated(self):
        unexpected_error = S3Error(
            "AccessDenied",
            "Access denied",
            "resource",
            "request-id",
            "host-id",
            None,
        )
        self.client.stat_object.side_effect = unexpected_error

        with patch(
            "scripts.upload_streaming_to_minio.register_object"
        ) as register:
            with self.assertRaises(S3Error):
                upload_committed_files(
                    self.client,
                    {self.file_path: self.record},
                    self.source_dir,
                )

        self.client.fput_object.assert_not_called()
        register.assert_not_called()


if __name__ == "__main__":
    unittest.main()

