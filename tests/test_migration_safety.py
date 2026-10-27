
import unittest
from unittest.mock import Mock, patch

from minio.error import S3Error

from src.tiering.engine import migrate_object


class FakeResponse:
    def __init__(self, content):
        self.content = content

    def stream(self, chunk_size):
        yield self.content

    def close(self):
        pass

    def release_conn(self):
        pass


class TestMigrationSafety(unittest.TestCase):

    def setUp(self):
        self.client = Mock()

        self.client.stat_object.side_effect = [
            Mock(size=100),
            self.missing_object_error(),
            Mock(size=100),
        ]

        self.client.get_object.side_effect = [
            FakeResponse(b"matching file content"),
            FakeResponse(b"matching file content"),
        ]

    @staticmethod
    def missing_object_error():
        return S3Error(
            Mock(),
            "NoSuchKey",
            "Object does not exist",
            "/warm-data/example.parquet",
            "request-id",
            "host-id",
            "warm-data",
            "example.parquet",
        )

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_matching_checksum_allows_source_deletion(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        migrate_object(
            self.client,
            "hot-data",
            "example.parquet",
            "warm-data",
        )

        self.client.copy_object.assert_called_once()
        self.assertEqual(self.client.get_object.call_count, 2)

        mock_preserve_metadata.assert_called_once_with(
            "hot-data/example.parquet",
            "warm-data/example.parquet",
        )

        self.client.remove_object.assert_called_once_with(
            "hot-data",
            "example.parquet",
        )

        mock_complete_metadata.assert_called_once_with(
            "hot-data/example.parquet",
            "warm-data/example.parquet",
        )

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_size_mismatch_preserves_source(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        self.client.stat_object.side_effect = [
            Mock(size=100),
            self.missing_object_error(),
            Mock(size=90),
        ]

        with self.assertRaises(RuntimeError):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "warm-data",
            )

        self.client.get_object.assert_not_called()

        mock_preserve_metadata.assert_not_called()
        mock_complete_metadata.assert_not_called()

        self.client.remove_object.assert_called_once_with(
            "warm-data",
            "example.parquet",
        )

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_checksum_mismatch_preserves_source(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        self.client.get_object.side_effect = [
            FakeResponse(b"original content"),
            FakeResponse(b"corrupted content"),
        ]

        with self.assertRaises(RuntimeError):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "warm-data",
            )

        mock_preserve_metadata.assert_not_called()
        mock_complete_metadata.assert_not_called()

        self.client.remove_object.assert_called_once_with(
            "warm-data",
            "example.parquet",
        )

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_existing_destination_is_not_overwritten(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        self.client.stat_object.side_effect = [
            Mock(size=100),
            Mock(size=50),
        ]

        with self.assertRaises(FileExistsError):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "warm-data",
            )

        self.client.copy_object.assert_not_called()
        self.client.remove_object.assert_not_called()
        mock_preserve_metadata.assert_not_called()
        mock_complete_metadata.assert_not_called()

    def test_same_source_and_destination_bucket_is_rejected(self):
        with self.assertRaises(ValueError):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "hot-data",
            )

        self.client.stat_object.assert_not_called()
        self.client.copy_object.assert_not_called()
        self.client.remove_object.assert_not_called()

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_source_deletion_failure_keeps_destination(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        self.client.remove_object.side_effect = RuntimeError(
            "Source deletion failed"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Source deletion failed",
        ):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "warm-data",
            )

        self.client.copy_object.assert_called_once()

        mock_preserve_metadata.assert_called_once_with(
            "hot-data/example.parquet",
            "warm-data/example.parquet",
        )

        # Source deletion was attempted, but destination cleanup
        # must not run because the source may still exist.
        self.client.remove_object.assert_called_once_with(
            "hot-data",
            "example.parquet",
        )

        mock_complete_metadata.assert_not_called()

    @patch("src.tiering.engine.preserve_object_metadata")
    @patch("src.tiering.engine.complete_metadata_migration")
    def test_metadata_preservation_failure_keeps_both_objects(
        self,
        mock_complete_metadata,
        mock_preserve_metadata,
    ):
        mock_preserve_metadata.side_effect = RuntimeError(
            "Metadata persistence failed"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Metadata persistence failed",
        ):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "warm-data",
            )

        # Once metadata preservation starts, the engine conservatively
        # retains the destination rather than risking data loss.
        self.client.remove_object.assert_not_called()
        mock_complete_metadata.assert_not_called()


if __name__ == "__main__":
    unittest.main()

