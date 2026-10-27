
import unittest
from unittest.mock import Mock

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

    def test_matching_checksum_allows_source_deletion(self):
        migrate_object(
            self.client,
            "hot-data",
            "example.parquet",
            "warm-data",
        )

        self.client.copy_object.assert_called_once()
        self.assertEqual(self.client.get_object.call_count, 2)
        self.client.remove_object.assert_called_once_with(
            "hot-data",
            "example.parquet",
        )

    def test_size_mismatch_preserves_source(self):
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
        self.client.remove_object.assert_called_once_with(
            "warm-data",
            "example.parquet",
        )

    def test_checksum_mismatch_preserves_source(self):
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

        self.client.remove_object.assert_called_once_with(
            "warm-data",
            "example.parquet",
        )

    def test_existing_destination_is_not_overwritten(self):
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

    def test_same_source_and_destination_bucket_is_rejected(self):
        with self.assertRaises(ValueError):
            migrate_object(
                self.client,
                "hot-data",
                "example.parquet",
                "hot-data",
            )

        self.client.stat_object.assert_not_called()


if __name__ == "__main__":
    unittest.main()


