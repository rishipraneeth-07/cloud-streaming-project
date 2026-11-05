import os
from uuid import uuid4

from src.tiering.metadata_repository import get_connection
import unittest
from unittest.mock import patch

from src.tiering.metadata_repository import (
    create_migration_record,
    update_migration_status,
)


class TestMigrationRepository(unittest.TestCase):

    @patch("src.tiering.metadata_repository.get_connection")
    def test_create_migration_record(self, mock_connection):
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = (1,)

        migration_id = create_migration_record(
            "hot-data",
            "orders/file.parquet",
            "warm-data",
            "orders/file.parquet",
        )

        self.assertEqual(migration_id, 1)
        mock_cursor.execute.assert_called_once()

    @patch("src.tiering.metadata_repository.get_connection")
    def test_update_migration_status_completed(self, mock_connection):
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        mock_cursor.rowcount = 1

        update_migration_status(1, "COMPLETED")

        self.assertEqual(mock_cursor.rowcount, 1)
        mock_cursor.execute.assert_called_once()

    @patch("src.tiering.metadata_repository.get_connection")
    def test_update_migration_status_failed(self, mock_connection):
        mock_cursor = mock_connection.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        mock_cursor.rowcount = 1

        update_migration_status(
            1,
            "FAILED",
            "Checksum verification failed",
        )

        mock_cursor.execute.assert_called_once()


class TestMigrationRepositoryIntegration(unittest.TestCase):

    def test_migration_lifecycle_in_postgres(self):
        suffix = uuid4().hex

        source_key = f"test-source/{suffix}.parquet"
        destination_key = f"test-destination/{suffix}.parquet"

        migration_id = None

        try:
            migration_id = create_migration_record(
                "hot-data",
                source_key,
                "warm-data",
                destination_key,
            )

            with get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT status, started_at, completed_at
                        FROM migration_history
                        WHERE id = %s
                        """,
                        (migration_id,),
                    )
                    row = cursor.fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row[0], "PENDING")
            self.assertIsNotNone(row[1])
            self.assertIsNone(row[2])

            update_migration_status(migration_id, "COMPLETED")

            with get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT status, completed_at, error_message
                        FROM migration_history
                        WHERE id = %s
                        """,
                        (migration_id,),
                    )
                    row = cursor.fetchone()

            self.assertEqual(row[0], "COMPLETED")
            self.assertIsNotNone(row[1])
            self.assertIsNone(row[2])

        finally:
            if migration_id is not None:
                with get_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM migration_history WHERE id = %s",
                            (migration_id,),
                        )
   
    def test_failed_migration_in_postgres(self):
        suffix = uuid4().hex

        migration_id = None
        error_message = "Checksum verification failed"

        try:
            migration_id = create_migration_record(
                "hot-data",
                f"test-source/{suffix}.parquet",
                "warm-data",
                f"test-destination/{suffix}.parquet",
            )

            update_migration_status(
                migration_id,
                "FAILED",
                error_message,
            )

            with get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT status, completed_at, error_message
                        FROM migration_history
                        WHERE id = %s
                        """,
                        (migration_id,),
                    )
                    row = cursor.fetchone()

            self.assertEqual(row[0], "FAILED")
            self.assertIsNotNone(row[1])
            self.assertEqual(row[2], error_message)

        finally:
            if migration_id is not None:
                with get_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "DELETE FROM migration_history WHERE id = %s",
                            (migration_id,),
                        )



if __name__ == "__main__":
    unittest.main()

