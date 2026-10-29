
import os
import unittest
from pathlib import Path

import psycopg


ROOT_DIR = Path(__file__).resolve().parents[1]


def load_env_file():
    for line in (ROOT_DIR / ".env").read_text().splitlines():
        line = line.strip()

        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(
                key.strip(),
                value.strip().strip("'\""),
            )


class TestPostgresSchema(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        load_env_file()

        cls.connection = psycopg.connect(
            host="localhost",
            port=5432,
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            autocommit=False,
        )

    @classmethod
    def tearDownClass(cls):
        cls.connection.rollback()
        cls.connection.close()

    def test_expected_tables_exist(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                """
            )
            tables = {row[0] for row in cursor.fetchall()}

        self.assertTrue(
            {
                "object_metadata",
                "migration_history",
                "pipeline_runs",
            }.issubset(tables)
        )

    
    def test_object_metadata_primary_key(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                  AND table_name = 'object_metadata'
                  AND constraint_type = 'PRIMARY KEY'
                """
            )
            result = cursor.fetchone()

        self.assertIsNotNone(result)


    def test_migration_history_rejects_invalid_status(self):
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO migration_history (
                        source_bucket,
                        source_key,
                        destination_bucket,
                        destination_key,
                        status
                    )
                    VALUES (
                        'hot-data',
                        'test-source',
                        'warm-data',
                        'test-destination',
                        'INVALID'
                    )
                    """
                )

        self.connection.rollback()

    def test_pipeline_runs_rejects_invalid_run_type(self):
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pipeline_runs (
                        pipeline_name,
                        run_type,
                        status
                    )
                    VALUES ('test-pipeline', 'INVALID', 'RUNNING')
                    """
                )

        self.connection.rollback()

    def test_pipeline_runs_rejects_negative_record_count(self):
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pipeline_runs (
                        pipeline_name,
                        run_type,
                        status,
                        records_processed
                    )
                    VALUES (
                        'test-pipeline',
                        'BATCH',
                        'SUCCESS',
                        -1
                    )
                    """
                )

        self.connection.rollback()


if __name__ == "__main__":
    unittest.main()

