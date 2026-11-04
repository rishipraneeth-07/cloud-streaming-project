
from contextlib import contextmanager

from src.tiering.metadata_repository import get_connection


def start_pipeline_run(pipeline_name, run_type):
    if not isinstance(pipeline_name, str) or not pipeline_name.strip():
        raise ValueError("Pipeline name must be a non-empty string")

    if run_type not in ("BATCH", "STREAMING", "TIERING"):
        raise ValueError("Invalid pipeline run type")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline_name,
                    run_type,
                    status,
                    records_processed,
                    records_failed
                )
                VALUES (%s, %s, 'RUNNING', 0, 0)
                RETURNING id
                """,
                (pipeline_name, run_type),
            )

            return cursor.fetchone()[0]


def finish_pipeline_run(
    run_id,
    status,
    records_processed=0,
    records_failed=0,
    error_message=None,
):
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError("Invalid pipeline status")

    if records_processed < 0 or records_failed < 0:
        raise ValueError("Record counts cannot be negative")

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_runs
                SET
                    status = %s,
                    records_processed = %s,
                    records_failed = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE id = %s
                """,
                (
                    status,
                    records_processed,
                    records_failed,
                    error_message,
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise KeyError(f"Pipeline run not found: {run_id}")


@contextmanager
def track_pipeline_run(pipeline_name, run_type):
    run_id = start_pipeline_run(pipeline_name, run_type)

    run = {
        "id": run_id,
        "records_processed": 0,
        "records_failed": 0,
    }

    try:
        yield run

    except KeyboardInterrupt:
        try:
            finish_pipeline_run(
                run_id,
                "SUCCESS",
                run["records_processed"],
                run["records_failed"],
            )
        except Exception as tracking_error:
            print(
                "WARNING: Could not record interrupted pipeline shutdown: "
                f"{tracking_error}"
            )

        raise

    except Exception as error:
        try:
            finish_pipeline_run(
                run_id,
                "FAILED",
                run["records_processed"],
                run["records_failed"],
                str(error),
            )
        except Exception as tracking_error:
            print(
                "WARNING: Could not record pipeline failure: "
                f"{tracking_error}"
            )

        raise

    else:
        finish_pipeline_run(
            run_id,
            "SUCCESS",
            run["records_processed"],
            run["records_failed"],
        )

