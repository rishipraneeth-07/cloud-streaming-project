from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

with DAG(
    dag_id="cloud_storage_tiering",
    description="Initialize object metadata and preview storage tiering decisions",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    tags=["minio", "storage-tiering"],
) as dag:

    initialize_metadata = BashOperator(
        task_id="initialize_object_metadata",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python scripts/initialize_object_metadata.py"
        ),
    )

    preview_tiering = BashOperator(
        task_id="preview_tiering",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.tiering.engine"
        ),
    )

    initialize_metadata >> preview_tiering
