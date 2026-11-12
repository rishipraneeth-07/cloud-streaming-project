import json
import logging
import os
import time
from pathlib import Path

import psycopg2
import requests
from minio import Minio
from kafka import KafkaAdminClient, KafkaConsumer
from prometheus_client import start_http_server, Gauge

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("project-exporter")
INTERVAL = int(os.getenv("METRICS_INTERVAL_SECONDS", "30"))
COST_FILE = Path(os.getenv("STORAGE_COSTS_FILE", "/app/storage_costs.json"))

minio_up = Gauge("project_minio_up", "MinIO API reachable")
minio_objects = Gauge("project_minio_bucket_objects", "Object count by MinIO bucket", ["bucket"])
minio_bytes = Gauge("project_minio_bucket_bytes", "Stored bytes by MinIO bucket", ["bucket"])
minio_monthly_cost = Gauge("project_minio_estimated_monthly_cost_usd", "Estimated monthly storage cost by bucket", ["bucket"])
postgres_up = Gauge("project_postgres_up", "PostgreSQL reachable")
migration_count = Gauge("project_migrations_total", "Migration records by status", ["status"])
pipeline_runs = Gauge("project_pipeline_runs_total", "Pipeline runs by pipeline and status", ["pipeline", "status"])
pipeline_records = Gauge("project_pipeline_records_total", "Cumulative records by pipeline and kind", ["pipeline", "kind"])

pipeline_active_runs = Gauge(
    "project_pipeline_active_runs",
    "Currently running pipeline executions",
    ["pipeline"],
)
pipeline_duration = Gauge(
    "project_pipeline_duration_seconds",
    "Average pipeline execution duration in seconds",
    ["pipeline"],
)
pipeline_throughput = Gauge(
    "project_pipeline_records_per_second",
    "Average pipeline processing throughput",
    ["pipeline"],
)

kafka_up = Gauge("project_kafka_up", "Kafka admin API reachable")
kafka_topic_count = Gauge("project_kafka_topics", "Kafka topic count")
kafka_consumer_lag = Gauge(
    "project_kafka_consumer_lag_records",
    "Kafka records behind the broker end offset for the committed consumer group",
    ["group", "topic", "partition"],
)
kafka_consumer_lag_available = Gauge(
    "project_kafka_consumer_lag_available",
    "Whether committed Kafka consumer lag is currently available for the configured group",
    ["group"],
)
airflow_component_up = Gauge("project_airflow_component_up", "Airflow health component", ["component"])
inventory_last_sync = Gauge("project_inventory_last_sync_unixtime", "Unix time of last successful inventory sync")

def db_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "project-postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )

def load_costs():
    try:
        data = json.loads(COST_FILE.read_text())
        return data.get("rates", {})
    except Exception:
        log.exception("Cannot read storage cost configuration")
        return {}

def infer_source(key):
    first = key.split("/", 1)[0].lower()
    if first in ("batch", "streaming"):
        return first
    return "unknown"

def collect_minio_and_sync_inventory():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    rates = load_costs()
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            for bucket in ("hot-data", "warm-data", "cold-data"):
                count, total_bytes = 0, 0
                for obj in client.list_objects(bucket, recursive=True):
                    count += 1
                    size = int(obj.size or 0)
                    total_bytes += size
                    cost = (size / (1024 ** 3)) * float(rates.get(bucket, 0))
                    cur.execute("""
                        INSERT INTO object_inventory
                            (bucket, object_name, size_bytes, etag, last_modified,
                             inferred_source, current_tier, estimated_monthly_cost_usd,
                             last_seen_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (bucket, object_name) DO UPDATE SET
                            size_bytes = EXCLUDED.size_bytes,
                            etag = EXCLUDED.etag,
                            last_modified = EXCLUDED.last_modified,
                            inferred_source = EXCLUDED.inferred_source,
                            current_tier = EXCLUDED.current_tier,
                            estimated_monthly_cost_usd = EXCLUDED.estimated_monthly_cost_usd,
                            last_seen_at = NOW()
                    """, (
                        bucket, obj.object_name, size, obj.etag,
                        obj.last_modified, infer_source(obj.object_name),
                        bucket, cost
                    ))
                minio_objects.labels(bucket).set(count)
                minio_bytes.labels(bucket).set(total_bytes)
                minio_monthly_cost.labels(bucket).set(
                    (total_bytes / (1024 ** 3)) * float(rates.get(bucket, 0))
                )
        conn.commit()
        minio_up.set(1)
        inventory_last_sync.set(time.time())
    except Exception:
        conn.rollback()
        minio_up.set(0)
        log.exception("MinIO inventory collection failed")
    finally:
        conn.close()

def collect_postgres():
    conn = None
    try:
        conn = db_connect()
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM migration_history GROUP BY status")
            for status, count in cur.fetchall():
                migration_count.labels(str(status)).set(count)

            cur.execute("""
                SELECT pipeline_name, status, COUNT(*),
                       COALESCE(SUM(records_processed),0),
                       COALESCE(SUM(records_failed),0)
                FROM pipeline_runs GROUP BY pipeline_name, status
            """)
            for name, status, count, processed, failed in cur.fetchall():
                pipeline_runs.labels(str(name), str(status)).set(count)
                pipeline_records.labels(str(name), "processed").set(processed)
                pipeline_records.labels(str(name), "failed").set(failed)

            cur.execute("""
                SELECT
                    pipeline_name,
                    COUNT(*) FILTER (WHERE status = 'RUNNING'),
                    COALESCE(SUM(records_processed), 0),
                    COALESCE(SUM(EXTRACT(EPOCH FROM (
                        COALESCE(completed_at, NOW()) - started_at
                    ))), 0)
                FROM pipeline_runs
                GROUP BY pipeline_name
            """)

            for name, active, processed, duration in cur.fetchall():
                active = int(active or 0)
                duration = float(duration or 0)
                processed = int(processed or 0)

                pipeline_active_runs.labels(str(name)).set(active)
                pipeline_duration.labels(str(name)).set(
                    duration / max(1, active) if active else 0
                )
                pipeline_throughput.labels(str(name)).set(
                    processed / duration if duration > 0 else 0
                )

        postgres_up.set(1)
    except Exception:
        postgres_up.set(0)
        log.exception("PostgreSQL metrics collection failed")
    finally:
        if conn:
            conn.close()

def collect_kafka():
    admin = None
    try:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "project-kafka:29092")
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        topics = admin.list_topics()
        kafka_topic_count.set(len(topics))
        kafka_up.set(1)
        group = os.getenv("KAFKA_CONSUMER_GROUP", "orders-structured-streaming")
        try:
            committed = admin.list_consumer_group_offsets(group)
            topic_partitions = list(committed)
            if not topic_partitions:
                kafka_consumer_lag_available.labels(group).set(0)
                return
            consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
            try:
                consumer.assign(topic_partitions)
                latest = consumer.end_offsets(topic_partitions)
            finally:
                consumer.close()
            for partition, committed_offset in committed.items():
                if partition.topic in topics and committed_offset is not None:
                    lag = max(0, int(latest.get(partition, 0)) - int(committed_offset.offset))
                    kafka_consumer_lag.labels(group, partition.topic, str(partition.partition)).set(lag)
            kafka_consumer_lag_available.labels(group).set(1)
        except Exception:
            kafka_consumer_lag_available.labels(group).set(0)
            log.exception("Kafka consumer lag is unavailable for group %s", group)
    except Exception:
        kafka_up.set(0)
        log.exception("Kafka metrics collection failed")
    finally:
        if admin:
            admin.close()

def collect_airflow():
    try:
        response = requests.get(
            os.getenv("AIRFLOW_HEALTH_URL", "http://project-airflow:8080/health"),
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        for name in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
            status = data.get(name, {}).get("status")
            heartbeat = data.get(name, {}).get("latest_scheduler_heartbeat") or data.get(name, {}).get("latest_dag_processor_heartbeat")
            if name == "dag_processor" and status is None:
                # Airflow 2.10 runs DAG parsing within the scheduler process.
                heartbeat = data.get("scheduler", {}).get("latest_scheduler_heartbeat")
            healthy = status == "healthy" or (name in ("scheduler", "dag_processor") and heartbeat is not None)
            airflow_component_up.labels(name).set(1 if healthy else 0)
    except Exception:
        log.exception("Airflow health collection failed")
        for name in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
            airflow_component_up.labels(name).set(0)

if __name__ == "__main__":
    start_http_server(9105)
    log.info("Exporter started on :9105")
    while True:
        collect_postgres()
        collect_minio_and_sync_inventory()
        collect_kafka()
        collect_airflow()
        time.sleep(INTERVAL)
