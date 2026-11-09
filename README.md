# Cloud-Native Batch and Streaming Data Pipeline with Cost-Optimized Storage Tiering

A local, containerized data platform demonstrating batch processing, event streaming, object storage, lifecycle tiering, metadata tracking, orchestration, and observability.

## Architecture

```text
                         BATCH
data/sample/orders.csv -> PySpark -> Parquet ------------------+
                                                               |
                         STREAMING                            v
Python producer -> Kafka topic `orders` -> Spark Structured Streaming
                                                               |
                                                               v
                                                     MinIO `hot-data`
                                                               |
                                           +-------------------+------------------+
                                           |                                      |
                                           v                                      v
                                      PostgreSQL                         Prometheus exporter
                                  metadata / run history                         |
                                           |                                     v
                                           v                                  Grafana
                                      Airflow DAG                         dashboards / cost

MinIO tiering policy: hot-data -> warm-data -> cold-data
Migration safety: copy -> size check -> SHA-256 check -> metadata update -> source deletion
```

## Components

- **Apache Kafka:** receives order events.
- **Apache Spark / PySpark:** batch transformations and Structured Streaming.
- **MinIO:** S3-compatible object storage with hot, warm, and cold buckets.
- **PostgreSQL:** object metadata, migration history, inventory, optimization history, and pipeline runs.
- **Apache Airflow:** scheduled metadata initialization and tiering preview.
- **Prometheus:** scrapes service and pipeline metrics.
- **Grafana:** operational dashboard and estimated storage-cost views.

Storage prices in `config/storage_costs.json` are illustrative estimates, not actual cloud-provider charges.

## 1. Prerequisites

- Ubuntu 24.04 (or compatible Linux)
- Docker Engine and Docker Compose plugin
- Java 21
- Python 3.12
- At least 8 GB RAM recommended for local Spark + Docker services

## 2. Environment setup

From the repository root:

```bash
cp .env.example .env
nano .env
```

Set unique local passwords. The MinIO credentials in `.env` must match the MinIO server credentials. Do not commit `.env`.

Create and activate the Python environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Check Java and Docker:

```bash
java -version
docker --version
docker compose version
```

## 3. Start infrastructure

Start Kafka and PostgreSQL from the repository root:

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.postgres.yml up -d
```

If MinIO is not already running, start it using the credentials from `.env`:

```bash
set -a
source .env
set +a

docker run -d --name minio --restart unless-stopped \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
  -e MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
  -v minio_data:/data \
  minio/minio:latest server /data --console-address ":9001"
```

If a MinIO container named `minio` already exists, do not run the `docker run` command again.

Connect MinIO to the Compose network so Airflow and the monitoring exporter can reach it by the hostname `minio`:

```bash
docker network connect cloud-streaming-project_default minio
```

If Docker reports that MinIO is already connected to this network, continue.

Create the three storage buckets:

```bash
source .venv/bin/activate
python scripts/setup_tier_buckets.py
```

Initialize the PostgreSQL schema:

```bash
docker exec -i project-postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < sql/schema.sql

docker exec -i project-postgres sh -c \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < sql/001_object_inventory.sql
```

The container receives `POSTGRES_*` variables from Compose. If your shell command does not see them, load `.env` first:

```bash
set -a; source .env; set +a
```

## 4. Batch pipeline

Run CSV -> Spark -> Parquet -> MinIO -> PostgreSQL metadata:

```bash
source .venv/bin/activate
python -m src.batch.process_orders
```

The job validates rows, creates `line_total`, writes order and product-summary Parquet datasets, uploads them under unique run-specific keys in `hot-data`, verifies object sizes, and registers trusted pipeline-creation timestamps.

Local Parquet output:
- `data/processed/orders/`
- `data/processed/product_summary/`

## 5. Streaming pipeline

Create the Kafka topic (if it does not already exist):

```bash
docker exec project-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic orders --partitions 1 --replication-factor 1
```

Terminal 1 — start Spark Structured Streaming:

```bash
source .venv/bin/activate
python -m src.streaming.spark_streaming
```

Terminal 2 — send sample CSV orders to Kafka:

```bash
source .venv/bin/activate
python -m src.streaming.producer
```

The producer sends one event per second. Spark consumes the `orders` topic, validates events, writes Parquet micro-batches, and uploads committed output files to `hot-data`. Stop the streaming process with `Ctrl+C`.

## 6. Storage tiering

The policy is in `config/tiering_rules.json`. The default is:

```json
{
  "tiers": {
    "hot": {"bucket": "hot-data", "max_age_days": 7},
    "warm": {"bucket": "warm-data", "max_age_days": 30},
    "cold": {"bucket": "cold-data"}
  },
  "dry_run": true
}
```

Preview metadata initialization (read-only):

```bash
python scripts/initialize_object_metadata.py
```

Run the tiering preview:

```bash
python scripts/run_tiering_with_tracking.py
```

The tiering engine only considers records whose original creation timestamp is confirmed. It verifies size and SHA-256 before deleting a source object. Keep `dry_run` set to `true` for the demo unless you intentionally approve a real migration after reviewing the preview.

The legacy helper is also safe by default:

```bash
python scripts/migrate_hot_to_warm.py
```

It only previews. Real execution requires an explicit `--apply` flag; review the source and destination objects before using it.

## 7. Airflow

Build and start Airflow:

```bash
docker compose -f docker-compose.airflow.yml up -d --build
```

Open http://localhost:8080. The DAG is `cloud_storage_tiering`. It initializes missing metadata in preview mode and runs the tiering preview. It does not automatically move objects while `dry_run` is true.

## 8. Monitoring

Start the exporter and Prometheus:

```bash
docker compose -f docker-compose.monitoring.yml up -d --build
```

Start Grafana:

```bash
docker compose -f docker-compose.grafana.yml up -d
```

Open:
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Exporter health: http://localhost:9105/health
- Exporter metrics: http://localhost:9105/metrics
- MinIO console: http://localhost:9001

Grafana dashboard: **Cloud-Native Data Pipeline — Operations & Cost Optimization**.

## 9. Tests and validation

Run unit tests:

```bash
source .venv/bin/activate
python -m unittest discover -s tests -v
```

Run the policy test independently:

```bash
python -m unittest tests.test_tiering_policy -v
```

Run syntax and JSON validation:

```bash
python - <<'PY'
import ast
import json
from pathlib import Path

root = Path(".")
for path in root.rglob("*.py"):
    if ".venv" not in path.parts:
        ast.parse(path.read_text(encoding="utf-8"))
for path in root.rglob("*.json"):
    json.loads(path.read_text(encoding="utf-8"))
print("Python syntax and JSON configuration checks passed.")
PY
```

Check service health:

```bash
docker compose -f docker-compose.kafka.yml ps
docker compose -f docker-compose.postgres.yml ps
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:9105/health
curl -fsS http://localhost:3000/api/health
```

Useful database checks:

```bash
set -a; source .env; set +a
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" project-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT pipeline_name, run_type, status, records_processed, records_failed FROM pipeline_runs ORDER BY id DESC LIMIT 20;"

docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" project-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT bucket, COUNT(*) AS objects, SUM(size_bytes) AS bytes FROM object_inventory GROUP BY bucket ORDER BY bucket;"
```

## 10. Stop services

```bash
docker compose -f docker-compose.grafana.yml down
docker compose -f docker-compose.monitoring.yml down
docker compose -f docker-compose.airflow.yml down
docker compose -f docker-compose.postgres.yml down
docker compose -f docker-compose.kafka.yml down
```

These commands stop containers but retain named volumes. Do not use `down -v` unless you intentionally want to delete persisted database, Kafka, Grafana, or Prometheus data.

## Safety and limitations

- Never commit `.env` or credentials.
- Tier migration is preview-only by default.
- Do not enable real tiering until you have reviewed the eligible object list and verified backups.
- Inventory synchronization is an observation/upsert process; it should not delete MinIO objects.
- Storage-cost metrics are estimates based on configurable example rates.
- Kafka topic count is not consumer lag or throughput.
- A local demo proves the pipeline flow, not production-scale resilience or cloud-provider billing accuracy.
