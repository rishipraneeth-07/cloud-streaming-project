# Cloud Pipeline Monitoring + Cost Optimization Add-on

This is an additive monitoring package for the existing `cloud-streaming-project`.
It does not replace your Spark, Kafka, Airflow, MinIO, or PostgreSQL pipeline.

## What it provides
- PostgreSQL inventory table for each MinIO object (bucket, key, bytes, last modified, ETag, tier, inferred batch/streaming path).
- Periodic MinIO inventory synchronization.
- Prometheus metrics for MinIO tiers, PostgreSQL migration history and pipeline runs, Kafka topic count, and Airflow component health.
- Grafana dashboard with tier totals, cost estimates, migration status, pipeline totals, and a per-file inventory table.
- Cost estimates using editable placeholder rates in `config/storage_costs.json`.
- A Parquet compression benchmark that defaults to dry-run and compares Snappy with ZSTD.

## Important
- Cost rates are illustrative placeholders, not your cloud provider's bill. Replace them with the rates for your chosen provider/region.
- MinIO is local object storage. The estimate is a model, not an invoice.
- File source (`batch`/`streaming`) is inferred from object path. It is not proof of Spark lineage.
- Kafka topic count is not throughput or consumer lag.
- Airflow health checks component health, not individual DAG task states.
- Compression benchmark does not overwrite objects unless `--apply` is explicitly supplied. Do not use `--apply` until you have backups and have reviewed the report.
- Inventory sync is additive/upsert-only. It never deletes MinIO objects or PostgreSQL metadata.
- Keep `.env` out of Git.

## Install
Copy the `monitoring/`, `sql/`, and `config/storage_costs.json` folders into the project root. Merge the service from `docker-compose.monitoring.yml` into your existing Compose network setup, or run it with the provided file.

```bash
# From ~/cloud-streaming-project after copying the files:
python -m pip install -r monitoring/exporter/requirements.txt
psql "$DATABASE_URL" -f sql/001_object_inventory.sql
docker compose --env-file .env -f docker-compose.monitoring.yml up -d --build
```

If you don't use `DATABASE_URL`, apply the SQL using your existing Postgres container:
```bash
set -a; source .env; set +a
docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" project-postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < sql/001_object_inventory.sql
```

Grafana: http://localhost:3000
Prometheus: http://localhost:9090
Exporter: http://localhost:9105/metrics

Default Grafana login remains the one configured in your current Grafana Compose file.

## Validate
```bash
curl -fsS http://localhost:9105/health
curl -fsS http://localhost:9105/metrics | grep '^project_'
curl -fsS http://localhost:9090/-/ready
```

## Compression benchmark
Install the optional dependency:
```bash
.venv/bin/pip install pyarrow
```
Then benchmark one Parquet object:
```bash
.venv/bin/python monitoring/scripts/benchmark_parquet_compression.py \
  --bucket hot-data --key 'batch/orders/YOUR_FILE.parquet'
```
This only downloads to a temporary directory and reports compressed sizes. It does not change MinIO.
