"""Safely convert historical MinIO CSV objects into verified Snappy Parquet."""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path

from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.tiering.metadata_repository import get_connection


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def object_key_for(source_key, source_sha256, target_prefix):
    safe_source = re.sub(r"[^A-Za-z0-9._/-]+", "_", source_key).strip("/")
    return f"{target_prefix.rstrip('/')}/{safe_source}.{source_sha256[:16]}.parquet"


def ensure_metadata_schema():
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historical_conversion_runs (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    source_bucket TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_sha256 TEXT NOT NULL,
                    target_bucket TEXT NOT NULL,
                    target_key TEXT NOT NULL,
                    target_sha256 TEXT,
                    row_count BIGINT,
                    schema_json JSONB,
                    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (source_bucket, source_key, source_sha256)
                )
            """)


def minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("https://", "").replace("http://", "")
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def sha256_object(client, bucket, key):
    response = client.get_object(bucket, key)
    try:
        digest = hashlib.sha256()
        for chunk in response.stream(1024 * 1024):
            digest.update(chunk)
        return digest.hexdigest()
    finally:
        response.close()
        response.release_conn()


def validate_csv_structure(source_path):
    with Path(source_path).open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        header = next(reader, None)
        if not header or any(not column.strip() for column in header):
            raise ValueError(f"Historical CSV has an empty or invalid header: {source_path}")
        expected_columns = len(header)
        try:
            for line_number, row in enumerate(reader, start=2):
                if len(row) != expected_columns:
                    raise ValueError(
                        f"Malformed CSV row at line {line_number}: expected "
                        f"{expected_columns} fields, got {len(row)}"
                    )
        except csv.Error as error:
            raise ValueError(f"Malformed CSV quoting at line {reader.line_num}: {error}") from error


def convert_csv_file(spark, source_path, output_dir):
    validate_csv_structure(source_path)
    dataframe = (
        spark.read.option("header", True).option("inferSchema", True)
        .option("mode", "FAILFAST")
        .option("unescapedQuoteHandling", "RAISE_ERROR")
        .csv(str(source_path))
    )
    source_schema = dataframe.schema.jsonValue()
    row_count = dataframe.count()
    if row_count <= 0 or not dataframe.columns:
        raise ValueError(f"Historical CSV is empty: {source_path}")
    dataframe.write.option("compression", "snappy").mode("errorifexists").parquet(str(output_dir))
    parquet_files = list(Path(output_dir).glob("part-*.parquet"))
    if len(parquet_files) != 1:
        raise RuntimeError(f"Expected one Parquet output, found {len(parquet_files)}")
    parquet_path = parquet_files[0]
    verified = spark.read.parquet(str(parquet_path))
    verify_conversion(row_count, source_schema, verified.count(), verified.schema.jsonValue())
    return {
        "path": parquet_path,
        "row_count": row_count,
        "schema": source_schema,
        "size_bytes": parquet_path.stat().st_size,
        "sha256": sha256_file(parquet_path),
    }


def verify_conversion(source_rows, source_schema, output_rows, output_schema):
    if source_rows != output_rows or source_schema != output_schema:
        raise RuntimeError("Converted Parquet row count or schema verification failed")


def upload_verified_output(client, bucket, key, parquet_path, expected_size, expected_sha256):
    client.fput_object(bucket, key, str(parquet_path), content_type="application/octet-stream")
    remote = client.stat_object(bucket, key)
    if remote.size != expected_size or sha256_object(client, bucket, key) != expected_sha256:
        raise RuntimeError(f"Converted object integrity verification failed: {bucket}/{key}")
    return remote


def process_object(client, spark, source_bucket, source, target_bucket, target_prefix, apply):
    source_key = source.object_name
    source_path = None
    output_dir = None
    with tempfile.TemporaryDirectory(prefix="historical-conversion-") as workdir:
        workdir = Path(workdir)
        source_path = workdir / "source.csv"
        client.fget_object(source_bucket, source_key, str(source_path))
        source_hash = sha256_file(source_path)
        target_key = object_key_for(source_key, source_hash, target_prefix)
        if not apply:
            print(f"DRY RUN: {source_bucket}/{source_key} -> {target_bucket}/{target_key}")
            return {"status": "DRY_RUN", "source": source_key, "target": target_key}

        try:
            existing = client.stat_object(target_bucket, target_key)
        except S3Error as error:
            if error.code not in ("NoSuchKey", "NoSuchObject", "NotFound"):
                raise
            existing = None
        if existing:
            recorded = sha256_object(client, target_bucket, target_key)
            with get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT target_sha256, status FROM historical_conversion_runs
                        WHERE source_bucket=%s AND source_key=%s AND source_sha256=%s
                    """, (source_bucket, source_key, source_hash))
                    metadata = cursor.fetchone()
            if recorded != (metadata[0] if metadata else None) or (metadata and metadata[1] != "SUCCESS"):
                print(f"Existing destination needs content verification before reuse: {target_bucket}/{target_key}")
            else:
                print(f"SKIPPED VERIFIED conversion: {source_bucket}/{source_key} -> {target_bucket}/{target_key}")
                return {"status": "SKIPPED_VERIFIED", "source": source_key, "target": target_key}

        output_dir = workdir / "parquet"
        conversion = convert_csv_file(spark, source_path, output_dir)
        parquet_path = conversion["path"]
        source_schema = conversion["schema"]
        row_count = conversion["row_count"]
        target_hash = conversion["sha256"]
        target_size = conversion["size_bytes"]
        if existing:
            remote_hash = sha256_object(client, target_bucket, target_key)
            if metadata and metadata[1] == "SUCCESS":
                raise RuntimeError(f"Existing conversion metadata disagrees with destination checksum: {target_bucket}/{target_key}")
            existing_file = workdir / "existing.parquet"
            client.fget_object(target_bucket, target_key, str(existing_file))
            existing_frame = spark.read.parquet(str(existing_file))
            verify_conversion(row_count, source_schema, existing_frame.count(), existing_frame.schema.jsonValue())
            target_hash = sha256_file(existing_file)
            target_size = existing_file.stat().st_size
            if existing.size != target_size or target_hash != remote_hash:
                raise RuntimeError(f"Existing destination failed size or checksum verification: {target_bucket}/{target_key}")
            print(f"REUSED verified destination after interrupted metadata write: {target_bucket}/{target_key}")
        else:
            upload_verified_output(client, target_bucket, target_key, parquet_path, target_size, target_hash)
        with get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO historical_conversion_runs
                        (source_bucket, source_key, source_sha256, target_bucket, target_key,
                         target_sha256, row_count, schema_json, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,'SUCCESS')
                    ON CONFLICT (source_bucket, source_key, source_sha256) DO UPDATE SET
                        target_bucket=EXCLUDED.target_bucket, target_key=EXCLUDED.target_key,
                        target_sha256=EXCLUDED.target_sha256, row_count=EXCLUDED.row_count,
                        schema_json=EXCLUDED.schema_json, status='SUCCESS'
                """, (source_bucket, source_key, source_hash, target_bucket, target_key,
                      target_hash, row_count, json.dumps(source_schema)))
        print(f"CONVERTED {source_bucket}/{source_key}: rows={row_count}, bytes={target_size}, sha256={target_hash}")
        return {"status": "SUCCESS", "source": source_key, "target": target_key, "row_count": row_count}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bucket", default=os.getenv("HISTORICAL_SOURCE_BUCKET", "hot-data"))
    parser.add_argument("--source-prefix", default=os.getenv("HISTORICAL_SOURCE_PREFIX", "legacy/"))
    parser.add_argument("--target-bucket", default=os.getenv("HISTORICAL_TARGET_BUCKET", "warm-data"))
    parser.add_argument("--target-prefix", default=os.getenv("HISTORICAL_TARGET_PREFIX", "converted/historical"))
    parser.add_argument("--apply", action="store_true", help="Convert and upload; default is dry-run")
    args = parser.parse_args()
    client = minio_client()
    eligible = [obj for obj in client.list_objects(args.source_bucket, prefix=args.source_prefix, recursive=True)
                if obj.object_name.lower().endswith(".csv")]
    print(f"Historical CSV objects discovered: {len(eligible)}")
    if not args.apply:
        for obj in eligible:
            process_object(client, None, args.source_bucket, obj, args.target_bucket, args.target_prefix, False)
        return
    ensure_metadata_schema()
    spark = SparkSession.builder.appName("HistoricalMinioCsvConversion").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        for obj in eligible:
            process_object(client, spark, args.source_bucket, obj, args.target_bucket, args.target_prefix, True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
