import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq
from minio import Minio

def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def main():
    parser = argparse.ArgumentParser(
        description="Read-only Parquet compression benchmark (Snappy source vs ZSTD candidate)"
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    with tempfile.TemporaryDirectory() as tmp:
        source = Path(tmp) / "source.parquet"
        candidate = Path(tmp) / "candidate-zstd.parquet"
        client.fget_object(args.bucket, args.key, str(source))

        table = pq.read_table(source)
        pq.write_table(table, candidate, compression="zstd")

        before = source.stat().st_size
        after = candidate.stat().st_size
        saved = before - after
        details = {
            "source_sha256": sha256(source),
            "candidate_sha256": sha256(candidate),
            "source_codec": "existing/unknown",
            "target_codec": "zstd",
            "dry_run": True,
        }

        print(f"Object: {args.bucket}/{args.key}")
        print(f"Current bytes: {before}")
        print(f"ZSTD candidate bytes: {after}")
        print(f"Estimated bytes saved: {saved}")
        print(f"Estimated size reduction: {(saved / before * 100):.2f}%" if before else "N/A")
        print("DRY RUN: MinIO object was not modified.")

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO object_optimization_history
                        (bucket, object_name, operation, source_codec, target_codec,
                         before_bytes, after_bytes, bytes_saved, status, details)
                    VALUES (%s, %s, 'COMPRESSION_BENCHMARK', 'unknown', 'zstd',
                            %s, %s, %s, 'BENCHMARKED', %s::jsonb)
                """, (
                    args.bucket, args.key, before, after, saved,
                    json.dumps(details),
                ))
            conn.commit()
            print("Benchmark result recorded in PostgreSQL.")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
