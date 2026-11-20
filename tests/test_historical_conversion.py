import hashlib
import pytest
from pyspark.sql import SparkSession

from scripts.convert_historical_minio_csv import (
    convert_csv_file,
    object_key_for,
    upload_verified_output,
    verify_conversion,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("historical-conversion-unit-tests")
        .config("spark.local.dir", "/dev/shm/spark-conversion-tests")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_historical_csv_converts_with_rows_and_schema_preserved(spark, tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("id,product\n1,pen\n2,book\n", encoding="utf-8")
    result = convert_csv_file(spark, source, tmp_path / "parquet")
    assert result["row_count"] == 2
    assert result["size_bytes"] > 0
    assert len(result["sha256"]) == 64
    parquet = spark.read.parquet(str(result["path"]))
    assert parquet.count() == 2
    assert parquet.schema.jsonValue() == result["schema"]


def test_empty_historical_csv_is_rejected(spark, tmp_path):
    source = tmp_path / "empty.csv"
    source.write_text("id,product\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        convert_csv_file(spark, source, tmp_path / "empty-parquet")


def test_malformed_historical_csv_fails_fast(spark, tmp_path):
    source = tmp_path / "malformed.csv"
    source.write_text('id,product\n1,pen\n2,"unterminated\n', encoding="utf-8")
    with pytest.raises(ValueError, match="Malformed CSV"):
        convert_csv_file(spark, source, tmp_path / "malformed-parquet")


def test_schema_mismatch_is_rejected():
    schema = {"fields": [{"name": "id", "type": "integer"}]}
    other_schema = {"fields": [{"name": "id", "type": "string"}]}
    with pytest.raises(RuntimeError, match="row count or schema"):
        verify_conversion(2, schema, 2, other_schema)


def test_destination_key_is_stable_and_source_scoped():
    digest = hashlib.sha256(b"same source bytes").hexdigest()
    first = object_key_for("legacy/one/orders.csv", digest, "converted/historical")
    repeated = object_key_for("legacy/one/orders.csv", digest, "converted/historical")
    other = object_key_for("legacy/two/orders.csv", digest, "converted/historical")
    assert first == repeated
    assert first != other


def test_failed_historical_upload_is_visible(tmp_path):
    parquet = tmp_path / "output.parquet"
    parquet.write_bytes(b"parquet-content")

    class FailingClient:
        def fput_object(self, *_args, **_kwargs):
            raise OSError("MinIO upload failed")

    with pytest.raises(OSError, match="MinIO upload failed"):
        upload_verified_output(
            FailingClient(), "warm-data", "converted/file.parquet", parquet,
            parquet.stat().st_size, hashlib.sha256(parquet.read_bytes()).hexdigest(),
        )

