import json
from types import SimpleNamespace

import pytest
from minio.error import S3Error

from src.streaming.quarantine import (
    failure_reason,
    persist_quarantine,
    quarantine_key,
)


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def stream(self, _chunk_size):
        yield self.data

    def close(self):
        pass

    def release_conn(self):
        pass


class FakeMinio:
    def __init__(self):
        self.objects = {}

    def stat_object(self, bucket, key):
        if (bucket, key) not in self.objects:
            raise S3Error(None, "NoSuchKey", "missing", key, None, None)
        return SimpleNamespace(size=len(self.objects[(bucket, key)]))

    def put_object(self, bucket, key, stream, length, **_kwargs):
        self.objects[(bucket, key)] = stream.read(length)

    def get_object(self, bucket, key):
        return FakeResponse(self.objects[(bucket, key)])


def valid_record():
    return {
        "topic": "orders",
        "partition": 0,
        "offset": 41,
        "source_timestamp": "2026-11-10T10:00:00+00:00",
        "json_value": '{"order_id":1,"product":"pen","quantity":2,"unit_price":3,"order_status":"COMPLETED","event_time":"now"}',
    }


def test_valid_event_has_no_quarantine_reason():
    assert failure_reason(valid_record()) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("{broken", "invalid_json"), ('[]', "record_must_be_json_object"),
     ('{"order_id":1}', "missing_or_empty_product")],
)
def test_invalid_event_has_clear_reason(raw, expected):
    record = valid_record()
    record["json_value"] = raw
    assert failure_reason(record) == expected


def test_quarantine_key_is_stable_for_kafka_identity():
    assert quarantine_key("quarantine/orders", "orders", 2, 99) == (
        "quarantine/orders/orders/partition-2/offset-99.json"
    )


def test_quarantine_write_is_idempotent_for_replayed_record():
    client = FakeMinio()
    record = valid_record()
    first_key, created = persist_quarantine(client, "hot-data", "quarantine/orders", record, "test")
    second_key, created_again = persist_quarantine(client, "hot-data", "quarantine/orders", record, "test")
    assert first_key == second_key
    assert created and not created_again
    envelope = json.loads(client.objects[("hot-data", first_key)])
    assert envelope["original_record"] == record["json_value"]
    assert envelope["source_offset"] == 41


def test_quarantine_collision_with_different_content_fails():
    client = FakeMinio()
    record = valid_record()
    key, _ = persist_quarantine(client, "hot-data", "quarantine/orders", record, "first")
    with pytest.raises(RuntimeError, match="collision"):
        persist_quarantine(client, "hot-data", "quarantine/orders", record, "different")


def test_quarantine_storage_failure_is_visible():
    class FailingMinio(FakeMinio):
        def put_object(self, *_args, **_kwargs):
            raise OSError("MinIO unavailable")

    with pytest.raises(OSError, match="MinIO unavailable"):
        persist_quarantine(FailingMinio(), "hot-data", "quarantine/orders", valid_record(), "test")

