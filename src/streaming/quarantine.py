"""Deterministic invalid-event handling for the Kafka streaming pipeline."""

import hashlib
import json
from datetime import datetime, timezone
from io import BytesIO

from minio.error import S3Error


def failure_reason(record):
    """Return a stable validation reason, or None for a valid order."""
    raw = record.get("json_value")
    if raw is None:
        return "missing_value"
    try:
        decoded = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return "invalid_json"
    if not isinstance(decoded, dict):
        return "record_must_be_json_object"
    for field in ("order_id", "product", "quantity", "unit_price", "order_status", "event_time"):
        if decoded.get(field) is None or decoded.get(field) == "":
            return f"missing_or_empty_{field}"
    try:
        if int(decoded["quantity"]) <= 0:
            return "quantity_must_be_positive"
        if float(decoded["unit_price"]) < 0:
            return "unit_price_must_be_nonnegative"
        int(decoded["order_id"])
    except (TypeError, ValueError):
        return "invalid_numeric_field"
    return None


def quarantine_key(prefix, topic, partition, offset):
    """Stable Kafka identity gives replay-safe, non-colliding object names."""
    return f"{prefix.rstrip('/')}/{topic}/partition-{partition}/offset-{offset}.json"


def quarantine_payload(record, reason):
    source_timestamp = record.get("source_timestamp")
    if hasattr(source_timestamp, "isoformat"):
        source_timestamp = source_timestamp.isoformat()
    payload = {
        "source_topic": record.get("topic"),
        "source_partition": int(record["partition"]),
        "source_offset": int(record["offset"]),
        "source_timestamp": source_timestamp,
        "quarantined_at": source_timestamp or datetime.now(timezone.utc).isoformat(),
        "failure_reason": reason,
        "original_record": record.get("json_value"),
    }
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def persist_quarantine(client, bucket, prefix, record, reason):
    key = quarantine_key(prefix, record["topic"], record["partition"], record["offset"])
    data = quarantine_payload(record, reason)
    digest = hashlib.sha256(data).hexdigest()
    try:
        existing = client.stat_object(bucket, key)
    except S3Error as error:
        if error.code not in ("NoSuchKey", "NoSuchObject", "NotFound"):
            raise
    else:
        response = client.get_object(bucket, key)
        try:
            actual = hashlib.sha256(b"".join(response.stream(1024 * 1024))).hexdigest()
        finally:
            response.close()
            response.release_conn()
        if existing.size != len(data) or actual != digest:
            raise RuntimeError(f"Quarantine key collision with different content: {bucket}/{key}")
        return key, False

    client.put_object(
        bucket,
        key,
        BytesIO(data),
        length=len(data),
        content_type="application/json",
        metadata={"sha256": digest, "failure-reason": reason},
    )
    return key, True

