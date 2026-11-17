
import csv
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_FILE = Path(os.getenv(
    "ORDERS_CSV_FILE",
    str(PROJECT_ROOT / "data" / "sample" / "orders.csv"),
))
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Publish order events to Kafka.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of CSV rows to send (useful for controlled runs).",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=float(os.getenv("PRODUCER_DELAY_SECONDS", "1")),
        help="Delay between events; set to 0 for a short controlled run.",
    )
    args = parser.parse_args(argv)

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be greater than zero")
    if args.delay_seconds < 0:
        parser.error("--delay-seconds cannot be negative")

    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent_count = 0

    try:
        with CSV_FILE.open("r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                if args.limit is not None and sent_count >= args.limit:
                    break

                event = {
                    "order_id": int(row["order_id"]),
                    "product": row["product"],
                    "quantity": int(row["quantity"]),
                    "unit_price": float(row["unit_price"]),
                    "order_status": row["order_status"],
                    "event_time": datetime.now(timezone.utc).isoformat(),
                }

                try:
                    metadata = producer.send(TOPIC, value=event).get(timeout=10)
                    sent_count += 1
                    print(
                        f"Sent order {event['order_id']} "
                        f"to {metadata.topic}, partition={metadata.partition}, "
                        f"offset={metadata.offset}"
                    )
                except KafkaError as error:
                    print(f"Failed to send order {event['order_id']}: {error}")
                    raise

                if args.delay_seconds:
                    time.sleep(args.delay_seconds)

        producer.flush()
        print(f"Finished. Successfully sent {sent_count} orders.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
