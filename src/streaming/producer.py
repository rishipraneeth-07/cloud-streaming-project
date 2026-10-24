
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_FILE = PROJECT_ROOT / "data" / "sample" / "orders.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def main():
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

                time.sleep(1)

        producer.flush()
        print(f"Finished. Successfully sent {sent_count} orders.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
