import os
from pathlib import Path

from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

bucket_name = "hot-data"

datasets = {
    "orders": Path("data/processed/orders"),
    "product_summary": Path("data/processed/product_summary"),
}

for dataset_name, directory in datasets.items():
    for file_path in directory.glob("*.parquet"):
        object_name = f"batch/{dataset_name}/{file_path.name}"

        client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
        )

        print(f"Uploaded: {object_name}")
