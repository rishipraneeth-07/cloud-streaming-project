import os
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

bucket_name = "hot-data"

for prefix in ["batch/orders/", "batch/product_summary/"]:
    print(f"\nObjects under {prefix}")

    objects = list(
        client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=True,
        )
    )

    for obj in objects:
        print(f"Name: {obj.object_name}")
        print(f"Size: {obj.size} bytes")

    print(f"Object count: {len(objects)}")

