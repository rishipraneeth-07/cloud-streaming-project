import os

from minio import Minio
from minio.commonconfig import CopySource

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

SOURCE_BUCKET = "hot-data"
DESTINATION_BUCKET = "warm-data"
SOURCE_PREFIX = "batch/orders/"

DRY_RUN = False

objects = list(
    client.list_objects(
        SOURCE_BUCKET,
        prefix=SOURCE_PREFIX,
        recursive=True,
    )
)

for obj in objects:
    object_name = obj.object_name

    print(f"\nObject: {object_name}")
    print(f"Source size: {obj.size} bytes")

    if DRY_RUN:
        print("DRY RUN: Migration skipped; no changes made.")
        continue

    client.copy_object(
        DESTINATION_BUCKET,
        object_name,
        CopySource(SOURCE_BUCKET, object_name),
    )

    source = client.stat_object(SOURCE_BUCKET, object_name)
    destination = client.stat_object(DESTINATION_BUCKET, object_name)

    if source.size != destination.size:
        print("Verification failed. Source object preserved.")
        continue

    print("Destination verified successfully.")

    client.remove_object(SOURCE_BUCKET, object_name)
    print("Source object deleted after verification.")
