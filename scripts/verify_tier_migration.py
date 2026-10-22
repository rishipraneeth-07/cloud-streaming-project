import os

from minio import Minio

client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ACCESS_KEY"],
    secret_key=os.environ["MINIO_SECRET_KEY"],
    secure=False,
)

object_name = (
    "batch/orders/"
    "part-00000-99ec7d75-e5fb-4f4c-8429-9ce144f90af0-c000.snappy.parquet"
)

for bucket in ["hot-data", "warm-data"]:
    try:
        obj = client.stat_object(bucket, object_name)
        print(f"{bucket}: object exists ({obj.size} bytes)")
    except Exception as exc:
        if getattr(exc, "code", "") == "NoSuchKey":
            print(f"{bucket}: object not found")
        else:
            raise
