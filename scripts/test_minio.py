import os
from minio import Minio


def main():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    if client.bucket_exists("hot-data"):
        print("Connected to MinIO successfully")
        print("Bucket 'hot-data' exists")
    else:
        print("Connected to MinIO successfully")
        print("Bucket 'hot-data' does not exist")


if __name__ == "__main__":
    main()
