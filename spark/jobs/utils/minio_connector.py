import boto3
from botocore.client import Config

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

BUCKETS = {
    "bronze": "lakehouse-bronze",
    "silver": "lakehouse-silver",
    "gold":   "lakehouse-gold",
    "raw":    "lakehouse-raw",
}


def get_minio_client():
    """Return a boto3 S3 client pointing to MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_file(local_path: str, bucket_key: str, object_name: str):
    """Upload a local file to a MinIO bucket."""
    client = get_minio_client()
    bucket = BUCKETS[bucket_key]
    client.upload_file(local_path, bucket, object_name)
    print(f"Uploaded {local_path} → s3://{bucket}/{object_name}")


def list_objects(bucket_key: str, prefix: str = ""):
    client = get_minio_client()
    bucket = BUCKETS[bucket_key]
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", [])]
