from botocore.exceptions import ClientError
import boto3
from src.utils.get_env import get_env

from datetime import datetime, timezone
import json


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=get_env("S3_ENDPOINT"),
        aws_access_key_id=get_env("S3_ACCESS_KEY"),
        aws_secret_access_key=get_env("S3_SECRET_KEY"),
    )

def ensure_bucket_exists(bucket: str):
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

def get_bucket() -> str:
    return get_env("S3_BUCKET")


def upload_raw_data(data: list) -> str:
    if not isinstance(data, list):
        raise ValueError("Expected list of records")

    s3 = get_s3_client()
    bucket = get_bucket()

    ensure_bucket_exists(bucket)

    now = datetime.now(timezone.utc)

    # Use a structured key format for better organization and potential partitioning
    # Format: raw/coin_cap/year=2024/month=01/day=01/HHMMSS.json
    key = (
        f"raw/coin_cap/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{now.strftime('%H%M%S')}.json"
    )

    # Upload the data as JSON
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )

    return key
def read_raw_data(key: str):
    s3 = get_s3_client()
    bucket = get_bucket()

    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))