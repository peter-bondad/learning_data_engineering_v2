import boto3
from src.utils.get_env import get_env

def get_s3_client():
    host = get_env("MINIO_HOST")
    port = get_env("MINIO_API_PORT")

    endpoint = get_env("S3_ENDPOINT", f"http://{host}:{port}")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=get_env("S3_ACCESS_KEY"),
        aws_secret_access_key=get_env("S3_SECRET_KEY"),
    )