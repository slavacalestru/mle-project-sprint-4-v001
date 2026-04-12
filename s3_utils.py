# Импортируем необходимые библиотеки для работы с S3
import os
import io
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import pandas as pd

load_dotenv(override=True)


# Функция для получения клиента S3
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url="https://storage.yandexcloud.net",
        config=Config(
            signature_version="s3v4",
            s3={"payload_signing_enabled": False},
        ),
    )


# Функция для загрузки parquet файла из S3 в DataFrame
def load_parquet_from_s3(key, **kwargs):
    bucket = os.environ["S3_BUCKET_NAME"]
    s3 = get_s3_client()
    buf = io.BytesIO(
        s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    )
    return pd.read_parquet(buf, **kwargs)
