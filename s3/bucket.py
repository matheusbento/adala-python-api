import boto3
import string
from minio import Minio
from minio.error import S3Error
from dotenv import dotenv_values

config = dict(dotenv_values(".env"))

AWS_KEY = config['AWS_KEY']
AWS_HOST = config['MINIO_ENDPOINT']
AWS_ACCESS = config['AWS_SECRET']
BUCKET_NAME = config['AWS_BUCKET']


def generate_s3_session(bucket_name: string = None):
    if bucket_name is None:
        bucket_name = 'datalake'
    # s3_client = boto3.client('s3')
    # s3_bucket_name = bucket_name
    # s3 = boto3.resource('s3',
    #                     aws_access_key_id=AWS_KEY,
    #                     aws_secret_access_key=AWS_ACCESS)

    client = Minio(
        # endpoint=AWS_HOST,
        # access_key=AWS_KEY,
        # secret_key=AWS_ACCESS,
        # secure=False
        endpoint='localhost:9010',
        access_key='DxEj1KDo2tCZzmWJ',
        secret_key='vQoHEqzNxs22ZfNRuy0g8j3tUbFprgYw',
        secure=False
    )

    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print("Bucket '{bucket}' already exists".format(bucket=bucket_name))

    return client, bucket_name
