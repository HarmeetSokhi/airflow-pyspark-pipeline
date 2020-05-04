
import boto3
import botocore
import os

TestBucketName = "mybucket"

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id='foo',
        aws_secret_access_key='bar',
        region_name='au-southeast-2',
        endpoint_url='http://localhost:4572'
    )

def setup():
    currentDirectory = os.getcwd()
    s3 = get_s3_client()
    s3.create_bucket(Bucket=TestBucketName)
    s3.upload_file(f'{currentDirectory}/pyspark/data/input/wordcount.txt', TestBucketName,"wordcount.txt")
    ##s3.Object(TestBucketName, 'hello.txt').put(Body=open('/tmp/hello.txt', 'rb'))

if __name__ == "__main__":
    setup()
