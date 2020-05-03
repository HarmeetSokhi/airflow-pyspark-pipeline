from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
import os
from boto.s3.connection import S3Connection

default_args = {
        'owner'                 : 'airflow',
        'description'           : 'Use of the DockerOperator',
        'depend_on_past'        : False,
        'start_date'            : datetime(2020, 1, 3),
        'email_on_failure'      : False,
        'email_on_retry'        : False,
        'retries'               : 1,
        'retry_delay'           : timedelta(minutes=5)
}

TestBucketName = "mybucket"

def setup():
    import boto3
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=TestBucketName)
    s3.create_bucket(Bucket=TestBucketName, CreateBucketConfiguration={
    'LocationConstraint': 'us-west-1'})
    s3.Object(TestBucketName, 'hello.txt').put(Body=open('/tmp/hello.txt', 'rb'))


currentDirectory = os.getcwd()
with DAG('spark_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
        t1 = BashOperator(
                task_id='print_current_date',
                bash_command='date'
        )


        t2 = DockerOperator(
                task_id='spark_docker',
                image='jupyter/all-spark-notebook',
                api_version='auto',
                auto_remove=True,
                network_mode="bridge",
                docker_url="unix://private/var/run/docker.sock",
                host_tmp_dir='/tmp', 
                tmp_dir='/tmp',
                volumes=[f'{currentDirectory}/pyspark:/home/jovyan'],
                command='spark-submit --master local[*] script/hellospark.py'
        )

        t3 = BashOperator(
                task_id='print_hello',
                bash_command='echo "hello world"'
        )

        t1 >> t2 >> t3