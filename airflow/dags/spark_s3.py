from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
import os


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


currentDirectory = os.getcwd()
with DAG('spark_pipeline_mock_s3', default_args=default_args, schedule_interval=None, catchup=False) as dag:
        t1 = BashOperator(
                task_id='Start_of_Dag',
                bash_command='date'
        )


        t2 = DockerOperator(
                task_id='Spark_S3_Wordcount',
                image='jupyter/all-spark-notebook',
                api_version='auto',
                auto_remove=True,
                network_mode="bridge",
                docker_url="unix://private/var/run/docker.sock",
                host_tmp_dir='/tmp', 
                tmp_dir='/tmp',
                volumes=[f'{currentDirectory}/pyspark:/home/jovyan'],
                command='sh -c "pip install boto3 && \
                        spark-submit --jars script/aws-java-sdk-1.7.4.jar,script/hadoop-aws-2.7.3.jar --master local[*] script/hellospark_s3.py"'
                )

        t3 = BashOperator(
                task_id='End_of_Dag',
                bash_command='echo "Bye'
        )

        t1 >> t2 >> t3