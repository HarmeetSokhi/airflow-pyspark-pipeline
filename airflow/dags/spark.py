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
print(f'current working directory: {currentDirectory}')
with DAG('docker_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
        t1 = BashOperator(
                task_id='print_current_date',
                bash_command='date'
        )


        t2 = DockerOperator(
                task_id='docker_command',
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