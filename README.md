Run the follwing to setup


ps -ax |grep gunicorn
kill -9 $(lsof -i:8080 -t) 2> /dev/null
ps -ax |grep gunicorn
rm airflow/airflow-webserver.pid 
export AIRFLOW_HOME=$PWD/airflow-pyspark-pipeline/airflow
echo $AIRFLOW_HOME
pipenv install apache-airflow
pipenv install docker
pipenv run airflow initdb
pipenv run airflow scheduler
export AIRFLOW_HOME=$PWD/airflow-pyspark-pipeline/airflow
pipenv run airflow webserver -p 8080