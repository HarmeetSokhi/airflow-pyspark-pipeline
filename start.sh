docker-compose down
docker-compose up -d
pipenv run python setup.py
ps -ax |grep gunicorn
kill -9 $(lsof -i:8080 -t) 2> /dev/null
ps -ax |grep gunicorn
rm -f airflow/airflow-webserver.pid 
export AIRFLOW_HOME=$PWD/airflow
echo $AIRFLOW_HOME
pipenv install apache-airflow
pipenv install docker
pipenv run airflow initdb
pipenv run airflow scheduler