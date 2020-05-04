### Run the follwing to setup

* Clone the repo
* sh start.sh
* go to another window
* export AIRFLOW_HOME=$PWD/airflow
* echo $AIRFLOW_HOME
* pipenv run airflow webserver -p 8080 -D

aws --endpoint-url=http://localhost:4572 s3 ls mybucket

If in a docker 
    curl host.docker.internal:4572

spark fixed jar from https://github.com/suburbanmtman/hadoop-2.7.3-spark-localstack/tree/master/dist