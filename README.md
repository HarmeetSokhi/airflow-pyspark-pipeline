### What is this repo?
This repo has below sample pipelines :
1. spark_pipeline_file: 
    * Reads from an input file 
    * Spins up Pyspark Docker using dockeroperator
    * runs wordcount for the input file in the data folder and
    * writes to an output file in the data folder of the repo.
2. spark_pipeline_mock_s3: 
    * Reads from an input file from s3 from localstack s3://mybucket/input
    * Spins up Pyspark Docker using docker operator
    * runs wordcount for the input file   and
    * writes to an output file in the s3://mybucket/output 
 

### How to setup and run the airflow pipeline

* Clone this repo
* Make sure you have pipenv installed
* Run the following in the terminal in the cloned repo
    * sh start.sh
* go to another window and run the following in the cloned repo
    * sh start_airflow.sh
* You should be able to see the airflow GUI at 
    * http://localhost:8080/admin/


Other Notes:

* To view Bucket:   aws --endpoint-url=http://localhost:4572 s3 ls mybucket

* In the docker to access locol host use the following: 
    * curl host.docker.internal:4572

* Spark jar  to support S3 has been downloaded from https://github.com/suburbanmtman/hadoop-2.7.3-spark-localstack/tree/master/dist

* If you see default example dags provided by Apache-airflow then set "load_examples = False" in airflow.cfg 
