from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf
import shutil
import os
import boto3

MockS3ServerPortEnvVar = 4572
hostdns="host.docker.internal"
AwsEndpointUriStrn= f"http://{hostdns}:{MockS3ServerPortEnvVar}"
TestBucketName= "mybucket"
s3filename = "wordcount.txt"


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id='foo',
        aws_secret_access_key='bar',
        region_name='au-southeast-2',
        endpoint_url='http://localhost:4572'
    )

if __name__ == "__main__":


    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-common:2.7.3" pyspark-shell'
    )
    sc = SparkContext("local","PySpark Word Count Exmaple")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "abc")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xyz")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"http://{hostdns}:{MockS3ServerPortEnvVar}")
    sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "3")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.multiobjectdelete.enable", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.change.detection.version.required", "false")



        # read data from text file and split each line into words
    file_s3_text = sc.textFile(f"s3a://{TestBucketName}/{s3filename}")
    words = file_s3_text.flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    # save the counts to output

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(TestBucketName)
    bucket.objects.filter(Prefix="/output").delete()

    wordCounts.saveAsTextFile(f"s3a://{TestBucketName}/output")
