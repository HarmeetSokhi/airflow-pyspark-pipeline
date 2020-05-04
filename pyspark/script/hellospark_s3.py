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
s3Inputfilename = "wordcount.txt"
s3Outfileprefix= "output"


def get_s3_resource():
    return boto3.resource(
        's3',
        aws_access_key_id='foo',
        aws_secret_access_key='bar',
        region_name='au-southeast-2',
        endpoint_url=AwsEndpointUriStrn
    )

def set_spark_context():
    sc = SparkContext("local","PySpark Word Count Exmaple")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "abc")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "xyz")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", AwsEndpointUriStrn)
    sc._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "3")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.multiobjectdelete.enable", "false")
    sc._jsc.hadoopConfiguration().set("fs.s3a.change.detection.version.required", "false")
    return sc

def run_word_count(sc):
    # read data from s3 file
    file_s3_text = sc.textFile(f"s3a://{TestBucketName}/{s3Inputfilename}")

    # Split each line into words
    words = file_s3_text.flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    # delete the old s3 output files
    s3 = get_s3_resource()
    bucket = s3.Bucket(TestBucketName)
    bucket.objects.filter(Prefix=f"{s3Outfileprefix}/").delete()
    
    # save the counts to s3 output
    wordCounts.saveAsTextFile(f"s3a://{TestBucketName}/{s3Outfileprefix}")

if __name__ == "__main__":
    sc = set_spark_context()
    run_word_count(sc)


