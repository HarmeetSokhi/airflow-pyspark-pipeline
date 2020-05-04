from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext
import shutil

if __name__ == "__main__":

    sc = SparkContext("local","PySpark Word Count Exmaple")

        # read data from text file and split each line into words
    words = sc.textFile("data/input/wordcount.txt").flatMap(lambda line: line.split(" "))

    # count the occurrence of each word
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    # save the counts to output
      
    try:
          shutil.rmtree('data/output')
    except:
        print("No output folder found")
    wordCounts.saveAsTextFile("data/output")