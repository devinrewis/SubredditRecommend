## json_to_orc.py
#
# This code will convert plain text, single line json
# stored in an S3 bucket into Apache ORC file format
# and store it in another bucket.
#
# WARNING: This script will load all of your JSONS on to your
# cluster at once so make sure you have enough disk space and memory.
#
##

from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import json

#create Spark context
sc = SparkContext(appName = "S3 JSON to ORC")
sq = SparkSession \
    .builder \
    .getOrCreate()


file = sc.textFile("####S3 BUCKET CONTAINING JSONS####").persist(StorageLevel(True, True, False, False, 1))
comments = sq.read.json(file)
comments.write.mode('append').format("orc").save("s3n://reddit-comment-data-orc/data-test/")
