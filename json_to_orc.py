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
from boto.s3.connection import S3Connection
import json
import yaml
import boto3
import botocore

#create Spark context and Spark session
sc = SparkContext(appName = "S3 JSON to ORC")
sq = SparkSession \
    .builder \
    .getOrCreate()

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

#get a list of files contained in S3 bucket
client = boto3.client('s3')
s3ObjectList = client.list_objects_v2(Bucket='dr-reddit-comment-data')
s3ObjectList = s3ObjectList['Contents']

def fetch_data(s3key):
    file = sq.read.json(settings['json-data'] + str(s3key))
    file.write.mode('append').format("orc").save(settings['orc-data'])

fileList = map(lambda d: d.get('Key'), s3ObjectList)
map(fetch_data, fileList)
















