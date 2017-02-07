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
from pyspark.sql import Row
import boto3
import botocore
import json
import yaml

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
'''
def fetch_files(key):
    for line in key.get_contents_as_string().splitlines():
        j = json.loads(line)
        yield j

conn = S3Connection()
bucket = conn.get_bucket(settings['json-data'])
keys = sc.parallelize(bucket.list())
files = keys.flatMap(fetch_files)
'''
s3 = boto3.resource('s3')
bucket = s3.Bucket(settings['json-data'])
keyList = [k.key for k in bucket.objects.all()]

def distributedJsonRead(s3Key):
    s3obj = boto3.resource('s3').Object(bucket_name=settings['json-data'], key=s3Key)
    contents = json.loads(s3obj.get()['Body'].read().decode('utf-8'))
    for dicts in content['interactions']:
        yield Row(**dicts)

pkeys = sc.parallelize(keyList) #keyList is a list of s3 keys
dataRdd = pkeys.flatMap(distributedJsonRead)

print(dataRdd.top(3))
#file = sc.textFile(settings['json-data']).persist(StorageLevel(True, True, False, False, 1))
#comments = sq.read.json(file)
#comments.write.mode('append').format("orc").save(settings['orc-data'])
















