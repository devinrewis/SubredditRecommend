from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import json
import yaml

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        
#create Spark, Hive, and SQL contexts
sc = SparkContext(appName = "Find Inactive")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
hiveContext.setConf("spark.sql.orc.filterPushdown", "true")

#load comment data
comments = hiveContext.read.format("orc").load(settings['orc-data'])

#count commments to find low activity subreddits
commentCounts = comments.select(comments['subreddit'])
commentCounts = commentCounts.groupby('subreddit').count().sort('count')

lowActivitySubs = commentCounts.filter(commentCounts['count'] <= 1000)

lowActivitySubs.show()








