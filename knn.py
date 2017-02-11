from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import *
from pyspark.ml.feature import *
from pyspark.ml.linalg import *
from pyspark.ml.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
from operator import add
from distribute_redis import *
from sklearn.neighbors import LSHForest

#create spark context and SQL context
sc = SparkContext(appName = "Recommend")
sqlContext = SQLContext(sc)

sc.addFile("settings.yaml")
sc.addPyFile("distribute_redis.py")

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)

subreddit_vectors = sqlContext.read.parquet(settings['subreddit-vectors']).limit(10)
author_vectors = sqlContext.read.parquet(settings['author-vectors']).limit(10)

subreddit_vectors = subreddit_vectors.select('vector')rdd.map(lambda l: l)
author_vectors = author_vectors.select('vector')rdd.map(lambda l: l)

vectors = subreddit_vectors.union(author_vectors)

print(vectors.collect())


















