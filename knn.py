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
import numpy as np

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

subreddit_vectors_df = sqlContext.read.parquet(settings['subreddit-vectors'])
author_vectors_df = sqlContext.read.parquet(settings['author-vectors'])

subreddit_vectors = subreddit_vectors_df.select('vector').rdd.map(lambda row: row.vector)
author_vectors = author_vectors_df.select('vector').rdd.map(lambda row: row.vector)

#vectors = subreddit_vectors.union(author_vectors)

#local_vecs = vectors.collect()
local_sub_vecs = subreddit_vectors.collect()
local_sub_keys = subreddit_vectors_df.collect()


lshf = LSHForest(random_state=42)

#lshf.fit(local_vecs)
lshf.fit(local_sub_vecs)

#distances, indices = lshf.kneighbors(X_test, n_neighbors=50)

s_results = subreddit_vectors_df.rdd.map(lambda x: [x.subreddit, lshf.kneighbors(x.vector, n_neighbors=100)])
a_results = author_vectors_df.rdd.map(lambda x: [x.author, lshf.kneighbors(x.vector, n_neighbors=100)])

s_results = s_results.map(lambda x: [x[0], [local_sub_keys[r], x[1][r] for r in x[2]])
#a_results = a_results.mapValues(deliver_author_redis)


print(s_results.take(10))
#print(a_results.take(10))










