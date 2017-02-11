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

subreddit_vectors = sqlContext.read.parquet(settings['subreddit-vectors'])
author_vectors = sqlContext.read.parquet(settings['author-vectors'])

subreddit_vectors = subreddit_vectors.select('vector').rdd.map(lambda row: row.vector)
author_vectors = author_vectors.select('vector').rdd.map(lambda row: row.vector)

vectors = subreddit_vectors.union(author_vectors)

X_test = [0.45051485,  0.50801887, -0.07704632,  0.22868334, -0.27106896, 0.33362839,  0.45018876,  0.37479838]

local_vecs = vectors.collect()

lshf = LSHForest(random_state=42)

lshf.fit(local_vecs)

distances, indices = lshf.kneighbors(X_test, n_neighbors=20)

print(distances)
print(indices)














