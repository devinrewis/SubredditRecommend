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

#create connector to Redis
rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)

#read in vector data from S3
subreddit_vectors_df = sqlContext.read.parquet(settings['subreddit-vectors'])
author_vectors_df = sqlContext.read.parquet(settings['author-vectors'])

#create RDDs that contain only vectors (no keys)
subreddit_vectors = subreddit_vectors_df.select('vector').rdd.map(lambda row: row.vector)
author_vectors = author_vectors_df.select('vector').rdd.map(lambda row: row.vector)

#localize vectors for use with LSHForest
local_sub_vecs = subreddit_vectors.collect()

#localize subreddit rdd so that names can be found later
local_sub_names = subreddit_vectors_df.collect()

#train LSHForest to vector space
#only subreddits need to be hashed, since results will only be subreddits
lshf = LSHForest(random_state=42)
lshf.fit(local_sub_vecs)

#find allpairs similarity
s_results = subreddit_vectors_df.rdd.map(lambda x: [x.subreddit, lshf.kneighbors(x.vector, n_neighbors=100)])
a_results = author_vectors_df.rdd.map(lambda x: [x.author, lshf.kneighbors(x.vector, n_neighbors=100)])

#convert ugly output structure to [key, [sub cosine], [sub index]]
s_results = s_results.map(lambda x: [x[0], x[1][0].tolist()[0], x[1][1][0].tolist()])
a_results = a_results.map(lambda x: [x[0], x[1][0].tolist()[0], x[1][1][0].tolist()])


s_results = s_results.map(lambda x: [x[0], [[local_sub_names[x[2][k]], x[1][k]] for k in range(0, len(x[2])]])


print(s_results.take(10))
#print(a_results.take(10))










