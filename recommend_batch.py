##########################################################################
# recommend.py
##########################################################################
#
# This script uses cosine similarity to recommend subreddits based
# on subreddit vectors that appear nearby the user input vector
#
##########################################################################

from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import *
from pyspark.ml.feature import *
from pyspark.ml.linalg import *
from pyspark.ml.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
import redis
import json
import yaml

#create spark context and SQL context
sc = SparkContext(appName = "Recommend")
sqlContext = SQLContext(sc)

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)
'''
class CosineSim:
    ##
    # CosineSim
    ##
    #
    # This is a generalized class for computing cosine similarity between
    # two points in a vector space.
    #
    # The CosineSim object is created by inputting a dataframe with
    # the first column containing keys and the second containing vector values.
    #
    # The cosine function will return keys and values containing the degrees of
    # distance from the provided vector.
    #
    
    def __init__(self, vs):
        self.vectorSpace = vs
        oldColumns = self.vectorSpace.columns
        newColumns = ["a", "vector"]
        self.vectorSpace = self.vectorSpace.withColumnRenamed(oldColumns[0], newColumns[0]).withColumnRenamed(oldColumns[1], newColumns[1])
        self.vectorKeys = self.vectorSpace.select('a').collect()
    
    def cosine(self, compareVector):
        ##
        # Cosine Similarity 
        ##
        #
        # compareVector: key of vector to be compared
        #
        # This function returns a dataframe containing keys of vectors sorted by
        # their distance from compareVector
        #
        
        #find the vector based on key provided by compareVector
        #a = self.vectorSpace.filter(self.vectorSpace.a == compareVector).collect()[0]['vector']
        a = compareVector
        a_mag = a.norm(2)

        #
        #compare a with list of b values
        # Formula:
        #    a dot b
        # -------------
        #  ||a||*||b||
        #
        
        #similar = self.vectorSpace.rdd.mapValues(lambda b: (a.dot(b))/(a_mag * b.norm(2))) \
        #    .sortBy(lambda x: x[1], ascending=False) #sort values for output
        
        for bkey in self.vectorKeys:
            b = 
        
        return similar
'''

def cosineSim(aVectors, bVectors):
       #Rename columns
       oldColumns = aVectors.columns
       newColumns = ["a", "a_vector"]
       aVectors = aVectors.withColumnRenamed(oldColumns[0], newColumns[0]).withColumnRenamed(oldColumns[1], newColumns[1])
       
       oldColumns = bVectors.columns
       newColumns = ["b", "b_vector"]
       bVectors = bVectors.withColumnRenamed(oldColumns[0], newColumns[0]).withColumnRenamed(oldColumns[1], newColumns[1])
       ###
       
       vectors = aVectors.crossJoin(bVectors)
       
       return vectors

#load subreddit vectors from S3
subreddit_vectors = sqlContext.read.parquet(settings['subreddit-vectors'])
author_vectors = sqlContext.read.parquet(settings['author-vectors'])

subreddit_vectors = subreddit_vectors.take(3)
author_vectors = author_vectors.take(3)

result = cosineSim(author_vectors, subreddit_vectors)
result.show()

#create CosineSim object for comparison
#subredditCompare = CosineSim(subreddit_vectors)
'''
##Compare Subreddits to Subreddits
#create list of subreddits to compare
sv = subreddit_vectors.rdd.keys().collect()

#do cosine comparison for each subreddit and store to Redis
for x in sv:
    rec_list = subredditCompare.cosine(x).collect()
    rec_json = json.dumps(rec_list)
    rdb.hset('subreddit', x, rec_json)
  
##Compare Authors to Subreddits

#create list of subreddits to compare
sv = author_vectors.rdd.keys().collect()

#do cosine comparison for each subreddit and store to Redis
for x in sv:
    rec_list = subredditCompare.cosine(author_vectors.filter(author_vectors.author == x).collect()[0]['vector']).collect()
    rec_json = json.dumps(rec_list)
    rdb.hset('author', x, rec_json)
'''






