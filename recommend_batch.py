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
from pyspark.mllib.stat import Statistics
import redis
import json


sc = SparkContext(appName = "Recommend")
sqlContext = SQLContext(sc)

rdb = redis.StrictRedis(host='##REDIS HOST##', port=6379, db=0)

class VectorSpace:
    ##
    # VectorSpace
    ##
    #
    # This is a generalized class for computing cosine similarity between
    # two points in a vector space.
    #
    # The VectorSpace object is created by inputting a dataframe with
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
        a = self.vectorSpace.filter(self.vectorSpace.a == compareVector).collect()[0]['vector']
        
        a_mag = a.norm(2)

        #
        #compare a with list of b values
        # Formula:
        #    a dot b
        # -------------
        #  ||a||*||b||
        #
        similar = self.vectorSpace.rdd.mapValues(lambda b: (a.dot(b))/(a_mag * b.norm(2))) \
            .sortBy(lambda x: x[1], ascending=False) #sort values for output
        
        return similar


#load subreddit vectors from S3
subreddit_vectors = sqlContext.read.parquet("##STORAGE LOCATION FOR SUBREDDIT VECTORS##")
#author_vectors = sqlContext.read.parquet("##STORAGE LOCATION FOR AUTHOR VECTORS##")

#create VectorSpace object for comparison
subredditCompare = VectorSpace(subreddit_vectors)

#create list of subreddits to compare
sv = subreddit_vectors.rdd.keys().collect()

#do cosine comparison for each subreddit and store to Redis
for x in sv:
    rec_list = subredditCompare.cosine(x).collect()
    rec_json = json.dumps(rec_list)
    rdb.set(x, rec_json)






