##########################################################################
# word2vec_train.py
##########################################################################
#
# This script trains a word2vec model on a set of comments
# from S3 and saves the resulting model back to S3 for
# later use.
#
# word2vec_transform.py uses this trained model to project
# comments into a vector space.
#
##########################################################################


from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import *
from pyspark.ml.feature import *
from pyspark.ml.linalg import *
from pyspark.ml.linalg import SparseVector, DenseVector, VectorUDT
from pyspark.mllib.stat import Statistics
from fnmatch import fnmatch
import pickle
import string
import boto3
import botocore
import yaml

sc = SparkContext(appName = "Word2Vec Train")
sqlContext = SQLContext(sc)

hiveContext = HiveContext(sc)
hiveContext.setConf("spark.sql.orc.filterPushdown", "true")

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

comments = hiveContext.read.format("orc").load(settings['orc-data'])

#select author & body columns
comments = comments.select(comments['author'], comments['subreddit'], comments['body'])
#filter out authors who've deleted their accounts
comments = comments.filter(comments.author != "[deleted]")
#filter out deleted comments, can't be analyzed
comments = comments.filter(comments.body != "[deleted]")

#tokenize comments for processing
tokenizer = RegexTokenizer(inputCol="body", outputCol="words") \
            .setPattern("[\\W_]+") \
            .setMinTokenLength(4)
comments = tokenizer.transform(comments)

#remove stop words
stopWordFile = open(settings['stop-word-file'])
sWords = stopWordFile.read().split('\n')
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=sWords)
comments = remover.transform(comments)

#parameters for word2vec model
word2vec = Word2Vec(vectorSize=8, minCount=15, maxIter=1, inputCol="filtered", outputCol="result")

#train the model against comment data
model = word2vec.fit(comments)

#use for debugging, show resulting dataframe
#should appear as dataframe containing words and vectors
#model.getVectors().show()

#save model to S3 for later use
model.save(settings['word2vec-model'])









