from pyspark import SparkContext
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import *
from pyspark.ml.linalg import DenseVector
from sklearn.neighbors import LSHForest
import numpy as np
import yaml

sc = SparkContext(appName = "Validate")
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

#select author, subreddit & body columns
comments = comments.select(comments['author'], comments['subreddit'], comments['body'])

#filter out authors who've deleted their accounts
comments = comments.filter(comments.author != "[deleted]")

#count commments to find top posters and their top subreddit
commentCounts = comments.select(comments['author'], comments['subreddit'])
commentCounts = commentCounts.groupby('author', 'subreddit').count().sort('count', ascending=False)

commentCounts.show()


######create list of authors to analyze
testList = commentCounts.take(100)
testList = [testList[1]]

#tokenize comments for processing
tokenizer = RegexTokenizer(inputCol="body", outputCol="words") \
            .setPattern("[\\W_]+") \
            .setMinTokenLength(4)
comments = tokenizer.transform(comments)

#filter stop words from comments
stopWordFile = open(settings['stop-word-file'])
sWords = stopWordFile.read().split('\n')
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=sWords)
comments = remover.transform(comments)

#parameters for word2vec model
word2vec = Word2Vec(vectorSize=8, minCount=15, maxIter=1, numPartitions=settings['numPartitions'], inputCol="filtered", outputCol="result")


#run test for each author individually
for author in testList:
    print(author)
    #filter out comments from author's top subreddit
    commentTest = comments.filter(((comments.author == author['author']) & (comments.subreddit != author['subreddit'])) | (comments.author != author['author']))
    
    #train word2vec on filtered subset
    model = word2vec.fit(commentTest)
    
    #create vectors from word2vec network
    subreddit_vectors = model.transform(commentTest)
    author_vectors = subreddit_vectors

    subreddit_vectors = subreddit_vectors.select('subreddit', 'result')
    author_vectors = author_vectors.select('author', 'result')

    #combine vectors for subreddits
    subreddit_vectors_df = subreddit_vectors.rdd.mapValues(lambda v: v.toArray()) \
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(lambda x: DenseVector(x)) \
        .toDF(['subreddit', 'vector'])
        
    #combine vectors for authors
    author_vectors_df = author_vectors.rdd.mapValues(lambda v: v.toArray()) \
        .reduceByKey(lambda x, y: x + y) \
        .mapValues(lambda x: DenseVector(x)) \
        .toDF(['author', 'vector'])
    
    #get vector for tested author
    author_test_vector = author_vectors_df.filter(author_vectors_df.author == author['author'])
    
    author_test_vector = author_test_vector.collect()[0]
    print(author_test_vector)
    
    
    #create RDDs that contain only vectors
    subreddit_vectors = subreddit_vectors_df.select('vector').rdd.map(lambda row: row.vector)
    author_vectors = author_vectors_df.select('vector').rdd.map(lambda row: row.vector)

    #localize vectors for use with LSHForest
    local_sub_vecs = subreddit_vectors.collect()

    #create a list of subreddit names so they can be accessed later
    subreddit_names = subreddit_vectors_df.select('subreddit').rdd.map(lambda row: row.subreddit)
    local_sub_names = subreddit_names.collect()

    #train LSHForest to vector space
    #only subreddits need to be hashed, since results will only be subreddits
    lshf = LSHForest(random_state=42)
    lshf.fit(local_sub_vecs)

    #find allpairs similarity
    a_results = list(lshf.kneighbors(author_test_vector['vector'], n_neighbors=100))
    
    #convert ugly output structure to [[sub cosine], [sub index]]
    a_results = [a_results[0][0].tolist(), a_results[1][0].tolist()]
    print(a_results)
    
    #a_results = map(lambda x: [local_sub_names[x[1][k]], 1 - x[0][k]], a_results)
    a_results = [[local_sub_names[a_results[1][x]], 1 - a_results[0][x]] for x in range(0, len(a_results[0]))]
    
    #create list of recommendations
    #author_rec_list = author_results.collect()
    
    
    print(a_results)
    #check to see where top sub occurs in recommendation










