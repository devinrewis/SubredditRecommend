# Subreddit Recommendation Engine using Word2Vec
The purpose of this project is to recommend subreddits to users based on the content of discussion that occurs in those subreddits.

Big thank you to [Jason Baumgartner](https://pushshift.io/) for providing the Reddit dataset. The data itself can be acquired from [here](https://files.pushshift.io/).

## Requirements
1. S3 Bucket with Reddit comments in JSON format
2. PySpark running on an AWS cluster
3. Riak database
4. Web Server

## Dependencies
On all nodes:
```bash
pip install pyyaml
pip install riak
pip install boto3
pip install numpy
pip install sklearn

sudo apt-get install python-dev libffi-dev libssl-dev
```

If you would like to use Redis then:
```
pip install redis-py
```
I would **highly** recommend you **don't** use Redis unless you are only running this on a subset of data. After running this app on the full reddit dataset the results turned out to be between 200-300 GB depending on how you set word2vec and LSHForest. There are millions of Reddit users and this application precomputes recommendations for all of them, so the results take up quite a bit of space.

## Running
Before running any of the scripts make sure to edit the settings.yaml and change the settings to be appropriate for your setup. The port for Riak is set to the default 8098, so make sure to change this if you need something different.

The order that I run these scripts is as follows:

1. json_to_orc.py
  * This will convert your JSON data to ORC
2. word2vec_train.py
  * This will train the word2vec algorithm and store the model to S3
  * **Caution:** The Spark MLLib implementation of word2vec will attempt to collect the model on to your master node so you can't actually train the model on the entire dataset unless you own a master node with 3 TB of memory. The way I got around this is by only training word2vec on a 6GB subset which gave me decent results.
3. word2vec_transform.py
  * This script will use the model from the previous step to transform each comment into a vector and then sum them up for both authors and subreddits and save the results to S3
4. knn.py
  * This script will use LSHForest and cosine similarity in order to the *k* nearest neighbors for each subreddit and author vector. By default I chose to have *k* be 100 since that is the max I would ever want to display on the website.
  * Be careful what you choose to set *k* to since you will have to store *k* results for every single author and subreddit in your dataset. When I used *k*=100 the results were nearly 400 GB when I ran the whole dataset through the pipeline.

If you'd like to test different settings through the whole pipeline I've written a script for this purpose called validate.py. I used this script to benchmark certain settings and dataset sizes.

## Optimizing
The word2vec implementation from MLLib has a limitation as I mentioned earlier, where you need to make sure you have enough memory to store the model. The main setting to change if you want to train your model on larger sets is minCount which will limit word2vecs vocabulary to words that only appear in as many comments as the value you set (i.e. if minCount=1000, then the word must appear in 1000 comments to be considered). Raising minCount will decrease your accuracy, but training your data on larger sets will increase your accuracy. So balancing minCount with the size of your subset is the main strategy to increasing word2vec's accuracy. I did not do extensive testing in this area as there are diminishing returns in respect to the recommendations because I am only matching the top 100 results and displaying the top 20. If you wanted to display more results than the marginal increases in accuracy you'd get from tuning these parameters will matter much more. For my case however, I was able to pass validation in that ~90% of the held out subreddits appeared in the top 20 results.
