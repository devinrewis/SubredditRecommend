# Subreddit Recommendation Engine using Word2Vec
The purpose of this project is to recommend subreddits to users based on the content of discussion that occurs in those subreddits.

Big thank you to [Jason Baumgartner](https://pushshift.io/) for providing the Reddit dataset. The data itself can be acquired from [here](https://files.pushshift.io/).

## Requirements
1. S3 Bucket with Reddit comments in JSON format
2. PySpark running on an AWS cluster
3. Redis database
4. Web Server
