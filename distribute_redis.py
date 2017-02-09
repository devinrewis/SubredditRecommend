import redis
import yaml

with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)

def deliver_author_redis(x):
    rdb.hset('authortest2', x[0].lower(), x[1])

def deliver_sub_redis(x):
    rdb.hset('subreddit', x[0].lower(), x[1])
