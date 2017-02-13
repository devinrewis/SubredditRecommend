import redis
import yaml
import json

with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)

def deliver_author_redis(x):
    rdb.hset('authorknn', x[0].lower(), json.dumps(x[1]))

def deliver_sub_redis(x):
    rdb.hset('subredditknn', x[0].lower(), json.dumps(x[1]))
