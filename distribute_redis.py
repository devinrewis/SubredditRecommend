import redis

rdb = redis.StrictRedis(host=settings['redis-host'], port=6379, db=0)

def deliver_redis(x):
    rdb.hset('authortest', x[0], x[1])
