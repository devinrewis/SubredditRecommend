import riak
import yaml
import json

with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

myClient = riak.RiakClient(host=settings['riak-host'], port=settings['riak-port'])

def deliver_author(x):
    bucket = myClient.bucket('author')
    key = bucket.new(x[0].lower(), data=json.dumps(x[1]))
    key.store()

def deliver_sub(x):
    bucket = myClient.bucket('subreddit')
    key = bucket.new(x[0].lower(), data=json.dumps(x[1]))
    key.store()
