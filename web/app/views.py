from app import app
from flask import render_template, request
import json
import requests
import redis
import yaml

#load settings.yaml
with open("settings.yaml", 'r') as stream:
    try:
        settings = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

@app.route('/', methods=['GET', 'POST'])
@app.route('/index')
def index():
    errors = []
    rec = dict() #dict to store recommendations

    rdb = redis.StrictRedis(host=settings['redis-host'], port=settings['redis-port'], db=0)
    if request.method == "POST":
        try:
            rec = rdb.hget(request.form['rec-type'], request.form['input-box']).decode('utf-8')
            rec = json.loads(rec)
            rec = rec[:20]
        except:
            errors.append("Unable to find subreddit or user. Try Again")
    return render_template('index.html', title = 'SubRec', errors=errors, rec=rec)
