from app import app
from flask import render_template, request
from query import *
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
    
    form = QueryForm(request.POST)

    rdb = redis.StrictRedis(host=settings['redis-host'], port=settings['redis-port'], db=0)
    
    if request.method == "POST" and form.validate():
        userInput['type'] = form.inputType.data
        userInput['query'] = form.userInput.data
        
        try:
            rec = rdb.hget(userInput['type'], userInput['query']).decode('utf-8')
            rec = json.loads(rec)
            rec = rec[:20]
        except:
            errors.append("Unable to find subreddit or user. Try Again")
    return render_template('index.html', title = 'SubRec', errors=errors, rec=rec)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
