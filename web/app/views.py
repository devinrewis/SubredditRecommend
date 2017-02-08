from app import app
from flask import render_template, request
from flask_wtf import FlaskForm
from app.query import *
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

app.secret_key = 'development'

@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    errors = []
    rec = dict() #dict to store recommendations
    userInput = dict()
    form = QueryForm()

    rdb = redis.StrictRedis(host=settings['redis-host'], port=settings['redis-port'], db=0)
    
    if request.method == 'POST':
        userInput['type'] = form.inputType.data
        userInput['query'] = form.userInput.data
        
        try:
            rec = rdb.hget(userInput['type'], userInput['query']).decode('utf-8')
            rec = json.loads(rec)
            rec = rec[:20]
        except:
            errors.append("Unable to find subreddit or user. Try Again")
    return render_template('index.html', title = 'SubRec', errors=errors, rec=rec, form=form)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
