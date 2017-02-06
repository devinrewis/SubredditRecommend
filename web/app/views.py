from app import app
from flask import render_template, request
import json
import requests
import redis

@app.route('/', methods=['GET', 'POST'])
@app.route('/index')
def index():
    errors = []
    rec = dict()

    rdb = redis.StrictRedis(host='##REDIS HOST##', port=6379, db=0)
    if request.method == "POST":
        try:
            subreddit = request.form['subreddit-input']
            rec = rdb.get(subreddit).decode('utf-8')
            rec = json.loads(rec)
            rec = rec[:50]
        except:
            errors.append("Unable to find subreddit. Try Again")
    return render_template('index.html', title = 'SubRec', errors=errors, rec=rec)
