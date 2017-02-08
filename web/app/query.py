from flask_wtf import FlaskForm
from wtforms import StringField, RadioField, validators

class QueryForm(FlaskForm):
    userInput = StringField('userInput') #, [validators.Length(min=3, max=20)])
    inputType = RadioField('inputType', choices=[('subreddit', 'Subreddit'), ('author', 'User')], default='subreddit')






