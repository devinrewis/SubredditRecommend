from flask_wtf import FlaskForm
from wtforms import StringField, RadioField, validators

class QueryForm(FlaskForm):
    userInput = StringField('userInput') #, [validators.Length(min=3, max=20)])
    inputType = RadioField('inputType', choices=[('subreddit', 'Subreddit'), ('author', 'User')], default='subreddit')
'''   
def takeQuery(request):
    form = QueryForm(request.POST)
    if request.method == 'POST' and form.validate():
        rec['type'] = form.inputType.data
        rec['query'] = form.userInput.data
    return rec
'''  
