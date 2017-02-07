from wtforms import Form, StringField, validators

class QueryForm(Form):
    userInput = StringField('userInput', [validators.Length(min=3, max=20)]
    inputType = RadioField('inputType', choices['Subreddit', 'User'], default='Subreddit')
'''   
def takeQuery(request):
    form = QueryForm(request.POST)
    if request.method == 'POST' and form.validate():
        rec['type'] = form.inputType.data
        rec['query'] = form.userInput.data
    return rec
'''  
