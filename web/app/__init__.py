from flask import Flask
from flask_wtf import CSRFProtect
app = Flask(__name__)
csrf = CSRFProtect(app)
from app import views
