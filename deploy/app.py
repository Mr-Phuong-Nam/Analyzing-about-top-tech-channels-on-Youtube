from joblib import dump, load
import json

import pandas as pd

from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.ensemble import RandomForestRegressor

from flask import Flask, render_template, request

app = Flask(__name__)
model = load('../models/model.joblib')
mlb = load('./materials/mlb.joblib')
with open('./materials/tags.json', 'r') as f:
    tags = json.load(f)

def make_data_predict(duration, tags):
  new_data = pd.DataFrame({ 'duration': [duration], 'tags': [tags]})
  return new_data.join(pd.DataFrame(mlb.transform(new_data.pop('tags')), columns = mlb.classes_, index = new_data.index))

@app.route('/')
def home():
	return render_template('index.html', tags = tags)

@app.route('/result', methods=['POST'])
def result():
  duration = request.form['duration']
  st_tag = request.form['st_tag']
  nd_tag = request.form['nd_tag']
  rd_tag = request.form['rd_tag']
  tags = list(set([st_tag, nd_tag, rd_tag]))
  tags = [tag for tag in tags if tag != 'None']
  data = make_data_predict(duration, tags)
  prediction = model.predict(data)
  return render_template('result.html', prediction = prediction)