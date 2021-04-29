#Import libraries and packages
from flask import Flask,render_template,url_for,request
import pandas as pd 
import pickle
import pandas as pd 
import numpy as np 
import re
import string 
import nltk
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from nltk.stem.porter import *
#data cleaning function to remove pattern
def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)
    return input_txt
#function to generate a column about the punctuation of a tweet 
def count_punc(text):
    count = sum([1 for char in text if char in string.punctuation])
    return round((count/len(text) - text.count(" ")), 3)*100
#initiate the flask application 
app = Flask(__name__, template_folder='templates')

@app.route('/')
def home():
	return render_template('home.html')

@app.route('/predict',methods=['POST'])
def predict():
    model = pickle.load(open("classifier.pkl", "rb"))
    cv = pickle.load(open("vectorizer.pkl", "rb"))
    if request.method == 'POST':
        message = request.form['message']
        data = [message]
        vect = pd.DataFrame(cv.transform(data).toarray())
        body_len = pd.DataFrame([len(data) - data.count(" ")])
        punc = pd.DataFrame([count_punc(data)])
        total_data = pd.concat([body_len, punc,vect], axis =1)
        my_prediction = model.predict(total_data)
    return render_template('result.html',prediction = my_prediction)

if __name__ == '__main__':
	app.run(debug=True)
