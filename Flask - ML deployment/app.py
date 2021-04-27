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
    count = sum([i for char in text if char in string.punctuation])
    return round((count/len(text) - text.count(" ")), 3)*100
#initiate the flask application 
app = Flask(__name__)

@app.route('/')
def home():
	return render_template('home.html')

@app.route('/predict',methods=['POST'])
def predict():
    data = pd.read_csv('Tweets.csv', encoding="ANSI")
    data.drop(['Unnamed: 0'], axis=1, inplace=True)
    data['text'] = np.vectorize(remove_pattern)(data['text'], "@[/w]*")
    data['text'] = np.vectorize(remove_pattern)(data['text'], r"http\S+")
    data['text'] = data['text'].str.replace("[^a-zA-Z#]", " ")
    tokenized_tweet = data['text'].apply(lambda x: x.split())
    stemmer = PorterStemmer()
    tokenized_tweet = tokenized_tweet.apply(lambda x: [stemmer.stem(i) for i in x])
    for i in range(len(tokenized_tweet)):
        tokenized_tweet[i] =  ' '.join(tokenized_tweet[i])
    data['text'] = tokenized_tweet
    data['body_len'] = data['text'].apply(lambda x: len(x) - x.count(" "))
    data['punc%'] = data['text'].apply(lambda x: count_punc(x))
    cv = CountVectorizer(stop_words = 'english')
    cvdf = cv.fit_transform(data['text'])
    X_cv_feat = pd.concat([data['body_len'], data['punc%'], pd.DataFrame(cvdf.toarray())], axis =1)
    X= X_tfidf_feat
    y = data['label']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
    clf_1= LogisticRegression()
    clf_1.fit(X_train,y_train)
    clf_1.score(X_test,y_test)
    if request.method == 'POST':
        message = request.form['message']
        data = [message]
        vect = pd.DataFrame(cv.transform(data).toarray())
        body_len = pd.DataFrame(len(data) - data.count(" "))
        punc = ([count_punc(data)])
        total_data = pd.concat([body_len, punc,vect], axis =1)
        my_prediction = clf_1.predict(total_data)
    return render_template('result.html',prediction = my_prediction)

if __name__ == '__main__':
	app.run(debug=True)
