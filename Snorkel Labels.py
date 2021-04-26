#Import libraries
from snorkel.labeling import LabelingFunction
import re
from snorkel.preprocess import preprocessor
from textblob import TextBlob
from snorkel.labeling import PandasLFApplier
from snorkel.labeling.model import LabelModel
from snorkel.labeling import LFAnalysis
from snorkel.labeling import filter_unlabeled_dataframe
#define constants to represent the class labels :official, not_official
ABSTAIN = -1
OFFICIAL = 0
NOT_OFFICIAL = 1
#define function which looks into the input words 
def keyword_lookup(x, keywords, label):
    if any(word in x.text.lower() for word in keywords):
        return label
    return ABSTAIN
#define function which assigns a correct label
def make_keyword_lf(keywords, label=OFFICIAL):
    return LabelingFunction(
        name=f"keyword_{keywords[0]}",
        f=keyword_lookup,
        resources=dict(keywords=keywords, label=label),
    )
#determine the Snorkel wording requirements 
""" Official tweets contain word 'cases' """
keyword_cases = make_keyword_lf(keywords=["cases"])

"""Official tweets contain word vaccines or vaccinated or vaccination"""
keyword_vaccine = make_keyword_lf(keywords=["vaccines", "vaccinated", "vaccination"])

"""Official tweets contain word 'pandemic' """
keyword_pandemic = make_keyword_lf(keywords=["pandemic"])

"""Official tweets contain word 'covid' """
keyword_covid = make_keyword_lf(keywords=["covid"])

"""Official tweets contain word 'death' """
keyword_death = make_keyword_lf(keywords=["death"])

"""Official tweets contain word 'government' """
keyword_gov = make_keyword_lf(keywords=["government"])

"""Official tweets contain word 'infection' """
keyword_infection = make_keyword_lf(keywords=["infection"])

"""Official tweets contain word 'jab' """
keyword_jab = make_keyword_lf(keywords=["jab"])

"""Official tweets contain word 'quarantine' """
keyword_quarntine = make_keyword_lf(keywords=["quarantine"])

"""Official tweets contain word 'side effects' """
keyword_effect = make_keyword_lf(keywords=["side effects"])

"""Official tweets contain word 'scientists' """
keyword_scientists = make_keyword_lf(keywords=["scientists"])

"""Official tweets contain word 'medical' """
keyword_medical = make_keyword_lf(keywords=["medical"])

"""Official tweets contain word 'efficacy' """
keyword_efficacy = make_keyword_lf(keywords=["efficacy"])

"""Unofficial tweets usually contain word subscribe"""
keyword_subscribe = make_keyword_lf(keywords=["subscribe"], label=NOT_OFFICIAL)

"""Unofficial tweets usually contain word http which links to another advertisment"""
keyword_http = make_keyword_lf(keywords=["http"], label=NOT_OFFICIAL)

#define a function which differentiates number of tweets based on length
@labeling_function()
def long_tweet(x):
    """Official tweets are often long, such as"""
    return OFFICIAL if len(x.text.split()) > 6 else ABSTAIN

#use a regular expression to determine tweets that make reference to a number of cases
@labeling_function()
def regex_cases_rise(x):
    return OFFICIAL if re.search(r"cases.*rise.*[0-9]+", x.text, flags=re.I) else ABSTAIN

@labeling_function()
def regex_cases_now(x):
    return OFFICIAL if re.search(r"cases.*now", x.text, flags=re.I) else ABSTAIN

#set up a preprocessor function to determine polarity & subjectivity 
@preprocessor(memoize=True)
def textblob_sentiment(x):
    scores = TextBlob(x.text)
    x.polarity = scores.sentiment.polarity
    x.subjectivity = scores.sentiment.subjectivity
    return x
#find polarity
@labeling_function(pre=[textblob_sentiment])
def textblob_polarity(x):
    return OFFICIAL if x.polarity > 0.9 else ABSTAIN
#find subjectivity 
@labeling_function(pre=[textblob_sentiment])
def textblob_subjectivity(x):
    return OFFICIAL if x.subjectivity >= 0.5 else ABSTAIN

#combine all the labeling functions 
lfs = [keyword_cases, keyword_vaccine, keyword_pandemic, keyword_covid, keyword_death, keyword_gov,
       keyword_infection, keyword_medical, keyword_scientists, keyword_efficacy, keyword_jab,
       keyword_effect, keyword_quarntine,
       keyword_subscribe, keyword_http, long_tweet, 
       regex_cases_rise, regex_cases_now, textblob_polarity, textblob_subjectivity]

#apply the lfs on the dataframe
applier = PandasLFApplier(lfs=lfs)
L_snorkel = applier.apply(df=final_result)

#apply the label model
label_model = LabelModel(cardinality=2, verbose=True)
#fit on the data
label_model.fit(L_snorkel, n_epochs=500, log_freq=100, seed=123)
#predict and create the labels
final_result["label"] = label_model.predict(L=L_snorkel, tie_break_policy="Abstain")

#determine the labeling analysis
LFAnalysis(L_snorkel, lfs).lf_summary()

#determine the probabilities
probs_train = label_model.predict_proba(L=L_snorkel)
#find the final dataframe with labels (0,1)
df_filtered, probs_filtered = filter_unlabeled_dataframe(X= final_result, y=probs_train,
L=L_snorkel)

#inspect the final dataframe 
df_filtered

#get the label counts 
df_filtered['label'].value_counts()

#create a csv file to be used in the further analysis
df_filtered.to_csv('Tweets.csv')





