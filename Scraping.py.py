#import libraries
import requests
import json
import pandas as pd
#headers function
def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers
#function to set the rules to filter the live stream of tweets
def set_rules(headers, sample_rules):
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
#define get_stream function
def get_stream(headers, max_number_tweets):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    counter_tweets = 0
    stream_tweets = {'stream_tweets':[]}
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            stream_tweets["stream_tweets"].append(json_response)
            print(json.dumps(json_response, indent=4, sort_keys=True))
            counter_tweets += 1
            if counter_tweets >= max_number_tweets:
                with open("stream_tweets.json", 'w') as f:
                    json.dump(stream_tweets, f, indent=4)
                # Close the stream of tweets
                response.close()
                # Break the loop
                break
# Let's call all the functions
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAA%2FwOQEAAAAAgP1qA27vDaOuOB%2Fwl%2FC7yJqi5lU%3Da5aW8ClszLCM2MDdAsdLGbiCGYBs78yyXzlb34w062lf8jpAlz'
# Creat the headers for the HTTP request
headers = create_headers(bearer_token)

# Set the rules to scraoe and filter 50 tweets that mention vaccines/vaccination/transperancy/efficiacy
# We are setting to get only the tweets in English and exclduing retweets
sample_rules = [
    {"value": " (vaccine OR vaccination OR vaccinated ) lang:en -is:retweet"},
    {"value": " (transperancy OR efficacy) lang:en -is:retweet"},
]

set_rules = set_rules(headers, sample_rules)
# Get the stream of tweets live
max_number_tweets = 50
get_stream(headers, max_number_tweets)
# load data using Python JSON module
with open('/project/stream_tweets.json','r') as f:
    data = json.loads(f.read())
# Flatten data because nested list is put up into a single stream_tweets.
df = pd.json_normalize(data, record_path =['stream_tweets'])
df.drop('matching_rules', axis =1,  inplace =True)
df = df.rename(columns={"data.id": "id", "data.text": "text"})

# Set the rules to scraPe and filter 50 tweets about the EU Medicines Agency and WHO
query = ("EU Medicines Agency", "World Health Organization (WHO)")
# Prepare the headers to pass the authentication to Twitter's api
headers = {
    'Authorization': 'Bearer {}'.format(bearer_token),
}
params = (
    ('query', query),
    ('max_results', str(int(max_results))), # Let's make sure that the number is an string
)

# Does the request to get the most recent tweets
response = requests.get('https://api.twitter.com/2/tweets/search/recent', headers=headers, params=params)

# Validates that the query was successful
if response.status_code == 200:
    print("URL of query:", response.url)
    
    # Let's convert the query result to a dictionary that we can iterate over
    tweets =  json.loads(response.text) 
    print("Total number of most recent tweets: ", len(tweets['data']))
          
    for tweet in tweets['data']:
        print("tweet_id: ", tweet['id'], "tweet_text: ", tweet['text'])
# load data using Python JSON module
with open('/project/twitterQuery.json','r') as f:
    data_1 = json.loads(f.read())
# Flatten data
df_1 = pd.json_normalize((data_1), record_path =['data'])
df_1
#Append the two dataframes
final_result = df.append([df_1])
#inspect the final dataframe
final_result.head(25)