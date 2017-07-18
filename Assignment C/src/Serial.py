import os
import bz2
import json
import pandas as pd
import re
import time
import numpy as np
import sys

#read the data in into an array: tweets_data.
tweets_data = []
wdir = os.path.dirname(os.path.realpath(__file__))
os.chdir(wdir)
sys.path.append("../lib")
filedir = r'C:\Users\Fang\Desktop\BARUCH\9794\Project A\Assignment C'
for _filepath, _filename, _files in os.walk(filedir):
    for _filename in _files:
        if (_filename.endswith('.json.bz2') and
                _filename.startswith(".") is not True):
            path = os.path.join(_filepath, _filename)
            path = path.replace("\\", "/")
            print(path)
            #print(path)
            #newpath = os.path.join(_filepath, _filename[:-4])  # cut off the '.bz2' part
            #filenames = os.path.basename(path)
            #which is equivalent to,
            #filenames = os.path.split(path)[1]
            #print(filenames)
            try:
                with bz2.BZ2File(path.encode('utf-8'),'rb') as file:
                  line = file.readline()
                  tweet = json.loads(line)  # load it as Python dict
                  tweets_data.append(tweet)
                  #print(json.dumps(tweet, indent=4))  # pretty-print


            except:
                  EOFError

tweets = pd.DataFrame()
tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
tweets['lang'] = map(lambda tweet: tweet['lang'], tweets_data)
tweets['created_at'] = map(lambda tweet: tweet['created_at'], tweets_data)
tweets['created_at'] = time.strftime('%Y-%m-%d', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')) #change tweet creat time format to yymmdd

print("number of tweets: "+str(len(tweets['lang']))) 

EngTweets = tweets[tweets['lang'] == 'en']
#print(EngTweets)

#This function Lowercase the text and return frequency of a word in text
def word_in_text(word, text):
    count=0
    word = word.lower()
    text = text.lower()
    match = re.search(word, text)
    if match:
        count=count+1
        return count
    return 0


mention_freq=[] #number of times the company is mentioned in a tweet

#tokenize and lowercase the text
for index, row in EngTweets.iterrows():
   #print row['text']
   tokenized_tweet=re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",row['text'].lower()).split() #tokenize the text
   s = " ";
   #print(s.join(newtweet))
   tweets_string=s.join(tokenized_tweet)
   count=word_in_text( 'ibm',tweets_string)
   mention_freq.append(count)

tweet_time=[]  #tweet created at 
tweet_time=EngTweets['created_at']

compant_ts = pd.DataFrame()
compant_ts['created_at']=tweet_time
compant_ts['mentioned times']=mention_freq         
print(compant_ts)
print("*****")
summary = compant_ts.groupby('created_at').aggregate({'mentioned times':np.sum}) #sum of the frequency for each day
print(summary)

#read IBM stock price data
price_df= pd.DataFrame.from_csv('C:\Users\Fang\Desktop\BARUCH\9794\Project A\Assignment C\IBM.csv')
print(price_df)

#Fit linear regression model to the hostorical data
y=price_df.Close
x=summary['mentioned_times']
lm=LinearRegression()
linmodel=lm.fit(x,y)
