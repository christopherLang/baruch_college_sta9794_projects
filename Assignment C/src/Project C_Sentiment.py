# -*- coding: utf-8 -*-
"""
Created on Fri May  5 11:42:57 2017

@author: ciao
"""

import os
import bz2
import sys
#import pandas as pd
import datetime as dt
import re
import json
from textblob import TextBlob as tb


# ============================================== Simple Sentiment Analysis Using TextBlob ============================================
# =====================================================================================================================================

class TwitterAnalysis():
    
    ''' 
    ============================================== Ignore This Part ============================================
    
    def read_bz2file(self,filedir):
        ''' to read the zipped bz2 files as dict'''
        try:
            with bz2.BZ2File(filedir, "rb") as file:
                data = file.readline().decode('utf-8').strip()
                data = json.loads(data)        
        
        except Exception as e:
            data = None
        
        finally:
            file.close()
        
        return data
    
    
    def clean_tweet(self, tweet):
        
        #Utility function to clean tweet text by removing links, special characters
        #using simple regex statements.
        
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
    ===============================================================================================================================
    '''
    
         
    def get_tweets(self,filedir):
        '''Main function to fetch tweets.'''
        # empty list to store parsed tweets
        tweets = []
 
        try:
            for tweet in self.read_bz2file(filedir).items(): #self.data???
                # empty dictionary to store tweets params we need
                parsed_tweet = {}
 
                if tweet[0] == 'text':
                    parsed_tweet['text'] = tweet[1] 
                parsed_tweet['sentiment'] = self.get_sentiment(tweet[1])  # saving sentiment of tweet
 
                if tweet[0] == 'retweet_count':
                    if tweet.retweet_count > 0:
                        # if tweet has retweets, it will be appended only once
                        if parsed_tweet not in tweets:
                            tweets.append(parsed_tweet)
                    else:
                        tweets.append(parsed_tweet)
                        
            return tweets

        except:
            # Print error (if any)
            print("Error : " + str(sys.exc_info()[0]))
      
    
    def field_filter(self,tweet):
        '''First to clean tweet text by removing links, special characters using simple regex statements.'''
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
        
        fields_keep = [
            "text",
            "id_str",
            "favorite_count",
            "retweet_count",
            "entities",
            "created_at",
            "user"
        ]
     
        if tweet['lang'] != "en":
            result = None
    
        else:
            result = dict([(k, v) for k, v in tweet.items() if k in fields_keep])
    
            try:
                hashtags = result['entities']['hashtags']
                hashtags = [i['text'] for i in hashtags]
                result['hashtags'] = hashtags
    
            except KeyError:
                result['hashtags'] = None
    
            user_fields_keep = [
                "favourites_count",
                "follow_request_sent",
                "followers_count",
                "friends_count",
                "verified",
                "id_str"
            ]
    
            user = dict([(k, v) for k, v in result['user'].items()
                         if k in user_fields_keep])
    
            result['user'] = user
    
            result['created_at'] = dt.datetime.strptime(result['created_at'],
                                                        "%a %b %d %H:%M:%S %z %Y")
    
        return result
    
      
    def task(self,filedir):
        t = self.read_bz2file(filedir)
        t = self.field_filter(t)
        t = self.get_tweets(t)
    
        return t
    
    
    def get_sentiment(self, filedir): #tweet???
        '''Classify sentiment using textblob'''    
        # Create TextBlob object
        analysis = tb(self.task(filedir)) #self.field_filter(tweet)???
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'Positive'
        elif analysis.sentiment.polarity == 0: 
            return 'Neutral'
        else:
            return 'Negative'

def main():
    t = TwitterAnalysis()
    tweets = t.get_tweets()

    # Positive tweets 
    postweets = [tweet for tweet in tweets if tweet['sentiment'] == 'Positive']
    # Percentage of positive tweets
    print("Positive tweets percentage: {} %".format(100*len(postweets)/len(tweets)))
    # Negative tweets from tweets
    negtweets = [tweet for tweet in tweets if tweet['sentiment'] == 'Negative']
    # percentage of negative tweets
    print("Negative tweets percentage: {} %".format(100*len(negtweets)/len(tweets)))
    # percentage of neutral tweets
    print("Neutral tweets percentage: {} % \
        ".format(100*len(tweets - negtweets - postweets)/len(tweets)))      

if __name__ == "__main__":
    main()


