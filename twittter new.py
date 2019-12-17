# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:45:11 2019

@author: Ritvik.Tiwari
"""

import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import schedule
import time
import os
import re
import subprocess
import time 
from textblob import TextBlob
import pyodbc
#import pymssql


consumer_key ="Beyvz2jNGx7iY0QfRaR9I7mMH"
consumer_secret ="7PbZJRkfe8lxhQuv58NTJPxjgAH3E3lFWWrpzVFBpzmiInpFgL"
access_token ="1002133872698572800-DEBzVyaOTks3DUfWwpRe0xR5Cet98C"
access_token_secret ="w209iD0bdGepyrHF4YY0M6M7LSGv5vCG1Sbqum6SWuFGY"

def connect(username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, location, sentiment):
    
    #Connecting to database
    try:
        server = 'GTIGGNLAP325'
        database = 'ptest'
        user = 'SqlAdmin'
        password = 'root@123'
        con = pyodbc.connect('DRIVER= {SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
        print("Connection successful")
        #con = pymssql.connect(server, username, password, database)
        #con = pyodbc.connect(r'DSN=test;UID=user;PWD=pwd')
        
        #if con.is_connected():
            
            #insert Data            
        cursor = con.cursor()
        cursor1 = con.cursor()
            # Create Table
        #cursor.execute('SELECT * FROM ptest.tweet')
        
        #query = "INSERT INTO tweet (username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, quote_count, reply_count, location, truncated, sentiment) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s ,%s ,%s ,%s)"
        #cursor.execute(query, (username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, quote_count, reply_count, location, truncated, sentiment))
        cursor.execute('''INSERT INTO ptest.dbo.tweet (username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, location, sentiment) VALUES (?, ?, ?, ?, ?, ?, ? ,? ,? ,? ,? ,?)''', (username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, location, sentiment))
        cursor1.execute ("Select * from ptest.dbo.tweet")
        results = cursor1.fetchall()
        print(results)
        for row in results:
            print (row[0])
        print("Number of rows updated: %d" % cursor.rowcount)
        
        #cursor.execute(query, (username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, quote_count, reply_count, location, truncated, sentiment))
        con.commit()
            
            
    except Error as e:
       print(e)

    cursor.close()
    cursor1.close()
    con.close()

    return


#Accessing Twitter API
class Streamlistener(tweepy.StreamListener):
    
   # tagsGT = ['GrantThornton','GrantThorntonIndia','lifeatGT','good2great','vibrantbharat','GTTaxUpdates'] 
    def on_connect(self):
        print("You are connected to the Twitter API")

    def on_status(self, status):
        print("Status is:",status.full_text)
        
    def on_error(self):
        if status_code != 200:
            print("error found")
            #Returning false disconnects the stream
            return False

#Extracting the desired attributes
            
    def on_data(self,data):
        
        try:
            raw_data = json.loads(data)
            cleanTweet = ''
            print(raw_data)

            if 'text' in raw_data:
                 
                username = raw_data['user']['screen_name']
                name = raw_data['user']['name']
                id = raw_data['id']
                source = raw_data['source']
                created_at = parser.parse(raw_data['created_at'])
                tempTweet = raw_data['text']               
                #hashtags = re.findall(r"^#""\w",tweet)
                hash = []
                
                
                #hashtags = [0]['text'])
                
                if tempTweet[0] == 'R' and tempTweet[1] == 'T':
                    if raw_data['is_quote_status'] == True and raw_data['retweeted_status']['is_quote_status'] == True:
                        
                        retweet_count = raw_data['quoted_status']['retweet_count'] + raw_data['retweeted_status']['quoted_status']['retweet_count'] + raw_data['retweeted_status']['retweet_count']
                        favorite_count = raw_data['quoted_status']['favorite_count'] + raw_data['retweeted_status']['quoted_status']['favorite_count'] + raw_data['retweeted_status']['favorite_count']
                        #quote_count = raw_data['quoted_status']['quote_count'] + raw_data['retweeted_status']['quoted_status']['quote_count'] + raw_data['retweeted_status']['quote_count']
                        #reply_count = raw_data['quoted_status']['reply_count'] + raw_data['retweeted_status']['quoted_status']['reply_count'] + raw_data['retweeted_status']['reply_count']
                        if raw_data['retweeted_status']['truncated'] == False:
                            tweet = raw_data['retweeted_status']['text']
                            hash = raw_data['retweeted_status']['entities']['hashtags']
                        else:
                            tweet = raw_data['retweeted_status']['extended_tweet']['full_text']
                            hash = raw_data['retweeted_status']['extended_tweet']['entities']['hashtags']
                        
                    elif raw_data['retweeted_status']['is_quote_status'] == False and raw_data['is_quote_status'] == True:
                        
                        retweet_count = raw_data['quoted_status']['retweet_count'] + raw_data['retweeted_status']['retweet_count']
                        favorite_count = raw_data['quoted_status']['favorite_count'] + raw_data['retweeted_status']['favorite_count']
                        #quote_count = raw_data['quoted_status']['quote_count'] + raw_data['retweeted_status']['quote_count']
                        #reply_count = raw_data['quoted_status']['reply_count'] + raw_data['retweeted_status']['quote_count']
                        
                        if raw_data['retweeted_status']['truncated'] == False:
                            tweet = raw_data['retweeted_status']['text']
                            hash = raw_data['retweeted_status']['entities']['hashtags']
                        else:
                            tweet = raw_data['retweeted_status']['extended_tweet']['full_text']
                            hash = raw_data['retweeted_status']['extended_tweet']['entities']['hashtags']
                        
                    elif raw_data['retweeted_status']['is_quote_status'] == True and raw_data['is_quote_status'] == False:
                        
                        retweet_count = raw_data['retweeted_status']['quoted_status']['retweet_count'] + raw_data['retweeted_status']['retweet_count']
                        favorite_count = raw_data['retweeted_status']['quoted_status']['favorite_count'] + raw_data['retweeted_status']['favorite_count']
                        #quote_count = raw_data['retweeted_status']['quoted_status']['quote_count'] + raw_data['retweeted_status']['quote_count']
                        #reply_count = raw_data['retweeted_status']['quoted_status']['reply_count'] + raw_data['retweeted_status']['reply_count']
                        
                        if raw_data['retweeted_status']['truncated'] == False:
                            tweet = raw_data['retweeted_status']['text']
                            hash = raw_data['retweeted_status']['entities']['hashtags']
                        else:
                            tweet = raw_data['retweeted_status']['extended_tweet']['full_text']
                            hash = raw_data['retweeted_status']['extended_tweet']['entities']['hashtags']
                        
                    else:
                        
                        retweet_count = raw_data['retweeted_status']['retweet_count']
                        favorite_count = raw_data['retweeted_status']['favorite_count']
                        #quote_count = raw_data['retweeted_status']['quote_count']
                        #reply_count = raw_data['retweeted_status']['reply_count']
                        
                        if raw_data['retweeted_status']['truncated'] == False:
                            tweet = raw_data['retweeted_status']['text']
                            hash = raw_data['retweeted_status']['entities']['hashtags']
                        else:
                            tweet = raw_data['retweeted_status']['extended_tweet']['full_text']
                            hash = raw_data['retweeted_status']['extended_tweet']['entities']['hashtags']
                        
                elif raw_data['is_quote_status'] == True:
                    
                    retweet_count = raw_data['quoted_status']['retweet_count']
                    favorite_count = raw_data['quoted_status']['favorite_count']
                    #quote_count = raw_data['quoted_status']['quote_count']
                    #reply_count = raw_data['quoted_status']['reply_count']
                    if raw_data['truncated'] == False:
                            tweet = raw_data['text']
                            hash = raw_data['entities']['hashtags']
                    else:
                            tweet = raw_data['extended_tweet']['full_text']
                            hash = raw_data['extended_tweet']['entities']['hashtags']
                else:
                    
                    retweet_count = raw_data['retweet_count']
                    favorite_count = raw_data['favorite_count']
                    #quote_count = raw_data['quote_count']
                    #reply_count = raw_data['reply_count']
                    
                    if raw_data['truncated'] == False:
                            tweet = raw_data['text']
                            hash = raw_data['entities']['hashtags']
                    else:
                            tweet = raw_data['extended_tweet']['full_text']
                            hash = raw_data['extended_tweet']['entities']['hashtags']
                    
                temp = ''
                for value in hash:
                    temp = temp +',' + value['text']
                    #temp = re.sub(r",","",temp)
                
                hashtag = temp
                #print (hashtag)
                
                followers_count = raw_data['user']['followers_count']
                """
                if raw_data['place'] is not None:
                    place = raw_data['place']['country']
                    #print(place)
                else:
                    place = None
                """
                
                location = raw_data['user']['location']
                #geo = raw_data['geo']
                #coordinates = raw_data['place']
                #place = raw_data['place']
                #truncated = raw_data['truncated']
                #tweet = re.sub(r"http\S+", "", tweet)
                #tweet = re.sub(r"#\S+", "", tweet)
                #tweet = re.sub(r"@\S+", "", tweet)
                cleanTweet = ' '.join(re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', ' ', tweet).split())
                analysis = TextBlob(cleanTweet)
                if analysis.sentiment.polarity > 0:
                     sentiment = 'Positive'
                elif analysis.sentiment.polarity == 0:
                    sentiment = 'Neutral'
                else:
                    sentiment = 'Negative'

                #Inserting MySQL database
                connect(username, name, id, source, created_at, tweet, hashtag, favorite_count, retweet_count, followers_count, location, sentiment)
                print("Tweet colleted at: {} ".format(str(created_at)))
        except Error as e:
            print(e)
        
        except Exception as e:
            print("Error is",e)

                

#if __name__== '__main__':
def mainFunction():
        
    
    #Twitter Authentification
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    #Streaming
    listener = Streamlistener(api = api)
    stream = tweepy.Stream(auth, listener = listener, tweet_mode='extended')

    
        #Filtering the stream
    """track = ['GrantThornton', 'GTIndia', 'GTI', 'grantthornton', 'GrantThorntonIN', 'GTILLP']"""
    #track = ['@AyushmanBharat','@BrandIndia','@TrustInBuisness','@IndiaMeetBritain','@Skilling','@Textile','@ERP','@Consulting','@CIISocialImpactReport2019','@ICYMI','@RSLDCproject','@SmartCities','@smartcity','@GTIndia','@GrantThorntonIN','@GTILLP','@GrantThornton' ]
    track = ['#GTIndia','#GrantThorntonIN','#GTILLP','#GrantThornton','#LifeatGT','#VibrantBharat','#GTTaxUpdates','#good2Great']
    stream.filter(track = track)
        
if __name__ == '__main__':
            mainFunction()
            error = True
            while error:
                    try:
                        mainFunction()
                        error=False
                    except IOError:
                        error=True

    

    
    #def update():
        #temp = username;
        #cursor=con.cursor
        #query = "UPDATE twitter.tweet set favorite_count where username = temp"
        #cursor.execute(query,(favorite_count))
        #print(favorite_count)        
    
#schedule.every(1).minutes.do(update) 
#while True: 
