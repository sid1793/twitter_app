import tweepy
import json
from pymongo import MongoClient
from bson import json_util
from tweepy.utils import import_simplejson
from boto.sqs.message import Message
import boto.sqs
import uuid
import os

json = import_simplejson()


#Twitter auth
TWITTER_CONSUMER_KEY = os.environ.get('TWITTER_CONSUMER_KEY')
TWITTER_CONSUMER_SECRET = os.environ.get('TWITTER_CONSUMER_SECRET') 
TWITTER_ACCESS_TOKEN_KEY = os.environ.get('TWITTER_ACCESS_TOKEN_KEY')
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')

auth1 = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
auth1.set_access_token(TWITTER_ACCESS_TOKEN_KEY, TWITTER_ACCESS_TOKEN_SECRET)


#Mongo Connection
mongocon = MongoClient('mongodb://localhost:27017/')
db = mongocon.tstream
col = db.tweets
print "Number of tweets in the db: ", col.count()
    
k = 0


#AWS auth
REGION = 'us-east-1'
conn = boto.sqs.connect_to_region(REGION)
tweets_queue = conn.get_queue('tweets_queue')



class StreamListener(tweepy.StreamListener):
    # mongocon = MongoClient('mongodb://localhost:27017/')
    # db = mongocon.tstream
    # col = db.tweets
    json = import_simplejson()

    
    def on_status(self, tweet):
        print 'Ran on_status'

    def on_error(self, status_code):
        return False

    def on_data(self, data):
        global k
        if data[0].isdigit():
            pass
        else:
            k += 1
            d = json.loads(data)
        if d.get("geo") and d.get("text"):
            tweet_id = uuid.uuid1().hex
            d['tweet_id'] = tweet_id
            col.insert(d)
            print("Inserted new tweet in database")

            m = Message()
            body = {'tweet_id': tweet_id  ,'text': d['text']}
            m.set_body(json.dumps(body))
            tweets_queue.write(m)
            print("Inserted new tweet in Queue")

l = StreamListener()
streamer = tweepy.Stream(auth=auth1, listener=l)
streamer.filter(locations=[-180,-90,180,90])

