import tweepy
import json
from pymongo import MongoClient
from bson import json_util
from tweepy.utils import import_simplejson

json = import_simplejson()
mongocon = MongoClient()

db = mongocon.tstream
col = db.tweets_tail

consumer_key = '6Jq90RtRfmxTK40TftqZlKuDg'
consumer_secret = 'NjsUmyITJsQNzbCQ09ccnKQIjAvo5cD0sVGujDokuwoPrd5sAz'

access_token_key = '710573731689603072-t9a2eOhPkqxLZQZQiEccp2MAVuphQQp'
access_token_secret = 'wzk08UtwRMgUNJUTd1nBIkE9Y49aPeiKdf46j5uI537wp'

auth1 = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth1.set_access_token(access_token_key, access_token_secret)

class StreamListener(tweepy.StreamListener):
    mongocon = MongoClient()
    db = mongocon.tstream
    col = db.tweets
    json = import_simplejson()


    def on_status(self, tweet):
        print 'Ran on_status'

    def on_error(self, status_code):
        return False

    def on_data(self, data):
        if data[0].isdigit():
            pass
        else:
            col.insert(json.loads(data))
            print(json.loads(data))


l = StreamListener()
streamer = tweepy.Stream(auth=auth1, listener=l)
setTerms = ['bigdata', 'hadoop', 'twitter']
streamer.filter(track = setTerms)
