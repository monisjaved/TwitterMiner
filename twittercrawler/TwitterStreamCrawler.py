from twython import TwythonStreamer
from pymongo import MongoClient
import requests
from config import *
from tags import tags_dict


class TwitterStreamCrawler(TwythonStreamer):
    def init(self, ip_addr, port_number, db_name="", collection_name=""):
        '''add databse method args required
            ip_addr: IP Address of Mongo Server
            port_number: Port Number of Mongo Server
            db_name: Database Name of Mongo
            collection_name: Collection name of Mongo Server
        '''
        self.client = MongoClient(ip_addr,port_number)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.streamer = super.init(consumer_key, consumer_secret, access_token, access_token_secret)


    def on_success(self, data):
        '''overload parents on_success call to assign tag 
            and insert data in mongo
        '''
        if 'text' in data and 'lang' in data and data['lang'] in self.langs:

            hashtags = [k['text'] for k in data['entities']['hashtags']]
            topic_found_flag = False

            for hashtag in hashtags + data['text'].split(" "):
                hashtag = hashtag.lower()
                for tag in searchable_tags:
                    if hashtag in tags_dict[tag].split(", "):
                        data['topic'] = tag
                        topic_found_flag = True
                        break

                if topic_found_flag:
                    break


            if 'topic' in data:
                self.collection.insert(data)
                self.n += 1
                if self.n % 1000 == 0:
                    print 'count = ',n
                
    def on_error(self, status_code, data):
        '''
            overload parents on_error call to print error code
        '''
        print 'Stream Error with code',status_code
        # self.disconnect()

    def stream(self, searchable_tags = [], langs = ['en'], max_count = -1):
        '''stream method to actually find tweets from stream, args:
            searchable_tags [optional]: list of tags to be searched (can only be from tags present n tags.py)
            langs [optional]: list of languages that can be searched
            max_count [optional]: Max count of tweets to be fetched for each tag ang language
        '''
        self.n = 0
        self.max_count = max_count
        self.searchable_tags = searchable_tags
        while self.n < self.max_count:
            try:
                self.streamer.statuses.filter(track=searchable_tags)
            except requests.exceptions.ChunkedEncodingError:
                continue

    