from twython import Twython
from time import sleep
from pymongo import MongoClient
from config import *
from tags import tags_dict

class TwitterSearchCrawler(object):
    def init(self, ip_addr, port_number, db_name, collection_name):
        '''init method args required
            ip_addr: IP Address of Mongo Server
            port_number: Port Number of Mongo Server
            db_name: Database Name of Mongo
            collection_name: Collection name of Mongo Server
        '''
        self.twitter = Twython(consumer_key, consumer_secret, access_token, access_token_secret)
        self.client = MongoClient(ip_addr,port_number)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def search(self, searchable_tags = [], langs=['en'], max_count = 10000, max_id = ""):
        '''search method to find tweets, args
            searchable_tags [optional]: list of tags to be searched (can only be from tags present n tags.py)
            langs [optional]: list of languages that can be searched
            max_count [optional]: Max count of tweets to be fetched for each tag ang language
            max_id [optional]: Max Id to be used for pagination
        '''

        if len(searchable_tags) == 0:
            searchable_tags = tags_dict.keys()
        for tag in searchable_tags:
            for lang in langs:
                
                n = 0
                max_id = ""
                break_flag = False

                print 'TwitterSearchCrawler searching for %s in %s language' % (tag,lang)
                while n < max_count and not break_flag:
                    sleep(5)
                    
                    #if max_id already present paginate from that
                    if max_id != "":
                        data = self.twitter.search(q=' OR '.join(tags_dict[tag].split(", ")),count=100,lang=lang)
                    else:
                        data = self.twitter.search(q=' OR '.join(tags_dict[tag].split(", ")),count=100,max_id=max_id,lang=lang)    
                    
                    #see if pagination available
                    if data['search_metadata'].has_key('next_results'): 
                        max_id = data['search_metadata']['next_results'].split("&q")[0].split("?max_id=")[1]

                    for result in data['statuses']:
                        #remove retweets
                        if not result.has_key('retweeted_status'):
                            #check if language is same as that was used
                            if result['lang'] == lang:
                                result['track'] = tag
                                self.collection.insert(result)
                                n += 1
                                if n % 500 == 0:
                                    print 'count =',n

                    #if no results found
                    if data['search_metadata']['count'] == 0:
                        break_flag = True
 

    

    
    
    
