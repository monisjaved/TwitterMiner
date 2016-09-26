#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from pymongo import MongoClient
from string import punctuation
import copy
from datetime import datetime
from nltk.corpus import stopwords
import pysolr
import sys

try:
    address = sys.argv[1] 
except IndexError:
    address = "localhost:8983"

try:
    core_name = sys.argv[2]
except IndexError:
    core_name = "IRProject"

try:
    ip_addr = sys.argv[3]
except IndexError:
    ip_addr = "localhost"

try:
    db_name = sys.argv[4]
except IndexError:
    db_name = "InfoRet"

try:
    collection_name = sys.argv[5]
except IndexError:
    collection_name = "Project1"

#rstablish solr connection
solr = pysolr.Solr('http://%s/solr/%s'%(address,core_name), timeout=10)

client = MongoClient(ip_addr)
db = client[db_name]
collection = db[collection_name]

#regex to find all emojis and emoticons
special_symbols_pattern = re.compile(u'['
                                    u'\U0001F300-\U0001F64F'
                                    u'\U0001F680-\U0001F6FF'
                                    ']+', 
                                    re.UNICODE)

#list of all punctuations
punctuations = list(punctuation)

#list of langs allowed
langs = ['en','es','tr','ko']

#stop words dict for each language
stop_words = {'en':stopwords.words("english"), "es":stopwords.words("spanish"), 
                "tr":open("misc/turkish_stopwords.txt").read().split("\n"),
                'ko':open("misc/korean_stopwords.txt").read().split("\n")}

#list of kaomojis
kaomojis = open("misc/kaomojis.txt").read().split("\n")

count = 0
tweet_list = []
for tweets in collection.find().batch_size(100).skip(190696):
    if tweets['lang'] in langs:
        tweet = {}
        text = copy.copy(tweets['text'])

        # Separate fields that index : hashtags, mentions, URLs, emoticons+ (emoticons + emojis
        # + kaomojis) 
        tweet['hashtags'] = [k['text'] for k in tweets['entities']['hashtags']]
        for hashtag in tweet['hashtags']:
            text = text.replace("#"+hashtag,' ')

        tweet['mentions'] = [k['screen_name'] for k in tweets['entities']['user_mentions']]
        for user_mentions in tweet['mentions']:
            text = text.replace("@"+user_mentions,' ')

        url_list = [k['url'] for k in tweets['entities']['urls']]
        if 'media' in tweets:
            url_list.extend([k['url'] for k in tweets['media']['url']])
        tweet['tweet_urls'] = url_list 
        for url in tweet['tweet_urls']:
            text = text.replace(url, ' ')
        
            
        emojis = []
        emojis.extend(special_symbols_pattern.findall(text))
        text = re.sub(special_symbols_pattern,' ',text)

        for emo in kaomojis:
            emojis.extend(re.findall(re.escape(emo),text))
            text = re.sub(re.escape(emo),' ',text)

        tweet['tweet_emoticons'] = emojis

        #removing punctutations + quotes sybmbole
        for punct in punctuations+[u'\u201c',u'\u201d']:
            text = text.replace(punct, ' ')

        #removing stopwords
        # text = text.encode("utf8")
        for stopword in stop_words[tweets['lang']]:
            text = re.sub(re.escape(" " + stopword + " "), " ",text)

        #removing extra whitespaces
        text = ' '.join(text.split())


        # One copy of the tweet text that retains all content (see below) irrespective of the
        # language. This field should be set as the default field while searching.
        tweet['tweet_text'] = tweets['text']

        # Additionally index date, geolocation (if present), and any other fields you may like.
        tweet['id'] = tweets['id']

        if tweets['coordinates']:
            tweet['tweet_loc'] = ",".join([str(j) for j in tweets['coordinates']['coordinates'][::-1]])

        tweet['tweet_lang'] = tweets['lang']

        try:
            tweet['tweet_date'] = datetime.fromtimestamp(int(tweets['timestamp_ms'])/1000).strftime("%Y-%m-%dT%H:00:00Z")
        except KeyError:
            tweet['tweet_date'] = datetime.strptime(re.sub(r"\+[0-9]{4} ","",tweets['created_at']),"%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%dT%H:00:00Z")

        for lang in langs:
            if lang == tweets['lang']:
                # Language of the tweet (as identified by Twitter) and a language specific copy of the
                # tweet text that removes all stopwords (language specific), punctuation, emoticons,
                # emojis, kaomojis, hashtags, mentions, URLs and other Twitter discourse tokens. Thus,
                # you would have at least five separate fields that index the tweet text, the general field
                # above plus four for each language. For any given tweet, only two of the five fields would
                # have a value.
                
                tweet['text_'+lang] = text
            else:
                tweet['text_'+lang] = ""   

        if 'topic' in tweet and tweet['topic'] != 'NA':
            solr.add([tweet])
            print 'Added tweet with id %s to solr' % tweet['id']
            
