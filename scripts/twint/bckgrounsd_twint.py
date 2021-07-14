import re
import twint
from kafka import KafkaProducer
from json import dumps
from time import sleep
import argparse
from datetime import datetime, timedelta
import sys

def send_tweets_to_spark(df1, tcp_connection): # funcao de mandar mensagem bruta pro spark, nao sei se vai ser usada
 for ind, line in df1.iterrows():
     try:
         tweet_text = line['tweet']
         #print("Tweet Text: " + tweet_text)
         #print ("------------------------------------------")
         tcp_connection.send(tweet_text + '\n')
     except:
         e = sys.exc_info()[0]
         print("Error: %s" % e)

def produce_to_kafka(df1, producer):
    for ind, line in df1.iterrows():
        line = str(line.to_dict())  # bytes(str(line.to_dict()), 'utf-8')
        data = {'line': line}
        # print(ind)
        producer.send('client_side', value=data)
        sleep(0.1)

def twint_worker(keywords):
    # Configure
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    since = '2021-06-26 00:00:00'
    max_date = datetime.strptime(since, "%Y-%m-%d %H:%M:%S")
    #until = datetime.strptime(since, "%Y-%m-%d %H:%M:%S")
    #until = until + timedelta(0, 300)
    #until = until.strftime("%Y-%m-%d %H:%M:%S")
    #until = '2021-06-27 00:00:00'
    # run forever
    while True:
        try:
            del c
        except:
            pass
        c = twint.Config()
        c.Search = '#BBB21'#keywords
        c.Lang = "pt"
        c.Limit = 100#5000000
        c.Store_json = True
        #c.Until = max(datetime.strptime(until, "%Y-%m-%d %H:%M:%S"), datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        #print(max(datetime.strptime(until, "%Y-%m-%d %H:%M:%S"), datetime.now()).strftime("%Y-%m-%d %H:%M:%S"))
        c.Since = since
        #print(since)
        c.Pandas = True

        # Run
        twint.run.Search(c)
        df1 = twint.storage.panda.Tweets_df # df = df1[['id', 'created_at', 'date', 'tweet', 'user_id', 'user_id_str', 'username', 'nlikes', 'nretweets']]
        #print(df1)
        if not df1.empty:
            max_date = datetime.strptime(max(df1['date']), "%Y-%m-%d %H:%M:%S") + timedelta(0, 1)
            #print(max_date)
        #since = max(datetime.strptime(until, "%Y-%m-%d %H:%M:%S"), max_date).strftime("%Y-%m-%d %H:%M:%S")
        since = max_date.strftime("%Y-%m-%d %H:%M:%S")
        #until = datetime.strptime(since, "%Y-%m-%d %H:%M:%S")
        #until = until + timedelta(0, 60)
        #until = until.strftime("%Y-%m-%d %H:%M:%S")

        produce_to_kafka(df1, producer)

def parse_opt(known=False):
    parser = argparse.ArgumentParser()
    parser.add_argument('--keyword', type=str, default='', help='keyword to be listened by twint')
    opt = parser.parse_known_args()[0] if known else parser.parse_args()
    return opt

def remove_links(tweet):
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = re.sub(r'bit.ly/\S+', '', tweet)
    tweet = tweet.strip('[link]')
    return tweet
def remove_users(tweet):
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', tweet)
    return tweet
def reduce_lengthening(word):
    pattern = re.compile(r"(.)\1{2,}")
    return pattern.sub(r"\1\1", word)
def clean_tweets (tweet, bigrams=False, my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'):
    tweet = remove_users(tweet)
    tweet = remove_links(tweet)
    tweet = tweet.lower() # lower case
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
    tweet = re.sub('['+my_punctuation + ']+', ' ', tweet)
    tweet = re.sub('\s+', ' ', tweet)
    tweet = re.sub('([0-9]+)', '', tweet)
    return tweet

def trata_twitch(x):
    import os
    import pandas as p
    from datetime import datetime
    from datetime import timedelta
    import nltk
    from nltk.tokenize import RegexpTokenizer
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    from string import punctuation
    from nltk.stem import PorterStemmer
    from nltk.stem import WordNetLemmatizer
    import sys
    import autocorrect
    from autocorrect import Speller
    from collections import Counter

    lemmatizer = WordNetLemmatizer()
    my_stopwords = nltk.corpus.stopwords.words('english')
    word_rooter = nltk.stem.snowball.PorterStemmer(ignore_stopwords=False).stem


    return x

if __name__ == "__main__":
    opt = parse_opt()
    #print(opt.keyword)
    twint_worker(opt.keyword)

