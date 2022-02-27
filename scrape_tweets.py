import twint
from datetime import datetime, timedelta
import os
import time
import logging
import pandas as pd
from psycopg2.extras import Json
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from langdetect import detect


current_date = datetime.utcnow().strftime("%Y-%m-%d")


def analyze_sentiment_vader_lexicon(review):
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(review)
    # get aggregate scores and final sentiment
    agg_score = scores['compound']
    if agg_score >= 0.5:
        return 1
    elif agg_score <= -0.5:
        return -1
    else:
        return 0

##TODO change search symbols plus organsiation name LUNA : TERRA , UNI : UNISWAP
def coin_tweet(coins,before=current_date):
    """
    description: scraping tweets with coin hashtags with at least 2 retweets
    """
    dirpath = f"unlabelled_data/{before}"
    if os.path.isdir(dirpath):
        print("Directory Exist")
    else:
        os.mkdir(dirpath)
    today = datetime.strptime(before, "%Y-%m-%d")
    today_date = today.strftime("%Y-%m-%d 00:00:00")
    start = (today-timedelta(days=1))
    start_date = start.strftime("%Y-%m-%d 00:00:00")
    for coin in coins:
        print(f"Scraping Tweets from {start_date} for {coin}")
        fpath = f'unlabelled_data/{before}/twitter_{coin}.csv'
        if os.path.isfile(fpath):
            print(f"{coin} Tweets File Already Exist")
            return
        c = twint.Config()
        c.Search = f"#{coin}"
        c.Since = start_date
        c.Store_csv = True
        c.Hide_output = True
        c.Stats = True
        c.Output = fpath
        c.Lang = "en"
        c.Until = today_date
        c.Debug = True
        c.Limit = 10000
        c.Popular_tweets=True
        twint.run.Search(c)
        print(f"Finished scraping for #{coin}, sleep for 5s")
        time.sleep(5)

        if len(coins[coin]) > 0:
            for text in coins[coin]:
                print(f"Scraping Tweets from {start_date} for {text}")
                c = twint.Config()
                c.Search = f"/{text}/"
                c.Since = start_date
                c.Store_csv = True
                c.Hide_output = True
                c.Stats = True
                c.Output = fpath
                c.Lang = "en"
                c.Until = today_date
                c.Debug = True
                c.Limit = 10000
                c.Popular_tweets=True
                twint.run.Search(c)
                print(f"Finished scraping for {text}, sleep for 5s")
                time.sleep(5)


if __name__ == '__main__':
    coin = "ETH"
    other_names = ["ethereum"]
    d = {coin:other_names}
    end_date = "2022-02-27"
    coin_tweet(d,end_date)
    fpath = rf"unlabelled_data\{date}\twitter_{coin}.csv"
    df = pd.read_csv(fpath)
    df.drop_duplicates(['id'],inplace=True)
    df.drop_duplicates(["tweet"],inplace=True)
    df['language_detect'] = df['tweet'].apply(lambda x : detect(x))
    df = df.loc[df.language_detect=='en'].copy(True)
    df['sentiment'] = df.tweet.apply(lambda x : analyze_sentiment_vader_lexicon(x))
    cols_order = ['id', 'conversation_id', 'created_at', 'date', 'time', 'timezone',
       'user_id', 'username', 'name', 'place', 'language', 'mentions',
       'urls', 'photos', 'replies_count', 'retweets_count', 'likes_count',
       'hashtags', 'cashtags', 'link', 'retweet', 'quote_url', 'video',
       'thumbnail', 'near', 'geo', 'source', 'user_rt_id', 'user_rt',
       'retweet_id', 'reply_to', 'retweet_date', 'translate', 'trans_src',
       'trans_dest', 'tweet','sentiment']
    df[cols_order].to_csv(fpath)
