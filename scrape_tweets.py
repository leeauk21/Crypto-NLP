import twint
from datetime import datetime, timedelta
import os
import time
import logging
import pandas as pd
from psycopg2.extras import Json


current_date = datetime.utcnow().strftime("%Y-%m-%d")


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
    coins = coins
    for i in coins:
        print(f"Scraping Tweets from {start_date} for {i}")
        fpath = f'unlabelled_data/{before}/twitter_{i}.csv'
        if os.path.isfile(fpath):
            print(f"{i} Tweets File Already Exist")
            continue
        c = twint.Config()
        c.Search = f"#{i}"
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
        print(f"Finished scraping {i}, sleep for 5s")
        time.sleep(5)

if __name__ == '__main__':
    coin_tweet(["LUNA"])
