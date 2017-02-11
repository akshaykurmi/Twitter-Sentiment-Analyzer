import json

import pymysql
import tweepy


def db_credentials():
    return json.load(open("../../data/mysql_credentials.json", "rt"))


def twitter_keys():
    return json.load(open("../../data/twitter_keys.json", "rt"))


def twitter_api(keys):
    auth = tweepy.OAuthHandler(keys["consumer_key"], keys["consumer_secret"])
    auth.set_access_token(keys["access_token"], keys["access_secret"])
    api = tweepy.API(auth)
    return api


def create_table_if_not_exists(cur, db, table_name, create_query):
    try:
        temp_con = pymysql.connect(host=db["host"], user=db["user"],
                                   passwd=db["password"], db="information_schema")
        temp_cur = temp_con.cursor()
        temp_cur.execute('select * from TABLES where \
                    table_schema="sentiment_analyzer" and table_name="' + table_name + '"')
        if len(temp_cur.fetchall()) == 0:
            cur.execute(create_query)
        temp_cur.close()
        temp_con.commit()
        temp_con.close()
    except Exception as e:
        print('Database Creation Failed')
        print(e)
