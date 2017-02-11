import csv
import json
import os

import pymysql

from utils import db_credentials, create_table_if_not_exists


def print_message(add_count, item, num, skip_count):
    print(num, item[1], "Added:", add_count, "Skipped:", skip_count)


def create_table(cur, db):
    table_name = "sanders_corpus"
    create_query = "create table sanders_corpus( \
                    topic_polarity varchar(100), \
                    tweet_text text character set utf8 collate utf8_unicode_ci, \
                    tweet_id varchar(50) primary key, \
                    topic varchar(100), \
                    retweet_count int, \
                    favorite_count int, \
                    lang varchar(10), \
                    entities text character set utf8 collate utf8_unicode_ci)"
    create_table_if_not_exists(cur, db, table_name, create_query)


def insert_into_table(con, cur, data, item):
    sentiment = item[0]
    topic = item[2]
    text = data.get("text", "NA")
    text = text.encode("unicode_escape")
    tweet_id = data.get("id_str", "NA")
    retweet_count = data.get("retweet_count", "NA")
    favorite_count = data.get("favorite_count", "NA")
    lang = data.get("lang", "NA")
    entities = str(data.get("entities", "NA"))
    cur.execute("insert into sanders_corpus values(%s, %s, %s, %s, \
                %s, %s, %s, %s)", (sentiment, text, tweet_id, topic, str(retweet_count),
                                   str(favorite_count), lang, entities))
    con.commit()


def store_in_db():
    with open("../../data/sanders/corpus.csv", "rt") as f:
        reader = csv.reader(f)
        tweet_list = [(row[1], row[2], row[0]) for row in reader]
    add_count = 0
    skip_count = 0
    db = db_credentials()
    try:
        con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                              db="sentiment_analyzer", charset="utf8")
        cur = con.cursor()
        create_table(cur, db)
        for num, item in enumerate(tweet_list):
            if os.path.exists("../../data/sanders/rawdata/" + item[1] + ".json"):
                data = json.load(open("../../data/sanders/rawdata/" + item[1] + ".json", "rt"))
                add_count += 1
                print_message(add_count, item, num, skip_count)
                insert_into_table(con, cur, data, item)
            else:
                skip_count += 1
                print_message(add_count, item, num, skip_count)
        cur.close()
        con.close()
    except Exception as e:
        print(e)
