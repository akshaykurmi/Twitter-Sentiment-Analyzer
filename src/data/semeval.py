import csv
import json
import os

import pymysql

from utils import db_credentials, create_table_if_not_exists


def print_message(num, item, add_count, skip_count, rep_count):
    print(num, item[0], "Added:", add_count, "Skipped:", skip_count,
          "Repeated:", rep_count)


def create_table(db):
    con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                          db="sentiment_analyzer", charset="utf8")
    cur = con.cursor()
    table_name = "semeval15_corpus"
    create_query = "create table semeval15_corpus( \
                topic_polarity varchar(100) default 'NA', \
                tweet_polarity varchar(100) default 'NA', \
                tweet_text text character set utf8 collate utf8_unicode_ci, \
                tweet_id varchar(50) primary key, \
                topic varchar(100) default 'NA', \
                retweet_count int, \
                favorite_count int, \
                lang varchar(10), \
                entities text character set utf8 collate utf8_unicode_ci)"
    create_table_if_not_exists(cur, db, table_name, create_query)
    cur.close()
    con.commit()
    con.close()


def phase_1(db):
    tweet_list = list()
    file_list = ["twitter-train-cleansed-B.tsv", "twitter-dev-gold-B.tsv",
                 "twitter-test-gold-B.tsv"]
    for file in file_list:
        with open("../../data/semeval15/" + file, "rt") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                tweet_list.append([row[0], row[len(row) - 1]])
    add_count = 0
    skip_count = 0
    rep_count = 0
    print("Total Tweets:", len(tweet_list), flush=True)
    try:
        con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                              db="sentiment_analyzer", charset="utf8")
        cur = con.cursor()
        for num, item in enumerate(tweet_list):
            if os.path.exists("../../data/semeval15/rawdata/" + item[0] + ".json"):
                with open("../../data/semeval15/rawdata/" + item[0] + ".json", "rt") as rf:
                    data = json.loads(rf.read())
                sentiment = item[1]
                text = data.get("text", "NA")
                text = text.encode("unicode_escape")
                tweet_id = data.get("id_str", "NA")
                retweet_count = data.get("retweet_count", "NA")
                favorite_count = data.get("favorite_count", "NA")
                lang = data.get("lang", "NA")
                entities = str(data.get("entities", "NA"))
                try:
                    cur.execute("insert into semeval15_corpus(tweet_polarity, tweet_text, \
                    tweet_id, retweet_count, favorite_count, lang, entities) \
                    values(%s, %s, %s, %s, %s, %s, %s)", (sentiment, text, tweet_id,
                                                          str(retweet_count), str(favorite_count), lang, entities))
                    con.commit()
                    add_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                except:
                    rep_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                    continue
            else:
                skip_count += 1
                print_message(num, item, add_count, skip_count, rep_count)
        cur.close()
        con.close()
    except Exception as e:
        print(e)
    return add_count, skip_count, rep_count


def phase_2(db):
    file = "twitter-train-CD-tweet-and-topics.csv"
    tweet_list = list()
    with open("../../data/semeval15/" + file, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            tweet_list.append([row[0], row[1], row[2], row[3]])
    add_count = 0
    skip_count = 0
    rep_count = 0
    print("Total Tweets:", len(tweet_list), flush=True)
    try:
        con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                              db="sentiment_analyzer", charset="utf8")
        cur = con.cursor()
        for num, item in enumerate(tweet_list):
            if os.path.exists("../../data/semeval15/rawdata/" + item[0] + ".json"):
                with open(r"../../data/semeval15/rawdata/" + item[0] + ".json", "rt") as rf:
                    data = json.loads(rf.read())
                topic = item[1]
                tweet_polarity = item[2]
                topic_polarity = item[3]
                text = data.get("text", "NA")
                text = text.encode("unicode_escape")
                tweet_id = data.get("id_str", "NA")
                retweet_count = str(data.get("retweet_count", "NA"))
                favorite_count = str(data.get("favorite_count", "NA"))
                lang = data.get("lang", "NA")
                entities = str(data.get("entities", "NA"))
                try:
                    cur.execute("insert into semeval15_corpus(tweet_polarity,  \
                    topic_polarity, topic, tweet_text, tweet_id, retweet_count, \
                    favorite_count, lang, entities) values(%s, %s, %s, %s, \
                    %s, %s, %s, %s, %s)", (tweet_polarity, topic_polarity, topic,
                                           text, tweet_id, retweet_count, favorite_count, lang, entities))
                    con.commit()
                    add_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                except:
                    rep_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                    continue
            else:
                skip_count += 1
                print_message(num, item, add_count, skip_count, rep_count)
        cur.close()
        con.close()
    except Exception as e:
        print(e)
    return add_count, skip_count, rep_count


def phase_3(db):
    file = "twitter-trial-CD.csv"
    tweet_list = list()
    with open("../../data/semeval15/" + file, "rt") as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            tweet_list.append([row[0], row[1], row[3]])
    add_count = 0
    skip_count = 0
    rep_count = 0
    print("Total Tweets:", len(tweet_list), flush=True)
    try:
        con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                              db="sentiment_analyzer", charset="utf8")
        cur = con.cursor()
        for num, item in enumerate(tweet_list):
            if os.path.exists("../../data/semeval15/rawdata/" + item[0] + ".json"):
                with open(r"../../data/semeval15/rawdata/" + item[0] + ".json", "rt") as rf:
                    data = json.loads(rf.read())
                topic = item[1]
                topic_polarity = item[2]
                text = data.get("text", "NA")
                text = text.encode("unicode_escape")
                tweet_id = data.get("id_str", "NA")
                retweet_count = str(data.get("retweet_count", "NA"))
                favorite_count = str(data.get("favorite_count", "NA"))
                lang = data.get("lang", "NA")
                entities = str(data.get("entities", "NA"))
                try:
                    cur.execute("insert into semeval15_corpus(topic_polarity,  \
                    topic, tweet_text, tweet_id, retweet_count, favorite_count, \
                    lang, entities) values(%s, %s, %s, %s, %s, %s, %s, %s)",
                                (topic_polarity, topic, text, tweet_id, retweet_count,
                                 favorite_count, lang, entities))
                    con.commit()
                    add_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                except:
                    rep_count += 1
                    print_message(num, item, add_count, skip_count, rep_count)
                    continue
            else:
                skip_count += 1
                print_message(num, item, add_count, skip_count, rep_count)
        cur.close()
        con.close()
    except Exception as e:
        print(e)
    return add_count, skip_count, rep_count


def store_in_db():
    db = db_credentials()
    create_table(db)
    a, b, c = phase_1(db)
    l, m, n = phase_2(db)
    x, y, z = phase_3(db)
    print("Phase 1")
    print("Added:", a, "Skipped:", b, "Repeated:", c)
    print("Phase 2")
    print("Added:", l, "Skipped:", m, "Repeated:", n)
    print("Phase 3")
    print("Added:", x, "Skipped:", y, "Repeated:", z)
    print("Total")
    print("Added:", a + l + x, "Skipped:", b + m + y, "Repeated:", c + n + z)
