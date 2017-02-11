import pymysql

from utils import db_credentials


def recreate_table(cur):
    try:
        cur.execute('drop table full_corpus')
    except:
        pass
    cur.execute("create table full_corpus( \
        topic_polarity varchar(100) default 'NA', \
        tweet_polarity varchar(100) default 'NA', \
        tweet_text text character set utf8 collate utf8_unicode_ci, \
        tweet_id varchar(50) primary key, \
        topic varchar(100) default 'NA', \
        retweet_count int default 0, \
        favorite_count int default 0, \
        entities text character set utf8 collate utf8_unicode_ci, \
        source varchar(50) default 'NA')")


def insert_sanders(add_count, con, cur, skip_count):
    cur.execute('select topic_polarity, tweet_text, tweet_id, \
        topic, retweet_count, favorite_count, entities from sanders_corpus \
        where lang="en"')
    result = cur.fetchall()
    for row in result:
        try:
            cur.execute('insert into full_corpus(topic_polarity, tweet_text, \
                tweet_id, topic, retweet_count, favorite_count, entities, source) \
                values(%s, %s, %s, %s, %s, %s, %s, %s)',
                        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], 'sanders'))
            con.commit()
            add_count += 1
            print('Added:', add_count, 'Skipped:', skip_count)
        except Exception as e:
            skip_count += 1
            print('Added:', add_count, 'Skipped:', skip_count)
            print(e)
            break


def insert_semeval(add_count, con, cur, skip_count):
    cur.execute('select topic_polarity, tweet_polarity, tweet_text, tweet_id, \
        topic, retweet_count, favorite_count, entities from semeval15_corpus \
        where lang="en"')
    result = cur.fetchall()
    for row in result:
        try:
            cur.execute('insert into full_corpus values(%s, %s, %s, %s, %s, \
                %s, %s, %s, %s)', (row[0], row[1], row[2], row[3], row[4], row[5],
                                   row[6], row[7], 'semeval15'))
            con.commit()
            add_count += 1
            print('Added:', add_count, 'Skipped:', skip_count)
        except Exception as e:
            skip_count += 1
            print('Added:', add_count, 'Skipped:', skip_count)
            print(e)
            break
    return add_count, skip_count


def merge_sanders_and_semeval():
    db = db_credentials()
    add_count = 0
    skip_count = 0
    try:
        con = pymysql.connect(host=db["host"], user=db["user"], passwd=db["password"],
                              db='sentiment_analyzer', charset='utf8')
        cur = con.cursor()
        recreate_table(cur)
        add_count, skip_count = insert_semeval(add_count, con, cur, skip_count)
        insert_sanders(add_count, con, cur, skip_count)
    except Exception as e:
        print(e)
