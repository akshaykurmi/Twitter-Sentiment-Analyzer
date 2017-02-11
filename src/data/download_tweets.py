import csv
import json
import os

import tweepy

from utils import twitter_keys, twitter_api


def print_message(file_id, done, remaining, skipped):
    print(file_id,
          "Done:", done,
          "| Remaining:", remaining,
          "| Skipped - ", skipped,
          flush=True)


def current_download_status(raw_data_path, input_file_path, file_list, index, delimiter):
    done_count = len([file for file in os.listdir(raw_data_path) if file.endswith(".json")])
    tweet_list = []
    for file in file_list:
        with open(input_file_path + file, "rt") as f:
            reader = csv.reader(f, delimiter=delimiter)
            tweet_list += [row[index] for row in reader]
    total_tweets = len(tweet_list)
    return done_count, tweet_list, total_tweets


def download_tweets(keys, done_count, total_tweets, tweet_list, raw_data_path):
    while done_count < total_tweets:
        for file_id in tweet_list:
            try:
                if not os.path.isfile(raw_data_path + file_id + ".json"):
                    tweet = twitter_api(keys).get_status(file_id)
                    with open(raw_data_path + file_id + ".json", "wt") as f:
                        json.dump(tweet._json, f)
                        done_count += 1
                        print_message(file_id, done_count, len(tweet_list), "False")
                else:
                    print_message(file_id, done_count, len(tweet_list), "Repeated File")
                tweet_list.remove(file_id)
            except tweepy.TweepError:
                print_message(file_id, done_count, len(tweet_list), "Tweepy Error")
                continue


def download_sanders():
    file_list = ["corpus.csv"]
    done_count, tweet_list, total_tweets = current_download_status("../../data/sanders/rawdata/",
                                                                   "../../data/sanders/",
                                                                   file_list,
                                                                   2,
                                                                   ",")
    download_tweets(twitter_keys(),
                    done_count,
                    total_tweets,
                    tweet_list,
                    "../../data/sanders/rawdata/")


def download_semeval():
    file_list = ['twitter-dev-full-A.tsv',
                 'twitter-dev-full-B.tsv',
                 'twitter-dev-gold-A.tsv',
                 'twitter-dev-gold-B.tsv',
                 'twitter-dev-input-A.tsv',
                 'twitter-dev-input-B.tsv',
                 'twitter-test-gold-A.tsv',
                 'twitter-test-gold-B.tsv',
                 'twitter-train-CD-topics.csv',
                 'twitter-train-CD-tweet-and-topics.csv',
                 'twitter-train-cleansed-A.tsv',
                 'twitter-train-cleansed-B.tsv',
                 'twitter-train-full-A.tsv',
                 'twitter-train-full-B.tsv',
                 'twitter-trial-CD.csv',
                 'twitter_full_cleansed.csv']
    done_count, tweet_list, total_tweets = current_download_status("../../data/semeval15/rawdata/",
                                                                   "../../data/semeval15/",
                                                                   file_list,
                                                                   0,
                                                                   "\t")
    download_tweets(twitter_keys(),
                    done_count,
                    total_tweets,
                    tweet_list,
                    "../../data/semeval15/rawdata/")
