import tweepy
import csv, json, os


def print_message(id, done, remaining, skipped):
    print(id, "Done:", done, "| Remaining:", remaining,
          "| Skipped - ", skipped, flush=True)


def twitter_api(keys):
    auth = tweepy.OAuthHandler(keys["consumer_key"], keys["consumer_secret"])
    auth.set_access_token(keys["access_token"], keys["access_secret"])
    api = tweepy.API(auth)
    return api


def download_sanders(keys):
    done_count = len([file for file in os.listdir(r"../data/sanders/rawdata/") if file.endswith(".json")])
    with open(r"../data/sanders/corpus.csv", "rt") as f:
        reader = csv.reader(f)
        tweet_list = [row[2] for row in reader]
    total_tweets = len(tweet_list)

    while done_count < total_tweets:
        for id in tweet_list:
            try:
                if not os.path.isfile(r"../data/sanders/rawdata/" + id + ".json"):
                    tweet = twitter_api(keys).get_status(id)
                    with open(r"../data/sanders/rawdata/" + id + ".json", "wt") as f:
                        json.dump(tweet._json, f)
                        done_count += 1
                        print_message(id, done_count, len(tweet_list), "False")
                else:
                    print_message(id, done_count, len(tweet_list), "Repeated File")
                tweet_list.remove(id)
            except tweepy.TweepError:
                print_message(id, done_count, len(tweet_list), "Tweepy Error")
                continue


if __name__ == "__main__":
    keys = json.load(open("../data/twitter_keys.json", "rt"))
    download_sanders(keys)