from src.data.download_tweets import download_sanders, download_semeval
from src.data.sanders import store_in_db as store_sanders
from src.data.semeval import store_in_db as store_semeval
from src.data.merge import merge_sanders_and_semeval


# Execute each of these functions one by one in the same order
if __name__ == "__main__":
    download_sanders()
    download_semeval()
    store_sanders()
    store_semeval()
    merge_sanders_and_semeval()
