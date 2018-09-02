from decouple import Csv, config

TWEETS = config('TWEETS', default=True, cast=bool)
ISUSERPROFILE = config('ISUSERPROFILE', default=True, cast=bool)
ISLOCATION = config('ISLOCATION', default=False, cast=bool)
ISREPLY = config('ISREPLY', default=False, cast=bool)

PG_DBNAME = config('PG_DBNAME')
PG_USER = config('PG_USER')
PG_PASSWORD = config('PG_PASSWORD')
DB_HOST = config('DB_HOST')

PROFILE_SEARCH = config('PROFILE_SEARCH', default=False, cast=bool)
TWITTER_USERNAME = config('TWITTER_USERNAME')
TWITTER_PASSWORD = config('TWITTER_PASSWORD')
PROXY_LIST = config('PROXY_LIST', cast=Csv())
