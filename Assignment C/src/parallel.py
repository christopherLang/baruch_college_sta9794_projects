import pyspark
from pyspark import SparkContext
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import StringType, IntegerType, TimestampType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pyspark.sql.functions as sqlfun
from pyspark import StorageLevel as stgl
import os
import sys
import json
import configparser
import datetime as dt
import re
from string import punctuation

# Developed and test on Spark 2.1.0 with Python 3.5.3
# Check the readme file for commands that setup the proper environment in
# Hortonwork's Sandbox VM


def main():
    os.chdir("/root/2017-STAT-9794-PySparks")
    sys.path.append("/root/2017-STAT-9794-PySparks/lib")

    cfg = load_configuration()

    # Set schema for tweet JSON DataFrame
    tweetdf_schema = [
        StructField("status_id", StringType(), False),
        StructField("datetime", TimestampType(), False),
        StructField("date", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("tf_ms", IntegerType(), False),
        StructField("tf_ibm", IntegerType(), False),
        StructField("tf_sbx", IntegerType(), False),
        StructField("tf_nv", IntegerType(), False),
        StructField("tf_yh", IntegerType(), False),
        StructField("n_mentions", IntegerType(), False),
        StructField("n_hashtags", IntegerType(), False),
        StructField("n_puncts", IntegerType(), False),
        StructField("n_exclamations", IntegerType(), False),
        StructField("n_questions", IntegerType(), False),
        StructField("n_alnum", IntegerType(), False),
        StructField("n_money", IntegerType(), False),
        StructField("text", StringType(), False)
    ]

    tweetdf_schema = StructType(tweetdf_schema)

    # Set stock price schema DataFrame
    prices_schema = [
        StructField("date", StringType(), False),
        StructField("close_ibm", FloatType(), False),
        StructField("close_msft", FloatType(), False),
        StructField("close_nvda", FloatType(), False),
        StructField("close_sbux", FloatType(), False),
        StructField("close_yhoo", FloatType(), False)
    ]

    prices_schema = StructType(prices_schema)

    with SparkContext() as sc:
        spark_session = (
            pyspark.sql.session.SparkSession
            .builder
            .master("spark://master:7077")
            .appName("Word Count")
            .getOrCreate()
        )

        twt_rdd = (
            sc.textFile(cfg['fs']['dataloc'])
            .repartition(cfg['sparkconfig'].getint('ncores') *
                         cfg['sparkconfig'].getint('partitionfactor'))
            .map(json.loads)
            .filter(lambda x: "delete" not in x.keys())
            .filter(lambda x: "lang" in x.keys())
            .filter(lambda x: x['lang'] == 'en')
            .map(tweet_scrubber)
            .map(feature_generator)
            .persist(stgl.MEMORY_AND_DISK)
        )

        # Load asset prices
        stocks_df = spark_session.read.csv(cfg['fs']['priceloc'],
                                           schema=prices_schema,
                                           header=True)

        df = (
            spark_session.createDataFrame(twt_rdd, schema=tweetdf_schema)
            .groupBy('date')
            .agg(
                sqlfun.countDistinct('status_id').alias('n_tweets'),
                sqlfun.sum('tf_ms').alias('tf_ms'),
                sqlfun.sum('tf_ibm').alias('tf_ibm'),
                sqlfun.sum('tf_sbx').alias('tf_sbx'),
                sqlfun.sum('tf_nv').alias('tf_nv'),
                sqlfun.sum('tf_yh').alias('tf_yh'),
                sqlfun.sum('n_mentions').alias('n_mentions'),
                sqlfun.sum('n_hashtags').alias('n_hashtags'),
                sqlfun.sum('n_puncts').alias('n_puncts'),
                sqlfun.sum('n_exclamations').alias('n_exclamations'),
                sqlfun.sum('n_questions').alias('n_questions'),
                sqlfun.sum('n_alnum').alias('n_alnum'),
                sqlfun.sum('n_money').alias('n_money')
            )
            .join(stocks_df, on="date", how="left")
            .dropna()
            .persist(stgl.MEMORY_AND_DISK)
        )

        # Create feature vector as a new column
        # The model frame will contain just two columns:
        # 1. Dependent variable (y in this case)
        # 2. Feature vector (column name is features)
        mf = (
            VectorAssembler(
                inputCols=['n_tweets', 'tf_ibm', 'n_mentions',
                           'n_hashtags', 'n_puncts', 'n_exclamations',
                           'n_questions', 'n_alnum', 'n_money'],
                outputCol="features")
            .transform(df)
            .select('close_ibm', 'features')
            .withColumnRenamed('close_ibm', 'label')
        )

        # Fit a linear regression model
        # The below fit seemed to have many zero coefficient
        # As stated in the project document, term frequency of IBM mentions
        # proved to have SOME correlation, though its a bit weak
        lrModel = LinearRegression(maxIter=1000, solver="normal").fit(mf)
        lrModel = None  # None-ing this reminds me that it is irrelevant
        mf = None

        # Reassemble a new mf (model frame) that only has the follow variables
        # 1. term frequency (tf_ibm)
        mf = (
            VectorAssembler(inputCols=['tf_ibm'], outputCol="features")
            .transform(df)
            .select('close_ibm', 'features')
            .withColumnRenamed('close_ibm', 'label')
        )

        lrModel = LinearRegression(maxIter=1000, solver="normal").fit(mf)


def feature_generator(tweet):
    result = dict()

    # Basic features, mostly for identifying the tweet status ----
    result['status_id'] = tweet['id_str']
    result['datetime'] = tweet['created_at']
    result['date'] = dt.datetime.strftime(result['datetime'], "%Y-%m-%d")
    result['user_id'] = tweet['user']['id_str']
    result['text'] = tweet['text']

    # Generating text features useful for modeling ----

    # Generate term frequency features
    tw_txt = tweet['text'].lower()
    result["tf_ms"] = len(re.findall("microsoft|MSFT", tw_txt,
                                     flags=re.I))
    result["tf_ibm"] = len(re.findall("ibm", tw_txt, flags=re.I))
    result["tf_sbx"] = len(re.findall("starbucks|SBUX", tw_txt,
                                      flags=re.I))
    result["tf_nv"] = len(re.findall("nvidia|NVDA", tw_txt,
                                     flags=re.I))
    result["tf_yh"] = len(re.findall("yahoo|yhoo", tw_txt,
                                     flags=re.I))

    # Generate more specialized features
    tw_txt = tweet['text'].replace("RT", " ")

    n_mentions = len(re.findall("[@][\w_]+", tweet['text']))
    n_hashtags = len(re.findall("[#][\w]+", tweet['text']))

    n_puncts = sum([1 if i in punctuation else 0 for i in tweet['text']])
    n_exclamations = sum([1 if i in ("!") else 0 for i in tweet['text']])
    n_questions = sum([1 if i in ("?") else 0 for i in tweet['text']])

    n_alnum = len(re.findall("\w", tw_txt))

    n_money = len(re.findall("[$]?\d+[.]?\d{1,2}", tw_txt))

    result["n_mentions"] = n_mentions
    result["n_hashtags"] = n_hashtags
    result["n_puncts"] = n_puncts
    result["n_exclamations"] = n_exclamations
    result["n_questions"] = n_questions
    result["n_alnum"] = n_alnum
    result["n_money"] = n_money

    return result


def tweet_scrubber(tweet):
    fields_keep = [
        "text",
        "id_str",
        "favorite_count",
        "retweet_count",
        "entities",
        "created_at",
        "user"
    ]

    result = dict([(k, v) for k, v in tweet.items() if k in fields_keep])

    try:
        hashtags = result['entities']['hashtags']
        hashtags = [i['text'] for i in hashtags]
        result['hashtags'] = hashtags

    except KeyError:
        result['hashtags'] = None

    user_fields_keep = [
        "favourites_count",
        "follow_request_sent",
        "followers_count",
        "friends_count",
        "verified",
        "id_str"
    ]

    user = dict([(k, v) for k, v in result['user'].items()
                 if k in user_fields_keep])

    result['user'] = user

    result['created_at'] = dt.datetime.strptime(
        result['created_at'],
        "%a %b %d %H:%M:%S %z %Y")

    return result


def load_configuration():
    config = configparser.ConfigParser()
    config.read("config/main_config.ini")

    return config


if __name__ == "__main__":
    main()
