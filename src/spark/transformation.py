from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse

#pega teweets
def get_tweets_data(df):

    tweet_df = df.select(f.explode("data")\
                .alias("twetts"))\
                .select("twetts.author_id", "twetts.conversation_id",
                        "twetts.created_at", "twetts.id",
                        "twetts.public_metrics.*", "twetts.text")
    return tweet_df

#pega dados do usuário
def get_users_data(df):

    user_df = df.select(f.explode("includes.users").alias("users")).select("users.*")
    return user_df

def export_json(df, dest):

    df.coalesce(1).write.mode("overwrite").json(dest)


def twitter_transformation(spark, src, dest, process_date):
    
    df = spark.read.json(src)

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_dest = join(dest, "{table_name}", f"process_date={process_date}")

    export_json(tweet_df, table_dest.format(table_name="tweet"))
    export_json(user_df, table_dest.format(table_name="user"))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)

    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transformation(spark, args.src, args.dest, args.process_date)