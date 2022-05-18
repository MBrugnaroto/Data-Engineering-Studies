from os.path import join
import argparse

import findspark
findspark.init()
from pyspark.sql import functions as f
from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession\
                    .builder\
                    .appName("twitter_transformation")\
                    .getOrCreate()

def read_extracts(spark, path):
    return spark.read.json(path)
                    
def get_users_data(df):
    return df\
            .select(
                f.explode("includes.users").alias("users"))\
            .select(
                "users.*")

def get_tweets_data(df):
    return df\
            .select(
                    f.explode("data").alias("tweet"))\
            .select(
                    "tweet.author_id", 
                    "tweet.conversation_id", 
                    "tweet.created_at", 
                    "tweet.id", 
                    "tweet.in_reply_to_user_id", 
                    "tweet.public_metrics.*", 
                    "tweet.text")

def define_dest_path(datalake_path):
    return  join(
                datalake_path,
                "silver",
                "{table_name}",
                "process_date={process_date}"
            )

def data_transformed_export(df, dest):
    df\
        .coalesce(1)\
        .write\
        .mode("overwrite")\
        .json(dest)

def twitter_transform(src, dest, process_date):
    spark = create_spark_session()
    df = read_extracts(spark, src)
    
    users_df = get_users_data(df)
    tweets_df = get_tweets_data(df)
    
    dest_users = define_dest_path(dest)\
                            .format(
                                table_name="user", 
                                process_date=f"{process_date}"
                            )
    
    dest_tweets = define_dest_path(dest)\
                            .format(
                                table_name="tweet", 
                                process_date=f"{process_date}"
                            )
                            
    data_transformed_export(users_df, dest_users)
    data_transformed_export(tweets_df, dest_tweets)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()
    
    twitter_transform(args.src, args.dest, args.process_date)
    