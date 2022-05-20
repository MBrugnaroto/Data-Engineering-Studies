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