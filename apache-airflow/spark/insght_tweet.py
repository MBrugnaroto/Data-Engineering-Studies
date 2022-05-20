import modules_spark as ModulesSpark
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = ModulesSpark.create_spark_session()
    users = ModulesSpark.read_extracts(spark,
                                       "/home/mbrugnar/datalake/silver/aluraonline/user")
    
    alura_df = users.select("id", "name", "username").distinct().where(f.col("username") == "AluraOnline")
    print(alura_df.toPandas())
    
    tweets = ModulesSpark.read_extracts(spark, 
                                       "/home/mbrugnar/datalake/silver/aluraonline/tweet")
    tweets.printSchema()