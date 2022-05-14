# Encontra o diretório do apache spark (utiliza a variável de ambiente $SPARK_HOME para pegar o diretório)
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession

def createWorker():
    """
        Inicializa o servidor e configura o worker.
    """
    return SparkSession.builder \
        .master('local[*]') \
        .appName('Iniciando com Spark') \
        .config("spark.driver.bindAddress", "localhost") \
        .getOrCreate()

spark = createWorker()