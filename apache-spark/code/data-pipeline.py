import os
import zipfile

# Encontra o diretório do apache spark (utiliza a variável de ambiente $SPARK_HOME para pegar o diretório)
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

def downloadAndUnzipData(url, dest):
    os.system(f'wget {url} -P {dest} -q --show-progress')
    filename = url.split('/')[-1]
    zipfile.ZipFile(dest+'/'+filename, 'r').extractall(path=dest)

def sparkDataFrame(spark, files):
    return spark.read.csv(files, sep=';', inferSchema=True)

def totalRecords(df, name):
    print(f'Total {name}: {df.count()}')
    
if __name__ == '__main__':
    path_raw_data =  '../raw-data'
    
    # downloadAndUnzipData('https://caelum-online-public.s3.amazonaws.com/' +
    #                      '2273-introducao-spark/01/empresas.zip', 
    #                      path_raw_data)
    # downloadAndUnzipData('https://caelum-online-public.s3.amazonaws.com/' +
    #                      '2273-introducao-spark/01/estabelecimentos.zip', 
    #                      path_raw_data)
    # downloadAndUnzipData('https://caelum-online-public.s3.amazonaws.com/' + 
    #                      '2273-introducao-spark/01/socios.zip', 
    #                      path_raw_data)
    
    spark = createWorker()
    df_companies = sparkDataFrame(spark, '../raw-data/empresas/part-*')
    df_establishments = sparkDataFrame(spark, '../raw-data/estabelecimentos/part-*')
    df_partners = sparkDataFrame(spark, '../raw-data/socios/part-*')
    
    totalRecords(df_companies, 'Companies')
    print(df_companies.limit(5).toPandas())
    totalRecords(df_establishments, 'Establishments')
    totalRecords(df_partners, 'Partners')