from operator import imod
from os.path import join
from datetime import datetime
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_extractor",
        query="AluraOnline",
        file_path=join(
            "/home/mbrugnar/workspace/data-engineering-studies/apache-airflow/datalake",
            "bronze",
            "extract_date={{ ds }}",
            "extraction_{{ ds_nodash }}.json"
        )
    )
    
    spark_operator = SparkSubmitOperator(
        application="twitte_transformation",
        
    )