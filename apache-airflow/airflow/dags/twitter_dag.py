from os.path import join
from datetime import datetime
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join(
            "/home/mbrugnar/workspace/data-engineering-studies/apache-airflow/datalake",
            "twitter_aluraonline",
            "extract_date={{ ds }}",
            "AluraOnline_{{ ds_nodash }}.json"
        )
    )