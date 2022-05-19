from os.path import join
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

BASE_FOLDER_ALURA = join()

TIMESTEMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'

with DAG(
    dag_id="twitter_dag", 
    default_args=ARGS,
    schedule_interval="0 9 * * *",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_extractor_aluraonline",
        query="AluraOnline",
        file_path=join(
            "/home/mbrugnar/workspace/data-engineering-studies/apache-airflow/datalake",
            "bronze",
            "aluraonline"
            "extract_date={{ ds }}",
            "extraction_{{ ds_nodash }}.json"
        ),
        start_time=("{{"
                        f"execution_date.strftime('{ TIMESTEMP_FORMAT }')"
                    "}}"),
        end_time=("{{"
                        f"next_execution_date.strftime('{ TIMESTEMP_FORMAT }')"
                    "}}")
    )
    
    spark_operator = SparkSubmitOperator(
        task_id="twitte_transformation_aluraonline",
        application=("/home/mbrugnar/workspace/data-engineering-studies/"
                    "apache-airflow/spark/transformation.py"),
        name="twitte_transformation",
        application_args=[
            "--src",
            "/home/mbrugnar/workspace/data-engineering-studies/"
            "apache-airflow/datalake/bronze/aluraonline/extract_date=2022-05-17",
            "--dest",
            "/home/mbrugnar/workspace/data-engineering-studies/"
            "apache-airflow/datalake/silver/aluraonline",
            "--process-date",
            "{{ ds }}"
        ]
    )
    
    twitter_operator >> spark_operator
    
    