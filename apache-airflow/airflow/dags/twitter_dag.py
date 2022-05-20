from pathlib import Path
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
SOURCE_FOLDER=str(Path(__file__).parents[2])
PARTITION_FODER = "extract_date={{ ds }}"
TIMESTEMP_FORMAT = '%Y-%m-%dT%H:%M:%S.00Z'
DATA_LAKE_ALURA = join(
            SOURCE_FOLDER,
            "datalake",
            "{layer}",
            "{table}",
            "{partition}")

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
            DATA_LAKE_ALURA.format(layer='bronze', 
                                   table='aluraonline', 
                                   partition=PARTITION_FODER),
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
        application=join(
                    SOURCE_FOLDER,
                    "spark/transformation.py"),
        name="twitte_transformation",
        application_args=[
            "--src",
            DATA_LAKE_ALURA.format(layer='bronze', 
                                   table='aluraonline', 
                                   partition=PARTITION_FODER),
            "--dest",
            DATA_LAKE_ALURA.format(layer='silver', 
                                   table='aluraonline', 
                                   partition=""),
            "--process-date",
            "{{ ds }}"
        ]
    )
    
    twitter_operator >> spark_operator
    
    