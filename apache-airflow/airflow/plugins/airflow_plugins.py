from airflow.plugins_manager import AirflowPlugin
from operators.twitter_operator import TwitterOperator

class AluraAirflowPlugins(AirflowPlugin):
    name = "alura"
    operators = [TwitterOperator]
    