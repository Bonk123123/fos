from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': days_ago(1)
}

with DAG('hadoop_spark_job', default_args=default_args, schedule_interval='@daily') as dag:
    spark_task = BashOperator(
        task_id='run_spark_job',
        bash_command='spark-submit --master yarn /path/to/app'
    )
