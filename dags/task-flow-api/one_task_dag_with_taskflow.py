"""One Task DAG with TaskFlow API"""
from datetime import datetime

from airflow.decorators import dag, task

default_args = {
    'owner': 'Victor',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2024, 1, 1)
}


@dag(dag_id='my_first_dag_with_task_flow',
     description='My first DAG in Airflow',
     schedule_interval='@once',
     default_args=default_args,
     tags=['dependencies', 'python', 'taskflow_api'])
def my_first_dag():
    @task
    def first_task():
        with open('message_with_taskflow.txt', 'w') as f:
            f.write("Hello with taskflow.")
