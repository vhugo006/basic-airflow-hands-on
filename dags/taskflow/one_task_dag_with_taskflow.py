"""One Task DAG with TaskFlow API"""
import os

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

airflow_home = os.environ.get("AIRFLOW_HOME")

default_args = {
    'owner': 'Victor'
}


@dag(dag_id='one_task_dag_with_taskflow',
     description='First DAG with TaskFlow API',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['dependencies', 'python', 'taskflow_api'])
def dag_with_taskflow_api():
    @task
    def first_task_with_taskflow():
        with open(f'{airflow_home}/lab/taskflow/message_with_taskflow.txt', 'w') as f:
            f.write("Hello with taskflow.")

    first_task_with_taskflow()


dag_with_taskflow_api()
