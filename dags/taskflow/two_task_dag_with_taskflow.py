"""Two Task DAG"""

import time

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Victor'
}


@dag(
    dag_id='two_tasks_dag_with_taskflow',
    description='Two Task DAG with TaskFlow API',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['dependencies', 'python', 'taskflow_api']
)
def two_tasks_dag_with_taskflow():
    @task
    def task0_with_taskflow():
        print('task0_with_taskflow run')

    @task
    def task1_with_taskflow():
        print('task1_with_taskflow sleeping')
        time.sleep(5)
        print('task1_with_taskflow run')

    task0_with_taskflow()
    task1_with_taskflow()


two_tasks_dag_with_taskflow()
