"""One Task DAG"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

airflow_home = os.environ.get("AIRFLOW_HOME")

default_args = {
    'owner': 'Victor',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False,
    'start_date': datetime(2023, 12, 23)
}

with DAG(
        dag_id='my_first_dag',
        description='My first DAG in Airflow',
        schedule_interval=None,
        default_args=default_args
) as dag:
    task1 = BashOperator(
        task_id='my_first_task',
        bash_command=f'echo "hello to my first DAG" > {airflow_home}/createthisfile.txt'
    )
