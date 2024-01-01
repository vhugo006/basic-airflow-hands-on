import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

airflow_home = os.environ.get("AIRFLOW_HOME")

with DAG(
        dag_id='load_dag',
        description='Load DAG',
        schedule_interval=None,
        start_date=datetime(2023, 12, 26),
        catchup=False
) as dag:
    load_task = BashOperator(
        task_id='load_task',
        bash_command=f'echo -e ".separator ","\n.import --skip 1 {airflow_home}'
                     '/lab/etl-handson/manual-extract-data.csv top_level_domains" |'
                     f'sqlite3 {airflow_home}/airflow-load-db.db',
        dag=dag
    )
