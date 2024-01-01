import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

airflow_home = os.environ.get("AIRFLOW_HOME")

with DAG(
        dag_id='extract_dag',
        description='Extract DAG',
        start_date=datetime(2023, 12, 23),
        catchup=False
) as dag:
    extract = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://pkgstore.datahub.io/core/top-level-domain-names/'
                     'top-level-domain-names.csv_csv/data/667f4464088f3ca10522e0e2e39c8ae4'
                     '/top-level-domain-names.csv_csv.csv '
                     f'-O {airflow_home}/lab/etl-handson/airflow-extract-data.csv'
    )
