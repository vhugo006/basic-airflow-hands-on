import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

airflow_home = os.environ.get("AIRFLOW_HOME")

with DAG(
        dag_id='transform_dag',
        description='Transform DAG',
        schedule_interval=None,
        start_date=datetime(2023, 12, 26),
        catchup=False
) as dag:
    def transform_data():
        today = datetime.today()
        df = pd.read_csv(f'{airflow_home}/lab/etl-handson/manual-extract-data.csv')

        generic_type_df = df[df['Type'] == "generic"]
        generic_type_df['Date'] = datetime.strftime(today, '%Y-%m-%d')
        generic_type_df.to_csv(f'{airflow_home}/lab/etl-handson/airflow-extract-data.csv', index=False)


    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)
