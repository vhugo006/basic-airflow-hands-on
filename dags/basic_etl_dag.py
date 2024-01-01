import os
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

airflow_home = os.environ.get("AIRFLOW_HOME")

with DAG(
        dag_id='basic_etl_dag',
        description='Basic ETL DAG',
        start_date=datetime(2023, 12, 23),
        catchup=False
) as dag:
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://pkgstore.datahub.io/core/top-level-domain-names/'
                     'top-level-domain-names.csv_csv/data/667f4464088f3ca10522e0e2e39c8ae4/'
                     'top-level-domain-names.csv_csv.csv '
                     f'-O {airflow_home}/lab/etl-handson/end-to-end/'
                     'basic-etl-extract-data.csv'
    )


    def transform_data():
        today = datetime.today()
        df = pd.read_csv(f'{airflow_home}/lab/etl-handson/end-to-end'
                         '/basic-etl-extract-data.csv')

        generic_type_df = df[df['Type'] == "generic"]
        generic_type_df['Date'] = datetime.strftime(today, '%Y-%m-%d')
        generic_type_df.to_csv(f'{airflow_home}/lab/etl-handson/''end-to-end/basic-etl-transform-data.csv', index=False)


    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)

    load_task = BashOperator(
        task_id='load_task',
        bash_command=f'echo -e ".separator ","\n.import --skip 1 {airflow_home}/'
                     'lab/etl-handson/end-to-end/basic-etl-transform-data.csv top_level_domains" |'
                     f'sqlite3 {airflow_home}/lab/etl-handson/'
                     'end-to-end/basic-etl-load-db.db',
        dag=dag
    )

    extract_task >> transform_task >> load_task
