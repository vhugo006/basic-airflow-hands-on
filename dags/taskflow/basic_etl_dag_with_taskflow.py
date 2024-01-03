import os
import sqlite3
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

airflow_home = os.environ.get("AIRFLOW_HOME")
default_args = {
    'owner': 'Victor'
}


@dag(dag_id='basic_etl_dag_with_taskflow',
     description='Basic ETL with Taskflow API',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['dependencies', 'python', 'taskflow_api'])
def basic_etl_dag_with_taskflow():
    @task
    def extract_task():
        url = ('https://pkgstore.datahub.io/core/top-level-domain-names/'
               'top-level-domain-names.csv_csv/data/667f4464088f3ca10522e0e2e39c8ae4/'
               'top-level-domain-names.csv_csv.csv')

        print(url)

        response = requests.get(url)

        with open(f'{airflow_home}/lab/etl-handson/end-to-end'
                  '/basic-etl-extract-data-with-taskflow.csv', 'wb') as output_file:
            output_file.write(response.content)

    @task
    def transform_data():
        today = datetime.today()
        df = pd.read_csv(f'{airflow_home}/lab/etl-handson/end-to-end'
                         '/basic-etl-extract-data-with-taskflow.csv')

        generic_type_df = df[df['Type'] == "generic"]
        generic_type_df['Date'] = datetime.strftime(today, '%Y-%m-%d')
        generic_type_df.to_csv(f'{airflow_home}/lab/etl-handson/end-to-end/basic-etl-transform-with-taskflow-data.csv',
                               index=False)

    @task
    def load_data():
        conn = sqlite3.connect(f'{airflow_home}/lab/etl-handson/end-to-end/load-db-with-taskflow.db')

        # Load CSV file
        df = pd.read_csv(f'{airflow_home}/lab/etl-handson/end-to-end/basic-etl-transform-with-taskflow-data.csv')

        # Write DataFrame to SQLite table
        df.to_sql('top_level_domains', conn, if_exists='replace', index=False)

    extract_task() >> transform_data() >> load_data()


basic_etl_dag_with_taskflow()
