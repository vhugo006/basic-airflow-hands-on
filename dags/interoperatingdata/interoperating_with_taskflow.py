import os

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

airflow_home = os.environ.get("AIRFLOW_HOME")

default_args = {
    'owner': 'Victor'
}


@dag(
    dag_id='interoperating_with_taskflow',
    description='Interoperating traditional tasks with taskflow',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['interop', 'python', 'taskflow', 'operators']
)
def interoperating_with_taskflow():
    def read_csv_file():
        df = pd.read_csv(f'{airflow_home}/lab/branching/datasets/car_data.csv')

        print(df)

        return df.to_json()

    @task
    def filter_teslas(json_data):
        df = pd.read_json(json_data)

        tesla_df = df[df['Brand'] == 'Tesla ']

        return tesla_df.to_json()

    def write_csv_result(filtered_teslas_json):
        df = pd.read_json(filtered_teslas_json)

        df.to_csv(f'{airflow_home}/lab/branching/output/teslas.csv', index=False)

    read_csv_file = PythonOperator(
        task_id='read_csv_file_task',
        python_callable=read_csv_file
    )

    filtered_teslas = filter_teslas(read_csv_file.output)

    write_csv_result = PythonOperator(
        task_id='write_csv_result_task',
        python_callable=write_csv_result,
        op_kwargs={'filtered_teslas_json': filtered_teslas}
    )


interoperating_with_taskflow()
