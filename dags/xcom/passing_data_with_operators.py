import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Victor'
}


def get_order_prices(**kwargs):
    # 'ti' stands to Task Instance, it's an object that holds information about the
    # current state of the task

    ti = kwargs['ti']

    order_price_data = {
        'o1': 234.45,
        'o2': 10.00,
        'o3': 34.77,
        'o4': 45.66,
        'o5': 399
    }

    order_price_data_string = json.dumps(order_price_data)

    # XCom is a short for cross-communication, allow tasks to exchange messages or small amount of data
    # that can be accessed by other tasks
    ti.xcom_push('order_price_data', order_price_data_string)


def compute_sum(**kwargs):
    ti = kwargs['ti']

    order_price_data_string = ti.xcom_pull(task_ids='get_order_prices', key='order_price_data')

    print(order_price_data_string)

    order_price_data = json.loads(order_price_data_string)
    total = 0

    for order in order_price_data:
        total += order_price_data[order]

    ti.xcom_push('total_price', total)


def compute_average(**kwargs):
    ti = kwargs['ti']

    order_price_data_string = ti.xcom_pull(task_ids='get_order_prices', key='order_price_data')
    print(order_price_data_string)

    order_price_data = json.loads(order_price_data_string)
    total = 0
    count = 0

    for order in order_price_data:
        total += order_price_data[order]
        count += 1

    average = total / count

    ti.xcom_push('average_price', average)


def display_results(**kwargs):
    ti = kwargs['ti']

    total = ti.xcom_pull(task_ids='compute_sum', key='total_price')
    average = ti.xcom_pull(task_ids='compute_average', key='average_price')

    print(f"Total price of goods {total}")
    print(f"Average price of goods {average}")


with DAG(
        dag_id='passing_data_with_operators',
        description='Passing Data with XCom',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['xcom', 'python', 'operators']
) as dag:
    get_order_prices = PythonOperator(
        task_id='get_order_prices',
        python_callable=get_order_prices
    )

    compute_sum = PythonOperator(
        task_id='compute_sum',
        python_callable=compute_sum
    )

    compute_average = PythonOperator(
        task_id='compute_average',
        python_callable=compute_average
    )

    display_results = PythonOperator(
        task_id='display_results',
        python_callable=display_results
    )

get_order_prices >> [compute_sum, compute_average] >> display_results
