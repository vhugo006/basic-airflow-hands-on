from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Victor'
}


@dag(dag_id='passing_data_with_taskflow',
     description='Passing Data with XCom and Taskflow API',
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@once',
     tags=['dependencies', 'python', 'taskflow_api'])
def passing_data_with_taskflow():
    @task
    def get_order_prices():

        order_prices_data = {
            'o1': 234.45,
            'o2': 10.00,
            'o3': 34.77,
            'o4': 45.66,
            'o5': 399
        }

        return order_prices_data

    @task
    def compute_sum(order_prices_data: dict):

        print(f'compute_sum order_prices_data: {order_prices_data}')
        total_prices = 0

        for order in order_prices_data:
            total_prices += order_prices_data[order]

        print(f'Total Prices : {total_prices}')
        return total_prices

    @task
    def compute_average(order_prices_data: dict):

        print(f'compute_average order_prices_data: {order_prices_data}')
        total_prices = 0
        count = 0

        for order in order_prices_data:
            total_prices += order_prices_data[order]
            count += 1

        print(f'Total: {total_prices}')
        print(f'Count: {count}')
        average_prices = total_prices / count

        return average_prices

    @task
    def display_results(total_prices, average_prices):

        print(f"Total price of goods {total_prices}")
        print(f"Average price of goods {average_prices}")

    order_price_data = get_order_prices()

    total = compute_sum(order_price_data)
    average = compute_average(order_price_data)

    display_results(total, average)


passing_data_with_taskflow()
