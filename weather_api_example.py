from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 10, 31),
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    'weather_api_example',
    default_args=default_args,
)

extract = SimpleHttpOperator(
    task_id='extract_data',
    method='GET',
    http_conn_id='openweathermap_api',
    headers={"Content-Type": "application/json"},
    response_filter=lambda response: response.json(),
    dag=dag,
)

def transform_data(ds, **kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    transformed_data = []
    for item in data:
        transformed_item = {
            'date': item[0]
        }
        transformed_data.append(transformed_item)
    return transformed_data



transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

def load_data(ds, **kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    with open('./data.csv', 'w') as f:
        for item in data:
            f.write(f"{item['date']}\n")

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract >> transform >> load