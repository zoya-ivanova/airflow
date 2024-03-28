from airflow import DAG
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
import requests

# location = "Москва"
#     url = f"https://goweather.herokuapp.com/weather/{location}"
#     response = requests.get(url)
#     weather_data = response.json()
#     print(f"Weather in {location}: {weather_data['temperature']}°C, {weather_data['description']}")

OPENWEATHER_KEY = Variable.get("secret_openweather_key") 
# URL = f'http://api.openweathermap.org/geo/1.0/direct?q=Cairo&limit=5&appid={OPENWEATHER_KEY}' 
URL_OPENWEATHER = f'http://api.openweathermap.org/geo/2.5/weather?q=London&limit=5&appid={OPENWEATHER_KEY}'

def choosing_description_weather(ti):
    current_temp = ti.xcom_pull(task_ids='get_temperature')
    if current_temp > 15:
        return 'warm_branch'
    return 'cold_branch'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 28),
    'retries': 1
}

dag = DAG(
    dag_id='get_temp_from_openweather',
    default_args=default_args,
    schedule_interval=None
)

get_response = HttpOperator(
    task_id='get_temperature',
    method='GET',
    http_conn_id='openweather',
    endpoint=URL_OPENWEATHER,
    response_filter=lambda response: response.json()["main"]["temp"],
    headers={},
    dag=dag
)

choosing_description = BranchPythonOperator(
    task_id='choosing_result',
    python_callable=choosing_description_weather,
    dag=dag
)

warm_branch_task = PythonOperator(
    task_id='warm_branch',
    python_callable=lambda ti: print(f'ТЕПЛО: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    dag=dag
)

cold_branch_task = PythonOperator(
    task_id='cold_branch',
    python_callable=lambda ti: print(f'ХОЛОДНО: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    dag=dag
)


get_response >> choosing_description
choosing_description >> warm_branch_task
choosing_description >> cold_branch_task
