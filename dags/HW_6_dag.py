# Операторы в Airflow и их применение для ETL
# 1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать рандомное число и печатать его в
# консоль.

# 2. Создайте PythonOperator, который генерирует рандомное число, возводит его в квадрат и выводит в консоль исходное число и результат.

# 3. Сделайте оператор, который отправляет запрос к https://goweather.herokuapp.com/weather/"location" (вместо location используйте ваше местоположение).

# 4. Задайте последовательный порядок выполнения операторов.

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests

dag = DAG(
    'Sem6_HW',
    schedule_interval= '0 12 * * *' ,
    start_date=datetime(2024, 3, 25),  
    catchup=False,
)

# BashOperator, ãåíåðèðóåò ðàíäîìíîå ÷èñëî è ïå÷àòàåò åãî â êîíñîëü
generate_random_number = BashOperator(
    task_id='generate_random_number',
    bash_command='echo $((RANDOM % 100))',  # Ãåíåðèðóåò ñëó÷àéíîå ÷èñëî îò 0 äî 99
    dag=dag,
)

# PythonOperator ãåíåðèðóåò ðàíäîìíîå ÷èñëî, âîçâîäèò åãî â êâàäðàò è âûâîäèò â êîíñîëü èñõîäíîå ÷èñëî è ðåçóëüòàò
def quadrate_random_number():
    random_number = int(open('/tmp/random_number.txt').read())
    squared_number = random_number ** 2
    print(f"Random number: {random_number}, Squared: {squared_number}")

quadrate_random_number_task = PythonOperator(
    task_id='quadrate_random_number',
    python_callable=quadrate_random_number,
    provide_context=True,
    dag=dag,
)

# Ñîçäàåì îïåðàòîð, êîòîðûé îòïðàâëÿåò çàïðîñ î ïîãîäå
def fetch_weather():
    location = "Ìîñêâà"
    url = f"https://goweather.herokuapp.com/weather/{location}"
    response = requests.get(url)
    weather_data = response.json()
    print(f"Weather in {location}: {weather_data['temperature']}°C, {weather_data['description']}")

fetch_weather_task = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather,
    dag=dag,
)

# Çàäàåì ñëåäóþùèé ïîðÿäîê âûïîëíåíèÿ îïåðàòîðîâ:
generate_random_number >> square_random_number_task >> fetch_weather_task
