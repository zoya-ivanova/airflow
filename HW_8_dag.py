# ETL: автоматизация подготовки данных (семинары)
# Специфика применения ETL в различных предметных сферах
# 1. Скачайте файлы boking.csv, client.csv и hotel.csv;
# 2. Создайте новый dag;
# 3. Создайте три оператора для получения данных и загрузите файлы. Передайте дата фреймы в оператор трансформации;
# 4. Создайте оператор который будет трансформировать данные:
# — Объедините все таблицы в одну;
# — Приведите даты к одному виду;
# — Удалите невалидные колонки;
# — Приведите все валюты к одной;
# 5. Создайте оператор загрузки в базу данных;
# 6. Запустите dag.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operator.postgres import PostgresOperator
import pandas as pd
import os
from datetime import datetime

def get_data(file_data):
    return pd.read_csv(file_data)

def transform_data(**kwargs):
    
    booking = kwargs['ti'].xcom_pull(task_ids = 'get_booking')
    client = kwargs['ti'].xcom_pull(task_ids = 'get_client')
    hotel = kwargs['ti'].xcom_pull(task_ids = 'get_hotel')

    # Удаляем строки Nan, задаем тип данных, приведём даты к одному виду
    booking.dropna(inplase=True)
    booking['booking_date'] = booking['booking_date'].str.replace('/', '-')
    hotel.dropna(inplase=True)
    client.dropna(inplase=True)
    client['age'] = client['age'].astype('int')

    # Объединяем все таблицы в одну, переименуем некоторые столбцы
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name' : 'client_name', 'type' : 'client_type'}, inplace = True)
    data = pd.merge(data, hotel, on = 'hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace = True)

    # Отредактируем невалидные колонки: заполним пропуски в возрасте средним значением
    data['age'].fillna(data['age'].mean(), inplace=True)
    data['age'] = data['age'].astype(int)
    
    # Пустое значение цены, также заполним средним по категории standard_1_bed
    data.booking_cost.fillna(data[data.room_type == 'standard_1_bed'].booking_cost.mean(), inplace=True)
    
    # В пустое значение валюты поставим валюту категории standard_1_bed
    data.loc[data.room_type == 'standard_1_bed', ['currency']] = 'GBP'
    
    # Приведите все валюты к одной (на момент анализа 1 EUR = 0.86 GBP)
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.86
    data.currency.replace('EUR', 'GBP', inplace=True)
    data.to_csv('/home/zo/airflow/data/data.csv', index=False)
    
dag = DAG('data_processing_dag', description='DAG for processing and loading data',
          schedule_interval=None, start_date=datetime(2024, 4, 1))

get_booking = PythonOperator(task_id='get_booking', python_callable=get_data, op_args=['/home/zo/airflow/data/booking.csv'], dag=dag)
get_hotel = PythonOperator(task_id='get_hotel', python_callable=get_data, op_args=['/home/zo/airflow/data/hotel.csv'], dag=dag)
get_client = PythonOperator(task_id='get_client', python_callable=get_data, op_args=['/home/zo/airflow/data/client.csv'], dag=dag)
transform_data_task = PythonOperator(task_id='transform_data_task', python_callable=transform_data, dag=dag)
create_table_postgres = PostgresOperator(task_id='create_data_table', 
                                         sql = """CREATE TABLE IF NOT EXISTS data(
                                         booking_date DATE,
                                         client_id INT NOT NULL,
                                         client_name VARCHAR(30),
                                         age INT
                                         client_type TEXT,
                                         hotel_id INT NOT NULL,
                                         name_hotel VARCHAR(30),
                                         room_type TEXT NOT NULL,
                                         booking_cost NUMERIC,
                                         currency VARCHAR(5));""",
                                         postgres_conn_id='pg_conn',
                                         database='airflow')

load_to_postgres_db = PythonOperator(task_id='load_to_postgres_db', postgres_conn_id='pg_conn',
                                     sql="""COPY data FROM '/home/zo/airflow/data/data.csv' WITH CSV HEADER; """)                                
get_booking >> transform_data_task                                      
get_hotel >> transform_data_task 
get_client >> transform_data_task 
transform_data_task >> create_table_postgres >> load_to_postgres_db

if _naime_ == '_main_':
    dag.cli()
