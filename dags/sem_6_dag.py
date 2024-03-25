# «адание 1: —оздайте новый граф, добавьте в него два BashOperator, первый должен 
# выводить на экран сообщение
# УHello from AirflowФ, второй должен брать код из bash файла. —оздайте bash файл 
# который будет выводить
# сообщение УHello from Airflow bash script processer.Ф
# —оздайте последовательную св€зь между первым и вторым оператором.
# ƒобавьте между первым и вторым операторами еще один BashOperator который будет 
# заканчивать свою работу в
# статусе skipped.    

from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG( 'hello_world' , description= 'Hello World DAG' ,
schedule_interval= '0 12 * * *' ,
start_date=datetime( 2023 , 1 , 1
), catchup= False )

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag)
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task',
bash_command='./home/airflow/airflow/dags/scripts/file1.sh', dag=dag)
hello_operator >> skipp_operator >> hello_file_operator    
