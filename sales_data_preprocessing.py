# Step-1: Import Libraries
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Step-2: Define default arguments for tasks
default_args = {
    'owner': 'Nagesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Step-3: Define DAG
dag = DAG(
    'sales_data_preprocess_dag',  # DAG ID with your name
    default_args=default_args,
    description='Adani Sales data preprocessing DAG',
    schedule_interval=None,   # timedelta(days=1),
    start_date=datetime(2024, 8, 11),
    catchup=False,
)

# Define the tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

def fn_connect_to_postGres_db():
    conn = psycopg2.connect(
    database="rpt_awll1201", user='postgres', password='K8V6tOpEn0', host='172.16.20.117', port='5432')
    print("Connection established")


connect_to_postGres_db_task = PythonOperator(
    task_id='connect_to_postGres_db_task',
    python_callable=fn_connect_to_postGres_db,
    dag=dag,
)


end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set up the task dependencies
start_task >> connect_to_postGres_db_task >> end_task
