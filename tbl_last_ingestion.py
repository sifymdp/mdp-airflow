from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2


def delete_previous_month_data(**kwargs):
    current_date = datetime.now().date()
    

    if current_date.day == True:
        previous_month = current_date.replace(day=1) - timedelta(days=1)
        previous_of_previous_month = previous_month.replace(
            day=1) - timedelta(days=1)
        print(f"Deleting data for {previous_of_previous_month}")
    else:
        print("Current date is not the first day of the month. No data deletion needed.")


def execute_insert_query(**kwargs):
    print("Executing insert query for the previous day's data.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tbl_last_ingestion',
    default_args=default_args,
    description='Execute insert query for previous day data and delete previous of previous month data if current date is first of the month',
    schedule_interval='@daily',
    catchup=False
)

delete_data_task = PythonOperator(
    task_id='delete_previous_of_previous_month_data_task',
    python_callable=delete_previous_month_data,
    provide_context=True,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='execute_insert_query_task',
    python_callable=execute_insert_query,
    provide_context=True,
    dag=dag,
)

delete_data_task >> insert_data_task
