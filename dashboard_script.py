from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2,datetime
import pandas as pd

db_params = {
    'host': '172.16.20.117',
    'database': 'testss',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}

dag=DAG(
    dag_id='dashboard_script',
    schedule=None,
    start_date=datetime(2024, 6, 12),
)

def connect_database(db_params):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    return connection, cursor


def close_connections(connection, cursor):
    cursor.close()
    connection.close()

def update_db_excel():
    connection, cursor = connect_database(db_params)
    file='excel path'
    data=pd.read_excel(file)
    table="testEmami"
    column1=""
    column2=""
    query=f" insert into public.testEmami (select * from public.video_game_sales order by RANDOM() limit 10)"
    cursor.execute(query)
    connection.commit()
    # for index, row in data.iterrows():

    #     query=f"UPDATE {table} SET column1={row[column1]}, column2={row[column2]}"
    #     cursor.execute(query)
    #     connection.commit()
    print("Db updated..")
    close_connections(connection,cursor)

update_db=PythonOperator(
    task_id='update_db_excel',
    python_callable=update_db_excel
)

update_db
