# Step-1: Import Libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from airflow.models import Variable
# Step-2: Define default arguments for tasks
default_args = {
    'owner': 'Nagesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
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
 
 
def fn_connect_to_postGres_db():
    conn = psycopg2.connect(
    database="rpt_awll1201", user='postgres', password='K8V6tOpEn0', host='172.16.20.117', port='5432')
    print("Connection established")
    cursor =conn.cursor()
    return cursor,conn
 
def sales_data_processing():
    cursor,conn = fn_connect_to_postGres_db()
    print("Db connected and cursor created")
    sales_order_start_date=Variable.get("sales_order_start_date")
    sales_order_end_date=Variable.get("sales_order_end_date")
    retailer_city_name=Variable.get("retailer_city_name")
    # retailer_name_value=Variable.get("retailer_name_value")
    product_type_value=Variable.get("product_type_value")
    columns = ['sales_order_date', 'retailer_city', 'product_type', 'base_quantity']
    columns_str = ", ".join(columns)
    final_columns = ['sales_order_date', 'retailer_city', 'product_type', 'base_quantity', 'base_quantity_sum']
 
    query = f"""
SELECT {columns_str}, SUM(base_quantity) as base_quantity_sum
FROM "AiMl_Adani".sales_data_feb24_to_may24
WHERE sales_order_date >= '{sales_order_start_date}'
  AND sales_order_date <= '{sales_order_end_date}'
  AND retailer_city = '{retailer_city_name}'
  AND retailer_name = '{retailer_name_value}'
  AND product_type = '{product_type_value}'
GROUP BY sales_order_date, retailer_city, retailer_name, product_type,base_quantity ORDER BY sales_order_date
"""
    print(query)
    cursor.execute(query)
    data = cursor.fetchall()
    print("data:::::",data)
    
 
# Creating a DataFrame
    train_df = pd.DataFrame(data, columns=final_columns)
 
# Display the DataFrame
    print(train_df.head())
    print(train_df.shape)
 
# Closing the connection
    conn.close()
    print('Connection closed')
 
# connect_to_postGres_db_task = PythonOperator(
#     task_id='connect_to_postGres_db_task',
#     python_callable=fn_connect_to_postGres_db,
#     dag=dag,
# )
 
# Defining Task
sales_data_processing = PythonOperator(
    task_id='sales_data_processing',
    python_callable=sales_data_processing,
    dag=dag,
)
 
# Set up the task dependencies
sales_data_processing
