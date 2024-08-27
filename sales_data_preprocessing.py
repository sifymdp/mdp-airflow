from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from airflow.models import Variable
from itertools import product
import numpy as np

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
    'sales_data_preprocess_dag',
    default_args=default_args,
    description='Adani Sales data preprocessing DAG',
    schedule_interval=None,
    start_date=datetime(2024, 8, 11),
    catchup=False,
)

def fn_connect_to_postGres_db():
    conn = psycopg2.connect(
        database="rpt_awll1201", user='postgres', password='K8V6tOpEn0', host='172.16.20.117', port='5432')
    print("Connection established")
    cursor = conn.cursor()
    return cursor, conn

def fetch_data_from_db(**kwargs):
    cursor, conn = fn_connect_to_postGres_db()
    print("Db connected and cursor created")
    
    # Fetching data from Airflow Variables
    sales_order_start_date = Variable.get("sales_order_start_date")
    sales_order_end_date = Variable.get("sales_order_end_date")
    retailer_city_name = Variable.get("retailer_city_name")
    product_type_value = Variable.get("product_type_value")
    
    columns = ['sales_order_date', 'retailer_city', 'product_type', 'base_quantity']
    columns_str = ", ".join(columns)
    final_columns = ['sales_order_date', 'retailer_city', 'product_type', 'base_quantity', 'base_quantity_sum']

    query = f"""
    SELECT {columns_str}, SUM(base_quantity) as base_quantity_sum
    FROM "AiMl_Adani".sales_data_feb24_to_may24
    WHERE sales_order_date >= '{sales_order_start_date}'
      AND sales_order_date <= '{sales_order_end_date}'
      AND retailer_city = '{retailer_city_name}'
      AND product_type = '{product_type_value}'
    GROUP BY sales_order_date, retailer_city, product_type, base_quantity ORDER BY sales_order_date
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    
    # Creating a DataFrame
    train_df = pd.DataFrame(data, columns=final_columns)

    # Closing the connection
    conn.close()
    
    # Push DataFrame to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='train_df', value=train_df.to_dict())

    print('Data pushed to XCom and connection closed')

def process_data(**kwargs):
    ti = kwargs['ti']
    # Pulling the DataFrame from XCom
    train_df_dict = ti.xcom_pull(key='train_df', task_ids='fetch_data_from_db_task')
    train_df = pd.DataFrame.from_dict(train_df_dict)
    
    print('Data fetched from XCom:')
    print(train_df.info())
    print('Data preprocessing is in progress...')

    train_df['sales_order_date'] = pd.to_datetime(train_df['sales_order_date'], format='%d-%m-%Y')
    train_df = train_df.sort_values(by='sales_order_date')
    train_df.reset_index(drop=True, inplace=True)
    train_df.dropna(inplace=True)
    print(train_df.info())

    train_df['product_type'] = train_df['product_type'].str.split('-').str[1]
    train_df.reset_index(drop=True, inplace=True)
    train_df = train_df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    train_df.dropna(inplace=True)
    train_df = train_df.sort_values(by='sales_order_date')
    train_df.reset_index(drop=True, inplace=True)
    train_df['sales_order_date'] = pd.to_datetime(train_df['sales_order_date'])

    cities = train_df['retailer_city'].unique()
    products = train_df['product_type'].unique()

    min_date = train_df['sales_order_date'].min()
    max_date = train_df['sales_order_date'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')

    combinations = []
    for date, city, prod in product(all_dates, cities, products):
        combinations.append((date, city, prod))

    complete_df = pd.DataFrame(combinations, columns=['sales_order_date', 'retailer_city', 'product_type'])
    merged_df = complete_df.merge(train_df, on=['sales_order_date', 'retailer_city', 'product_type'], how='left')
    merged_df['base_quantity'] = merged_df['base_quantity'].fillna(0)

    print(merged_df)
    print(merged_df.isnull().sum())

    train_df = merged_df.groupby(['sales_order_date', 'retailer_city', 'product_type'])['base_quantity'].sum().reset_index(name='per_day_quantity')
    original_counts = train_df.copy()

    print('calculate per_day_quantity')
    original_counts['per_day_quantity'].sum()
    print(original_counts.shape)

    original_counts = original_counts.dropna(how='any', axis=0)
    print(original_counts.shape)

    original_counts['sales_order_date'] = pd.to_datetime(original_counts['sales_order_date'])
    original_counts.sort_values(by=['sales_order_date', 'retailer_city', 'product_type'], inplace=True)
    grouped = original_counts.groupby(['retailer_city', 'product_type'])

    def calculate_next_7_days_sum(group):
        group = group.set_index('sales_order_date')
        group['Next_7_days_count'] = group['per_day_quantity'].rolling('7D').sum().shift(-6)
        return group.reset_index()

    final_result = grouped.apply(calculate_next_7_days_sum).reset_index(drop=True)
    print('calculate_next_7_days_sum completed...')

    print(final_result.head(10))
    final_result['Next_7_days_count'] = final_result['Next_7_days_count'].fillna(0)
    print(final_result.isnull().sum())
    print(final_result.head())

    final_result = final_result.sort_values(by='sales_order_date')
    final_result.to_csv("adani_processed_data.csv", index=False)
    print('data exported to adani_processed_data.csv file...')
    print(final_result.shape)
    print('data preprocessing has been completed...')

# Define the tasks
fetch_data_from_db_task = PythonOperator(
    task_id='fetch_data_from_db_task',
    python_callable=fetch_data_from_db,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,  # This allows the task to receive **kwargs
    dag=dag,
)


# Set up task dependencies
fetch_data_from_db_task >> process_data_task

