# Step-1: Import Libraries
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
 
def fetch_data_from_db():
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
  AND product_type = '{product_type_value}'
GROUP BY sales_order_date, retailer_city, product_type,base_quantity ORDER BY sales_order_date
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
    return train_df

    print('data preprocessing is in progress...')
    data_processing(train_df)
 

def data_processing(train_df):
    print(train_df.info())

    train_df['sales_order_date'] = pd.to_datetime(train_df['sales_order_date'], format='%d-%m-%Y')
    train_df = train_df.sort_values(by='sales_order_date')
    train_df.reset_index(drop=True, inplace=True)
    train_df.dropna(inplace=True)
    print(train_df.info())

    # Extract only names from product_type and remove index
    train_df['product_type'] = train_df['product_type'].str.split('-').str[1]
    train_df.reset_index(drop=True, inplace=True)

    # Convert all values to lowercase
    train_df = train_df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    
    # Remove all rows with null values
    train_df.dropna(inplace=True)

    # Order the DataFrame by the sales_order_date column
    train_df = train_df.sort_values(by='sales_order_date')

    # Reset index after dropping rows
    train_df.reset_index(drop=True, inplace=True)

    # Ensure sales_order_date is datetime
    train_df['sales_order_date'] = pd.to_datetime(train_df['sales_order_date'])

    # Define the unique values for each dimension
    # states = train_df['seller_state'].unique()
    cities = train_df['retailer_city'].unique()
    products = train_df['product_type'].unique()

    # Define a mapping from cities to states
    # city_state_map = train_df[['retailer_city', 'seller_state']].drop_duplicates().set_index('retailer_city').to_dict()['seller_state']

    # Generate a complete date range for each month and year present in the data
    min_date = train_df['sales_order_date'].min()
    max_date = train_df['sales_order_date'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')

    # Generate all combinations of date, city, and product, ensuring city-state mapping
    combinations = []
    for date, city, product in product(all_dates, cities, products):
        # state = city_state_map.get(city)
        # if state:
        combinations.append((date, city, product))

    # Create a DataFrame from all combinations
    complete_df = pd.DataFrame(combinations, columns=['sales_order_date', 'retailer_city', 'product_type'])

    # Merge with the original DataFrame to fill missing records with base_quantity as 0
    merged_df = complete_df.merge(train_df, on=['sales_order_date', 'retailer_city', 'product_type'], how='left')
    merged_df['base_quantity'] = merged_df['base_quantity'].fillna(0)

    # Display the resulting DataFrame
    print(merged_df)

    print(merged_df.isnull().sum())

    # Group the data by 'sales_order_date', 'seller_state', 'retailer_city', 'product_type' then sum occurrences
    train_df = merged_df.groupby(['sales_order_date', 'retailer_city', 'product_type'])['base_quantity'].sum().reset_index(name='per_day_quantity')
    train_df.head()

    original_counts=train_df.copy()

    original_counts['per_day_quantity'].sum()
    print(original_counts.shape)

    original_counts = original_counts.dropna(how='any',axis=0) 
    print(original_counts.shape)

    # Convert sales_order_date to datetime
    original_counts['sales_order_date'] = pd.to_datetime(original_counts['sales_order_date'])

    # Sort by required columns to ensure proper processing order
    original_counts.sort_values(by=['sales_order_date', 'retailer_city', 'product_type'], inplace=True)

    # Group by the necessary columns
    grouped = original_counts.groupby(['seller_state', 'retailer_city', 'product_type'])

    # Function to calculate the rolling sum for the next 7 days
    def calculate_next_7_days_sum(group):
        group = group.set_index('sales_order_date')
        group['Next_7_days_count'] = group['per_day_quantity'].rolling('7D').sum().shift(-6)
        return group.reset_index()

    # Apply the function to each group and concatenate the results
    final_result = grouped.apply(calculate_next_7_days_sum).reset_index(drop=True)

    # Show the final result
    print(final_result.head(10))

    final_result['Next_7_days_count'] = final_result['Next_7_days_count'].fillna(0)

    print(final_result.isnull().sum())

    print(final_result.head())

    final_result = final_result.sort_values(by='sales_order_date')

    final_result.to_csv("adani_processed_data.csv", index=False)


# Defining Task
fetch_data_from_db = PythonOperator(
    task_id='fetch_data_from_db',
    python_callable=fetch_data_from_db,
    dag=dag,
)

# # Defining Task
# data_processing = PythonOperator(
#     task_id='data_processing',
#     python_callable=data_processing,
#     dag=dag,
# )
 
# Set up the task dependencies
fetch_data_from_db

