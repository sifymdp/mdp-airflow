import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

db_params2 = {
    'host': '172.16.20.117',
    'database': 'testss',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}


def insert_data_to_postgres():
    connection2 = psycopg2.connect(**db_params2)
    cursor2 = connection2.cursor()
    insert = """INSERT INTO gainwell.tbl_sales_data
(transaction_type, invoice_type, distributor_id, distributor_name, retailer_name, salesman_name, invoice_date, sales_order_date, sku_name, product_brand, quantity, amount, transaction_id)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s);
               """
    query = """
             select 
transaction_type, 
invoice_type, 
distributor_id,
 distributor_name,
   retailer_name,
     salesman_name, 
     invoice_date,
       sales_order_date,
         sku_name, 
         product_brand,
           quantity,
             amount,
               transaction_id
 from gainwell.tb_sales_data tsd 
               """
    cursor2.execute(query)
    result = cursor2.fetchall()
    print("started inserting....")
    for row in result:
        transaction_type, invoice_type, distributor_id, distributor_name, retailer_name, salesman_name, invoice_date, sales_order_date, sku_name, product_brand, quantity, amount, transaction_id = row
        cursor2.execute(insert, (transaction_type, invoice_type, distributor_id, distributor_name, retailer_name,
                        salesman_name, invoice_date, sales_order_date, sku_name, product_brand, quantity, amount, transaction_id))
    print("inserted successfully")
    connection2.commit()
    cursor2.close()
    connection2.close()


dag = DAG(
    'insert_query_tbl_sales',
    schedule_interval=None,
    start_date=datetime(2023, 10, 5),
    catchup=False
)

insert_task = PythonOperator(
    task_id='insert_query',
    python_callable=insert_data_to_postgres,
    dag=dag
)
