from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook


db_params = {
    'host': '172.16.20.117',
    'database': 'rpt_awll1201',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}


salesman_query = """
select
        distributor_id,
        distributor_code,
        distributor_name,
        salesman_id,
        salesman_code,
        salesman_name,
        SUM(a.net_amnt) :: numeric - SUM(a.ret_amount) :: numeric AS salesman_wise_sales,
        concat(invoice_year,'-',invoice_month) as invoice_date 
    FROM
        (
            SELECT
                ROUND(
                    SUM(net_amount) :: numeric - SUM(invoice_level_discount) :: numeric,
                    4
                ) AS net_amnt, 0 AS ret_amount,
                distributor_id, distributor_code, distributor_name,
                salesman_id,salesman_code, salesman_name,
               EXTRACT(YEAR FROM invoice_date) AS invoice_year,
    		   EXTRACT(MONTH FROM invoice_date) AS invoice_month
            FROM
               rpt_awll1201.tbl_sales_data
            WHERE
                transaction_type = 'Sales' AND invoice_date >= '2023-07-01' AND invoice_date <= '2023-07-31'
            GROUP by
            distributor_id, distributor_code,distributor_name,
                salesman_id, salesman_code, salesman_name,
                invoice_year,invoice_month 
            UNION ALL
             SELECT
                ROUND(
                    SUM(net_amount) :: numeric - SUM(invoice_level_discount) :: numeric,
                    4
                ) AS net_amnt, 0 AS ret_amount,
                distributor_id, distributor_code, distributor_name,
                salesman_id,salesman_code, salesman_name,
               EXTRACT(YEAR FROM invoice_date) AS invoice_year,
    		   EXTRACT(MONTH FROM invoice_date) AS invoice_month
            FROM
               rpt_awll1201.tbl_sales_data
            WHERE
                transaction_type = 'Sales Return' AND invoice_date >= '2023-07-01' AND invoice_date <= '2023-07-31'
            GROUP by
            distributor_id, distributor_code,distributor_name,
                salesman_id, salesman_code, salesman_name,
                invoice_year,invoice_month 
        ) AS a
    GROUP by
        distributor_id, distributor_code,distributor_name,
                salesman_id, salesman_code, salesman_name,
                invoice_year,invoice_month;
        

"""

beat_query = """
    select
        distributor_id,
        distributor_code,
        distributor_name,
        beat_id,
        beat_code,
        beat_name,
        SUM(a.net_amnt) :: numeric - SUM(a.ret_amount) :: numeric AS beat_wise_sales,
        concat(invoice_year,'-',invoice_month) as date 
    FROM
        (
            SELECT
                ROUND(
                    SUM(net_amount) :: numeric - SUM(invoice_level_discount) :: numeric,
                    4
                ) AS net_amnt, 0 AS ret_amount,
                distributor_id, distributor_code, distributor_name,
                beat_id, beat_code, beat_name,
               EXTRACT(YEAR FROM invoice_date) AS invoice_year,
    		   EXTRACT(MONTH FROM invoice_date) AS invoice_month
            FROM
               rpt_awll1201.tbl_sales_data
            WHERE
                transaction_type = 'Sales' AND invoice_date >= '2023-07-01' AND invoice_date <= '2023-07-31'
            GROUP by
            distributor_id, distributor_code,distributor_name,
                beat_id, beat_code, beat_name,
                invoice_year,invoice_month 
            UNION ALL
             SELECT
                ROUND(
                    SUM(net_amount) :: numeric - SUM(invoice_level_discount) :: numeric,
                    4
                ) AS net_amnt, 0 AS ret_amount,
                distributor_id, distributor_code, distributor_name,
                beat_id, beat_code, beat_name,
               EXTRACT(YEAR FROM invoice_date) AS invoice_year,
    		   EXTRACT(MONTH FROM invoice_date) AS invoice_month
            FROM
               rpt_awll1201.tbl_sales_data
            WHERE
                transaction_type = 'Sales Return' AND invoice_date >= '2023-07-01' AND invoice_date <= '2023-07-31'
            GROUP by
            distributor_id, distributor_code,distributor_name,
                beat_id, beat_code, beat_name,
                invoice_year,invoice_month 
        ) AS a
    GROUP by
        distributor_id, distributor_code,distributor_name,
                beat_id, beat_code, beat_name,
                invoice_year,invoice_month;
"""


def execute_sql_query(sql_query):

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    result = cursor.fetchall()
    conn.close()
    return result


def merge_results(**kwargs):
    ti = kwargs['ti']

    # Retrieve results of both queries
    salesman_results = ti.xcom_pull(task_ids='execute_salesman_query')
    beat_results = ti.xcom_pull(task_ids='execute_beat_query')
    # Convert results to Pandas DataFrames
    df_salesman = pd.DataFrame(salesman_results, columns=[
                               'distributor_id', 'distributor_name', 'distributor_code', 'salesman_id', 'salesman_name', 'salesman_code', 'salesman_wise_sales', 'invoice_date'])

    df_beat = pd.DataFrame(beat_results, columns=[
                           'distributor_id', 'distributor_name', 'distributor_code', 'beat_id', 'beat_name', 'beat_code', 'beat_wise_sales', 'date'])
    df_beat.drop(columns=[
        'distributor_name', 'distributor_code', 'date'
    ], inplace=True)
    # Merge DataFrames on distributor_id
    merged_df = pd.merge(df_salesman, df_beat,
                         on='distributor_id', how='left')
    post_conn = 'postgres_sql'
    conn = BaseHook.get_connection(post_conn)

    schema_name = 'forum'
    table_name = 'salesmanbeat'
    engine = create_engine(
        f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    print(f"Engine URL: {engine.url}")
    con = engine.connect()
    merged_df = merged_df[['distributor_id', 'distributor_code', 'distributor_name', 'salesman_id', 'salesman_code',
                           'salesman_name', 'salesman_wise_sales', 'invoice_date', 'beat_id', 'beat_code', 'beat_name', 'beat_wise_sales']]
    print("this the list of columns in result merged/n", merged_df.columns)
    
    try:
        merged_df.to_sql(table_name, con, if_exists='append',
                         index=False, method='multi', schema=schema_name, )
        con.commit()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Error: {e}")

    print("merged")
    print(merged_df)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 1),
    'retries': 3,
}

dag = DAG(
    'combine_salesman_beat_data',
    default_args=default_args,
    schedule_interval=None,
)

execute_salesman_query = PythonOperator(
    task_id='execute_salesman_query',
    python_callable=execute_sql_query,
    op_args=[salesman_query],
    provide_context=True,
    dag=dag,
)

execute_beat_query = PythonOperator(
    task_id='execute_beat_query',
    python_callable=execute_sql_query,
    op_args=[beat_query],
    provide_context=True,
    dag=dag,
)

merge_results_task = PythonOperator(
    task_id='merge_results',
    python_callable=merge_results,
    provide_context=True,
    dag=dag,
)


execute_salesman_query >> merge_results_task
execute_beat_query >> merge_results_task
