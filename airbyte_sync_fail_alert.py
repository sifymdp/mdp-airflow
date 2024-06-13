from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import psycopg2
import os
from tabulate import tabulate

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

db_params = {
    'host': '172.16.20.124',
    'database': 'db-airbyte',
    'user': 'airbyte',
    'password': 'airbyte'
}
dag = DAG(
    'airbyte_sync_fail_alert',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

def query_airbyte_logs():
    conn=psycopg2.connect(**db_params)
    cursor=conn.cursor()
    sql="""
            select c.name as "connection_name" ,j.id as "job_id" ,j.config_type, j.status as "job_status" from "connection" c
            left join jobs j on j."scope" = c."id"::text 
            where c.id ='ef461ddb-036c-46e3-905f-84b9d454b44c'
            order by j.updated_at desc limit 1
        """   
    cursor.execute(sql)
    res1 = cursor.fetchall() 
    conn.commit()
    cursor.close()
    conn.close()
    return res1
    

def send_email_alert(**kwargs):
    res= kwargs.get('ti').xcom_pull(task_ids='query_airbyte_sync_failure')
    res1=res[0]
    if res1[3]=="failed" or res1[3]=="Failed":
        print("resss::",res1)
        subject = "Latest Airbyte Sync Status:"
        body=f"""<table style="font-family: arial, sans-serif; border-collapse: collapse; width: 100%;">
        <tr>
            <th style="border: 1px solid #000000; text-align: left; padding: 8px;">Connection Name</th>
            <th style="border: 1px solid #000000; text-align: left; padding: 8px;">Job Id</th>
            <th style="border: 1px solid #000000; text-align: left; padding: 8px;">Config Type</th>
            <th style="border: 1px solid #000000; text-align: left; padding: 8px;">Sync Status</th>
        </tr>
        <tr>
            <td style="border: 1px solid #000000; text-align: left; padding: 8px;">{res1[0]}</td>
            <td style="border: 1px solid #000000; text-align: left; padding: 8px;">{res1[1]}</td>
            <td style="border: 1px solid #000000; text-align: left; padding: 8px;">{res1[2]}</td>
            <td style="border: 1px solid #000000; text-align: left; padding: 8px;">{res1[3]}</td>
    
            
        </tr>
    </table>
    
    """
        to = ['managementairflow@gmail.com','saisushmitha.rama@sifycorp.com']
    
        return EmailOperator(
            task_id='send_email',
            to=to,
            subject=subject,
            html_content=body,
            dag=dag,
        ).execute(context=None)
    else:
        print("No failed syncs found")

task_query = PythonOperator(
    task_id='query_airbyte_sync_failure',
    python_callable=query_airbyte_logs,
    dag=dag,
)

task_send_email = PythonOperator(
    task_id='send_email_alert',
    python_callable=send_email_alert,
    provide_context=True,
    dag=dag,
)

task_query >>task_send_email
