from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import psycopg2
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 19),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

db_params = {
    'host': '172.16.20.123',
    'database': 'db-airbyte',
    'user': 'airbyte',
    'password': 'airbyte'
}
dag = DAG(
    'airbyte_sync_alert',
    default_args=default_args,
    description='Send email alert for number of records inserted during Airbyte sync',
    schedule_interval=timedelta(days=1),
)

def query_airbyte_logs():
    conn=psycopg2.connect(**db_params)
    cursor=conn.cursor()
    sql="""select 
    ss.stream_namespace,
    ss.stream_name,
    ss.records_emitted,
    ss.records_committed,
    ss.updated_at AS "time",
    st.job_type,
    st.run_state
FROM
    stream_stats ss
    left JOIN
    stream_statuses st ON 
    ss.attempt_id in (select id from attempts a where job_id IN (select j.id  from jobs j where scope='b183121f-e258-4852-bfd2-a32a3d8c6200' order by updated_at desc limit 1))
    AND st.job_id in (select j.id  from jobs j where scope='b183121f-e258-4852-bfd2-a32a3d8c6200' order by updated_at desc limit 1)
    order by ss.updated_at desc limit 1
        """   
    sqll="""     select job_type,run_state from public.stream_statuses ss where job_id in (select j.id  from jobs j where scope='b183121f-e258-4852-bfd2-a32a3d8c6200' order by updated_at desc limit 1) and connection_id in ('b183121f-e258-4852-bfd2-a32a3d8c6200')
"""
    res1=cursor.execute(sql)
    res2=cursor.execute(sqll)
    # print(res[0],res[1],res[2],res[3])
    print("result:::::",res1,"res22:::",res22)

    conn.commit()
    cursor.close()
    conn.close()
    # subject = "Latest Airbyte Sync Record"
    # body = f"Records Emitted: {res[2]}\nConnection Name: {res[0]}\nStream Name: {res[1]}\nDate: {res[4]}"
    # to = ['managementairflow@gmail.com']

    # return EmailOperator(
    #     task_id='send_email',
    #     to=to,
    #     subject=subject,
    #     html_content=body,
    #     dag=dag,
    # ).execute(context=None)

def send_email_alert(res):
    subject = "Latest Airbyte Sync Record"
    body = f"Records Emitted: {res[2]}\nConnection Name: {res[0]}\nStream Name: {res[1]}\nDate: {res[4]}"
    to = ['managementairflow@gmail.com']

    return EmailOperator(
        task_id='send_email',
        to=to,
        subject=subject,
        html_content=body,
        dag=dag,
    ).execute(context=None)

task_query = PythonOperator(
    task_id='query_airbyte_logs',
    python_callable=query_airbyte_logs,
    dag=dag,
)

# task_send_email = PythonOperator(
#     task_id='send_email_alert',
#     python_callable=send_email_alert,
#     provide_context=True,
#     dag=dag,
# )

task_query 
