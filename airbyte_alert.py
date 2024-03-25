from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import psycopg2
import os
from tabulate import tabulate

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
    cursor.execute(sql)
    res1 = cursor.fetchall() 
    cursor.execute(sqll) 
    res2=cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return res1,res2
    

def send_email_alert(**kwargs):
    res1,res2 = kwargs.get('ti').xcom_pull(task_ids='query_airbyte_logs')
    res1_t = res1[0]
    res2_t = res2[0]
    headers=["Stream namespace","Stream name","Records emitted","Records committed","Datetime","Job Type","Run State"]
    data=[[res1_t[0],res1_t[1],res1_t[2],res1_t[3],res1_t[4],res2_t[0],res2_t[1]]]
    table=tabulate(data,headers,tablefmt="grid")
    subject = "Latest Airbyte Sync Record"
    # body = f"Stream namespace: {res1_t[0]}\n Stream name: {res1_t[1]}\nRecords emitted: {res1_t[2]}\nRecords committed: {res1_t[3]}\nDatetime: {res1_t[4]}\nJob Type:{res2_t[0]}\nRun State:{res2_t[1]}"
    body=f"""<table style="border: 1px solid black;">
    <tr style="border: 1px solid black;>
        <th>Stream namespace</th>
        <th>Stream name</th>
        <th>Records emitted</th>
        <th>Records committed</th>
        <th>Datetime</th>
        <th>Job Type</th>
        <th>Run State</th>
    </tr>
    <tr style="border: 1px solid black;>
        <td>{res1_t[0]}</td>
        <td>{res1_t[1]}</td>
        <td>{res1_t[2]}</td>
        <td>{res1_t[3]}</td>
        <td>{res1_t[4]}</td>
        <td>{res2_t[0]}</td>
        <td>{res2_t[0]}</td>
    </tr>
</table>
"""
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

task_send_email = PythonOperator(
    task_id='send_email_alert',
    python_callable=send_email_alert,
    provide_context=True,
    dag=dag,
)

task_query >>task_send_email
