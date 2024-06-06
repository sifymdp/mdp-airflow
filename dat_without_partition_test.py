from datetime import datetime
import boto3
import pandas as pd
import time
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import logging
import os
import json
from collections import defaultdict
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def load_and_store_csv_to_s3(**context):

    # Connection ID in Airflow
    minio_conn_id = 'my_minio_connection'  
    minio_conn = BaseHook.get_connection(minio_conn_id)
    minio_endpoint_url = minio_conn.host
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    bucket = 'rawdata'
    folder = 'gainwell/'


   # Define the bucket and prefix where you want to store the partitioned CSV data
    bucket_name = 'cleaned'
    prefix1 = 'AWLL2/'

  
    timestamp_suffix = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
    prefix2 = f'{prefix1}{timestamp_suffix}/'

    # Read all files in the bucket
    s3_client = boto3.client('s3',
                            endpoint_url=minio_endpoint_url,
                            aws_access_key_id=minio_access_key,
                            aws_secret_access_key=minio_secret_key)
    
    response = s3_client.list_objects_v2(
        Bucket=bucket
    )
    print("Responce", response)
    cnt =0
    chunk_size = 20000
    

    # folder_path = os.path.join(minio_endpoint_url, bucket, folder)
    for obj in response['Contents']:
        log = obj['Key']
        print("Oj -> key", log)
        minio_csv_key = obj['Key'].split('/')[-1]
        s3_uri = f'{minio_endpoint_url}/{bucket}/{log}'
        chunks= pd.read_csv(s3_uri,chunksize=chunk_size)
        df_csv_new = pd.DataFrame()
        for df in chunks:
            print("file_processed",log)
            len_df= int(len(df)*0.05)
            airbyte_data = df['_airbyte_data']
            my_dict = defaultdict(list)
            check_dict={}
            count=0
            # for checking headers
            for data in airbyte_data:
                json_data = json.loads(data)
                for key, value in json_data.items():
                    if key not in check_dict:
                        check_dict[key]=1
                    else:
                        check_dict[key]+=1
                    count+=1
                    if (count==len_df):
                        break
            check_arr=[] 
            #keys
            print("lemght of dict::",len(check_dict))
            for check_dicts in check_dict.keys():
                if check_dict[check_dicts]>1:
                    # parent_key=check_dicts
                    check_arr.append(check_dicts)

            # appending the data
            for data in airbyte_data:
                json_data = json.loads(data)
                parent_list=[]        
                for key, value in json_data.items():
                    if key in check_arr:
                        my_dict[key].append(value)
                        parent_list.append(key)
                for missing_key in check_arr:
                    if missing_key not in parent_list:
                        my_dict[missing_key].append('0')
                #         key_latest= check_arr-parent_list
                #         my_dict[key_latest].append('null')
            
            df_chunk = pd.DataFrame(my_dict)
            # df_csv_new = df_csv_new.append(df_chunk, ignore_index=True)
            df_csv_new = pd.concat([df_csv_new, df_chunk], ignore_index=True)
            cnt+=1
            print("Basename ", os.path.splitext(log)[0])
            base_name = os.path.splitext(log)[0]
            file_name = base_name.split("/")[1].rstrip("0")
            print("File name", base_name.split("/")[1].rstrip("0"))
            output_file_name = f'{file_name}_Curated.csv'
            
            s3_client.put_object(Bucket=bucket_name,
                        Key=f'{prefix2}{output_file_name}',
                        Body=df_csv_new.to_csv(index=False))
            
dag1 = DAG('dat_without_partition_test',
          description='dag without partition_cleaned_test',
          schedule_interval=None,
          catchup=False,
          start_date=datetime.datetime(2023, 5, 17))

task1 = PythonOperator(
    task_id='dat_without_partition_test',
    python_callable=load_and_store_csv_to_s3,
    provide_context=True,
    dag=dag1
)

trigger_task = TriggerDagRunOperator(
    task_id='trigger_second_dag',
    trigger_dag_id='import_csv_to_postgresql_table_v01',
    dag=dag1
)

task1 >> trigger_task
