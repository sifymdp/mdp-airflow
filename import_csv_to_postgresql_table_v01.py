import psycopg2
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import logging
import requests
import csv
from io import StringIO

default_args = {
    'start_date': datetime(2023, 6, 13, hour=18),
    'retries': 0,
}

dag = DAG(
    'import_csv_to_postgresql_table_v01',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

def import_csv_to_postgres():
    minio_conn_id = 'my_minio_connection'
    minio_conn = BaseHook.get_connection(minio_conn_id)
    minio_endpoint_url = minio_conn.host
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    minio_bucket_name = 'cleaned'
    archive_bucket_name = 'archive'
    # folder_structure = 'AWLL2/2023-06-01-05-33-23'
    # minio_csv_key = 'Curated_data (1).csv'

    postgres_conn_id = 'gainwell_postgre'
    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_host = pg_conn.host
    pg_port = pg_conn.port
    pg_user = pg_conn.login
    pg_password = pg_conn.password
    pg_database = pg_conn.schema
    # postgres_table = 'test_minio'

    # s3_uri = f'{minio_endpoint_url}/{minio_bucket_name}/{folder_structure}/{minio_csv_key}'
    # logging.info('path %s', s3_uri)

    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )

    response = s3_client.list_objects_v2(
        Bucket=minio_bucket_name
    )
    
    for obj in response['Contents']:
        log=obj['Key']
        print("log",log)
        minio_csv_key = obj['Key'].split('/')[-1]
        print("minio_csv_key",minio_csv_key)
        test = obj['Key'].split('/')[-1]
        s3_uri = f'{minio_endpoint_url}/{minio_bucket_name}/{log}'
        print("s3_uri",s3_uri)
        # print("loggggg",s3_uri)
        response = requests.get(s3_uri)
        csv_data = response.text

        csv_file = StringIO(csv_data) #data with header
        # print("csv_file_withheader",csv_file.getvalue())

        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            database=pg_database
        )

        with pg_conn.cursor() as cursor:
            print(minio_csv_key)
            if minio_csv_key in ['gainwellChild_Curated.csv', 'gainwellComplaint_Curated.csv','gainwellEquipment_Curated.csv','gainwellOpportunity_Curated.csv','gainwellPurchase_Curated.csv','gainwellUser_Curated.csv','SalesCustomerData_Curated.csv']:
                postgres_table = minio_csv_key.replace('.csv', '')
                print("before check")
                table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{postgres_table}')"
                print("After check")
                cursor.execute(table_exists_query)
                table_exists = cursor.fetchone()[0]
                if table_exists:
                    print("table exists")
                    truncate_query = f'TRUNCATE TABLE gainwell."{postgres_table}"'
                    cursor.execute(truncate_query)
                    print("truncatedone")
                    copy_query = f"COPY gainwell.\"{postgres_table}\" FROM STDIN WITH (FORMAT CSV, HEADER True, QUOTE '\"', ESCAPE '\"', NULL '\\N')"
                    # csv_file.seek(0)
                    # next(csv_file)
                    # print("csv_file_1",csv_file.getvalue())
                    cursor.copy_expert(copy_query, csv_file)
                    # # sending data to archival Bucket
                    s3_client.copy_object(Bucket=archive_bucket_name,
                              CopySource={'Bucket': minio_bucket_name, 'Key': log},
                              Key=log)

                    # Delete the processed data from the "rawdata" bucket
                    s3_client.delete_object(Bucket=minio_bucket_name, Key=log)
                    # cursor.copy_from(csv_file, postgres_table, sep=',', null='')
                else:
                    logging.info("Table doesn't exist")
            else:
                logging.info("File "+minio_csv_key+" is not available")

        pg_conn.commit()
        pg_conn.close()

with dag:
    task_import_csv = PythonOperator(
        task_id='import_csv',
        python_callable=import_csv_to_postgres,
        task_concurrency=1
    )
