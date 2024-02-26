from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import urllib
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen, Request
default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'email': ['managementairflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}
db_params = {
    'host': '172.16.20.117',
    'database': 'GCP_LOGS',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}


def retrieve_latest_access_token():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT access_token, expire_time FROM api_tokens.access_tokens ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            access_token, expiration_time = row
            print("tokens fetched")
            return access_token, expiration_time

        else:
            print("fetched none")
            return None, None

    except Exception as e:
        print("Error fetching access token:", e)
        return None, None

    finally:
        cursor.close()
        conn.close()


def update_access_token(new_access_token, expiration_time):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO api_tokens.access_tokens (access_token, expire_time) VALUES (%s, %s)",
                       (new_access_token, expiration_time))
        conn.commit()
        print("Successfully updated the token")
    except Exception as e:
        print("Error updating access token:", e)

    finally:
        cursor.close()
        conn.close()


def check_and_refresh_access_token():
    # retrieve the latest token from db
    access_token, expiration_time = retrieve_latest_access_token()
    #  checking the token for expiration
    if access_token and expiration_time:
        current_time = datetime.now()
        # if the token is valid..it is passed for sending api request
        if expiration_time >= current_time:
            print("Access token is still valid.")
            return access_token
        else:
            print("Access token has expired. Refreshing...")
    else:
        print("No access token found in the database. Refreshing new token...")
     # if not valid..new token will be generated

    with open('/opt/airflow/secrets/Api_token_config.json') as f:
        config = json.load(f)

    authentication_config = config.get('authentication', {})
    refresh_token = authentication_config.get('refresh_token')
    client_id = authentication_config.get('client_id')
    client_secret = authentication_config.get('client_secret')
    redirect_uri = authentication_config.get('redirect_uri')
    token_refresh_url = authentication_config.get('token_refresh_url')
    payload = {
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token',
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri
    }
    print("payload for refresh:", payload)
    try:
        response = requests.post(token_refresh_url, data=payload)
        new_token_data = response.json()
        new_access_token = new_token_data.get('access_token')
        expires_in = new_token_data.get('expires_in')
        print("Expires in:", expires_in)
        expiration_time = datetime.now() + timedelta(seconds=expires_in)
        print("Refreshed!")
        update_access_token(new_access_token, expiration_time)
        return new_access_token
    except Exception as e:
        print("Failed to refresh access token:", e)
        return None


def make_api_request(**kwargs):

    # Get the access token from the task instance context
    access_token = kwargs['access_token']
    with open('/opt/airflow/secrets/Api_token_config.json', 'r') as api_det:
        Req_det = json.load(api_det)

    # Extract API request details
    api_request_config = Req_det.get('api_request', {})
    endpoint_url = api_request_config.get('endpoint_url')
    headers = api_request_config.get('headers')

    # Populate Authorization header with access token
    headers['Authorization'] = f'Zoho-Oauthtoken {access_token}'
    print("payload for sending api", access_token,
          "\n", endpoint_url, "\n", headers)
    # Load the logs from the JSON file
    logs_file_path = '/opt/airflow/logs_gcp/gce_logs.json'
    with open(logs_file_path, 'r') as logs_file:
        logs_data = json.load(logs_file)
    # Iterate over each log entry and send a separate API request
    for log_entry in logs_data:
        print("LOG_ENTRY:", log_entry)
        input_data = {"request": log_entry}
        print("input_data::::", input_data)
        data = urlencode({"input_data": input_data}).encode()
        print("encodedddddddddd", data)
        # httprequest = Request(
        # url=endpoint_url, headers=headers, data=data, method="POST")
        # try:
        #     with urlopen(httprequest) as response:
        #         print("sent successfull", response.read().decode())
        # except HTTPError as e:
        #     print("errorrr", e.read().decode())

        # sending separate api request
        # response = requests.post(endpoint_url, json=payload, headers=headers)
        # if response.status_code == 200:
        #     print(f"{datetime.now()}: API Request successful.")
        # else:
        #     print(
        #         "API Request failed with status code {response.status_code}.")
        #     print(f"{datetime.now()}: Response content: {response.text}")


Api_connectivity_itsm = DAG(
    'Api_connectivity_itsm',
    default_args=default_args,
    # schedule_interval=timedelta(minutes=60)
)

task_check_and_refresh_access_token = PythonOperator(
    task_id='check_and_refresh_access_token',
    python_callable=check_and_refresh_access_token,
    dag=Api_connectivity_itsm
)

task_make_api_request = PythonOperator(
    task_id='make_api_request',
    python_callable=make_api_request,
    provide_context=True,
    # Provide context to access the return value of the previous task
    op_kwargs={
        'access_token': "{{ task_instance.xcom_pull(task_ids='check_and_refresh_access_token') }}"},
    dag=Api_connectivity_itsm
)

task_check_and_refresh_access_token >> task_make_api_request
