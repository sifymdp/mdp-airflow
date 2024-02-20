from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
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
            "SELECT access_token, expire_time FROM api_tokens.access_tokens ORDER BY created_time DESC LIMIT 1")
        row = cursor.fetchone()
        if row:
            access_token, expiration_time = row
            return access_token, expiration_time
        else:
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

    except Exception as e:
        print("Error updating access token:", e)

    finally:
        cursor.close()
        conn.close()


def check_and_refresh_access_token():
    access_token, expiration_time = retrieve_latest_access_token()

    if access_token and expiration_time:
        current_time = datetime.now()
        if expiration_time >= current_time:
            print("Access token is still valid.")
            return access_token
        else:
            print("Access token has expired. Refreshing...")
    else:
        print("No access token found in the database. Refreshing...")
    response = requests.post(
        'http://localhost:3000/refresh-token')
    if response.status_code == 200:
        data = response.json()
        new_access_token = data['access_token']
        expiration_time = datetime.now(
        ) + timedelta(seconds=data['expires_in'])
        update_access_token(new_access_token, expiration_time)
        return new_access_token
    else:
        print("Failed to refresh access token.")
        return None


def make_api_request(access_token):
    payload = {
        'request': {
            'subject': 'ISCA Grafan API test'
        }
    }
    response = requests.post(
        'http://localhost:3000/create-request', json=payload)
    if response.status_code == 200:
        print("API Request successful.")
    else:
        print("API Request failed:", response.text)


Api_connectivity = DAG(
    'Api_connectivity',
    default_args=default_args,
    schedule_interval=timedelta(minutes=60)
)

task_check_and_refresh_access_token = PythonOperator(
    task_id='check_and_refresh_access_token',
    python_callable=check_and_refresh_access_token,
    dag=Api_connectivity
)

task_make_api_request = PythonOperator(
    task_id='make_api_request',
    python_callable=make_api_request,
    provide_context=True,  # Provide context to access the return value of the previous task
    dag=Api_connectivity
)

task_check_and_refresh_access_token >> task_make_api_request
