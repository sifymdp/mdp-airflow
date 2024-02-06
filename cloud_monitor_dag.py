from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'test_gcp_monitoring_connection',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


def test_connection_task(**kwargs):
    # try:
    #     # Set up Google Cloud Monitoring client
    #     credentials = service_account.Credentials.from_service_account_file(
    #         '/opt/airflow/secrets/google_cloud_default.json',
    #         scopes=['https://www.googleapis.com/auth/monitoring.read']
    #     )
    #     client = monitoring_v3.MetricServiceClient(credentials=credentials)
    #     project_id = 'airflow-8080'
    #     client.list_monitored_resource_descriptors(
    #         name=f'projects/{project_id}')
    #     print("Connection to Google Cloud Monitoring API successful.")
    # except Exception as e:
    #     print(f"Error connecting to Google Cloud Monitoring API: {e}")
    try:
        # Specify the path to your service account key file
        key_file_path = '/opt/airflow/secrets/google_cloud_default.json'

        # Initialize the service account credentials
        creds = service_account.Credentials.from_service_account_file(
            key_file_path)

        # Check if the credentials are valid
        if not isinstance(creds, credentials.Credentials):
            print("Google Cloud credentials are invalid.")
            return False

        print("Connection to Google Cloud successful.")
        return True
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        return False

test_connection = PythonOperator(
    task_id='test_connection',
    python_callable=test_connection_task,
    provide_context=True,
    dag=dag,
)

test_connection
