from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account
from google.cloud import compute_v1
import json
import time
from airflow.exceptions import AirflowException


default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': ['managementairflow@gmail.com', 'duraisiva1905@gmail.com', 'parukrish21@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}
dag = DAG(
    'test_gcp_monitoring_connection',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


def test_connection_task(**kwargs):
    with open('/opt/airflow/secrets/google_cloud_default.json', 'r') as file:
        json_content = file.read()
    json_account_info = json.loads(json_content)
    credentials = service_account.Credentials.from_service_account_info(
        json_account_info)
    client = compute_v1.InstancesClient(credentials=credentials)
    project = 'prj-contentportal-test-389901'
    zone = 'asia-south1-a'
    instances = client.list(project=project, zone=zone)
    for instance in instances:
        print("Instance Name:", instance.name)
        print("Instance ID:", instance.id)
        print("Machine Type:", instance.machine_type)
        print("Zone:", instance.zone)
        print("Status:", instance.status)
        print("-----------------------")


def get_cloud_function_metrics(**kwargs):
    with open('/opt/airflow/secrets/google_cloud_default.json', 'r') as file:
        json_content = file.read()
    json_account_info = json.loads(json_content)
    credentials = service_account.Credentials.from_service_account_info(
        json_account_info)
    project_id = 'prj-contentportal-test-389901'
    function_name = 'getImage'
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    project_name = f"projects/{project_id}"

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 1000200), "nanos": nanos},
        }
    )
    metric_type = 'cloudfunctions.googleapis.com/function/instance_count'
    result = client.list_time_series(
        name=project_name,
        filter=(f'metric.type="{metric_type}" '
                f'AND resource.labels.function_name="{function_name}"'),
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )
    InstanceCount = []
    for page in result.pages:
        for time_series in page.time_series:
            print(time_series.metric.type)
            for point in time_series.points:
                print("Start time:", point.interval.start_time)
                print("End time:", point.interval.end_time)
                print("InstanceCountValue:", point.value.int64_value)
                print("---------------------")
                InstanceCount.append(point.value.int64_value)
    if max(InstanceCount) > 1:
        print(max(InstanceCount))
        task_fails(InstanceCount)


def task_fails(InstanceCount):
    raise AirflowException("The instance count has exceeded")


# test_connection = PythonOperator(
#     task_id='test_connection',
#     python_callable=test_connection_task,
#     provide_context=True,
#     dag=dag,
# )
get_metrics = PythonOperator(
    task_id='get_cloud_function_metrics',
    python_callable=get_cloud_function_metrics,
    provide_context=True,
    dag=dag,
)

get_metrics
