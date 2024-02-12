from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import json
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': ['managementairflow@gmail.com', 'managementairflow@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}
dag = DAG(
    'test_gcp_monitoring_connection',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
)


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

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=5)

    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": int(end_time.timestamp())},
            "start_time": {"seconds": int(start_time.timestamp())},
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
    for page in result.pages:
        for time_series in page.time_series:
            for point in time_series.points:
                instance_count = point.value.int64_value
                start_time = point.interval.start_time
                end_time = point.interval.end_time
                if instance_count > 1:
                    print(start_time,end_time,instance_count)
                    raise AirflowException(f"The instance count ({instance_count}) has exceeded the threshold. "
                                           f"Start time: {start_time}, End time: {end_time}")


get_metrics = PythonOperator(
    task_id='get_cloud_function_metrics',
    python_callable=get_cloud_function_metrics,
    provide_context=True,
    dag=dag,
)

get_metrics
