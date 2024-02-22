from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import json
from airflow.exceptions import AirflowException
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime.today(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': ['managementairflow@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}


dag = DAG(
    'cloud_function_mail',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
)

# Function to monitor cloud function metrics


def get_cloud_function_logs():

    with open('/opt/airflow/secrets/cloud_metrics.json', 'r') as metric_file:
        config = json.load(metric_file)

    with open('/opt/airflow/secrets/google_cloud_default.json', 'r') as key_file:
        json_content = key_file.read()
    json_account_info = json.loads(json_content)

    credentials = service_account.Credentials.from_service_account_info(
        json_account_info)
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    all_exceeded_instances = []

    for metric_config in config['metrics_cloudfucntions']:
        # metrics from config
        project_id = metric_config['project_id']
        function_name = metric_config['function_name']
        threshold = metric_config['threshold']
        # interval for metrics
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=10)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": {"seconds": int(end_time.timestamp())},
                "start_time": {"seconds": int(start_time.timestamp())},
            }
        )

        metric_type = metric_config["metric_name"]
        project_name = f"projects/{project_id}"
        cloud_function_logs = client.list_time_series(
            name=project_name,
            filter=(f'metric.type="{metric_type}" '
                    f'AND resource.labels.function_name="{function_name}"'),
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        )
        exceeded_instances = []
        print(cloud_function_logs)
        for page in cloud_function_logs.pages:
            for time_series in page.time_series:
                for point in time_series.points:
                    value = point.value.int64_value
                    if value > threshold:

                        input_data = {
                            "request": {
                                "subject": "ISCA Anomaly",
                                "mode": {"name": "isca.helpdesk@timesgroup.com"},
                                "requester": {"name": "ISCA-PROD"},
                                "item": {"name": metric_type},
                                "subcategory": {"name": metric_config['subcategory_name']},
                                "status": {"name": "Open"},
                                "template": {"name": "Default Request"},
                                "request_type": {"name": "Incident"},
                                "description": f"The instance have exceeded the threshold for metric {metric_type}",
                                "category": {"name": metric_config['category_name']}
                            }
                        }
                        exceeded_instances.append({
                            # 'metric_type': metric_type,
                            # 'value': value,
                            # 'start_time': point.interval.start_time,
                            # 'end_time': point.interval.end_time
                            'data': input_data
                        })

        all_exceeded_instances.extend(exceeded_instances)

        print(all_exceeded_instances)
        # sent to db mongo as json

# exception for connection and null values
# map the retrieved data with the object given
    if all_exceeded_instances:
        exceeded_instances_json = json.dumps(all_exceeded_instances)
        # generate the new access token with the refresh token provided if access token expired
        # create api request to send data to the itsm tool
        raise AirflowException(
            f"The following instances have exceeded their thresholds:\n {exceeded_instances_json} \n")


cloud_function_logs = PythonOperator(
    task_id='send_cloud_function_metrics',
    python_callable=get_cloud_function_logs,
    dag=dag
)

cloud_function_logs
