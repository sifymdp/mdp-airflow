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
    'email_on_failure': False,
    'email_on_retry': False
}


dag = DAG(
    'cloud2_function_metrics',
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
    all_instance_data = []
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
        for ts in cloud_function_logs:
            instance_data = {
                'project_id': ts.resource.labels['project_id'],
                'zone': ts.resource.labels['region'],
                'function_name': ts.resource.labels['function_name'],
                'metric_type': ts.metric.type,
                'value_type': ts.value_type,
                'value': ""
            }
            points = []
            for point in ts.points:
                # value type int64 is mentioned as 2 and double as 3
                if ts.value_type == 2:  # int64
                    value = point.value.int64_value
                elif ts.value_type == 5:  # distribution
                    distribution_value = point.value.distribution_value
                    # Calculate the mean value from the distribution value
                    value = distribution_value.mean
                else:
                    value = None
                point_data = {
                    'value': value
                }

                points.append(point_data['value'])
                max_value = max(points)
                instance_data['value'] = max_value
            all_instance_data.append(instance_data)
    # print(all_instance_data)
    requests_data = []
    for data in all_instance_data:
        if data['value'] > threshold:
            request = {
                "subject": "ISCA Anomaly",
                "item": {"name": data['metric_type']},
                "description": f"The instance have exceeded the threshold by value:{data['value']} for {metric_config['subcategory_name']} in {data['function_name']}",
                "category": {"name": metric_config['category_name']},
                "subcategory": {"name": metric_config['subcategory_name']},
                "status": {"name": "Open"},
                "template": {"name": "Default Request"},
                "request_type": {"name": "Incident"}
            }
            # print(request, "\n")
            requests_data.append(request)
    request_data_json = json.dumps(requests_data)
    print(request_data_json)


# sent to db mongo as json
# exception for connection and null values
# map the retrieved data with the object given


cloud_function_logs = PythonOperator(
    task_id='get_cloud_function_metrics',
    python_callable=get_cloud_function_logs,
    dag=dag
)

cloud_function_logs
