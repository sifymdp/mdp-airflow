from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import compute_v1
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import json
from airflow.exceptions import AirflowException

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
    'gce_instance_metrics',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
)


def get_gce_instance_logs(**kwargs):
    # loading the metrics config file
    with open('/opt/airflow/secrets/cloud_metrics.json', 'r') as metric_file:
        config = json.load(metric_file)
    # Authenticating with credentials
    with open('/opt/airflow/secrets/google_cloud_default.json', 'r') as key_file:
        json_content = key_file.read()
    json_account_info = json.loads(json_content)
    credentials = service_account.Credentials.from_service_account_info(
        json_account_info)
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    # iterating for all the metrics mentioned in the json config
    all_instance_data = []
    for metric_config in config['metrics_gceinstances']:
        project_id = metric_config['project_id']
        zone = 'asia-south1-a'
        metric_type = metric_config["metric_name"]
        project_name = f"projects/{project_id}"
        # Define the resource type as gce requires this for filter
        resource_type = 'gce_instance'

        # the filter string
        filter_str = (f'metric.type="{metric_type}" AND '
                      f'resource.type="{resource_type}" AND '
                      f'resource.labels.zone="{zone}"')
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=10)
        interval = monitoring_v3.TimeInterval(
            {
                "end_time": end_time,
                "start_time": start_time,
            }
        )
        gce_instance_logs = client.list_time_series(
            name=project_name,
            filter=filter_str,
            interval=interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL
        )
        # print(gce_instance_logs)
        count = 0
        for ts in gce_instance_logs:
            # iterating the timeseries page and retrieving instance data
            instance_data = {
                'project_id': ts.resource.labels['project_id'],
                'zone': ts.resource.labels['zone'],
                'instance_id': ts.resource.labels['instance_id'],
                'instance_name': ts.metric.labels['instance_name'],
                'metric_type': ts.metric.type,
                'metric_kind': ts.metric_kind,
                'value_type': ts.value_type,
                'value': ""
            }
            # iterating points inside the instance as it had multiple points value
            # and to load that in instance data [points]
            points = []
            for point in ts.points:
                # value type int64 is mentioned as 2 and double as 3
                if ts.value_type == 2:
                    value = point.value.int64_value
                elif ts.value_type == 3:
                    value = point.value.double_value
                else:
                    value = None
                point_data = {
                    'value': value
                }
                # instead of appending all points data ..we can append max value of point
                points.append(point_data['value'])
                max_value = max(points)
                instance_data['value'] = max_value

                # after this..the max value of points data has to be found and compared with thershold
                # and then retrieved data have to be mapped
            # print("INSTANCE DATA:", instance_data)
            all_instance_data.append(instance_data)
    # print(all_instance_data)
    requests_data = []
    for data in all_instance_data:
        request = {
            "subject": "ISCA Anomaly",
            "item": {"name": data['metric_type']},
            "description": f"GCE Instance {data['instance_name']} with Instance Id:{data['instance_id']} has exceeded the thershold by {data['value']} for respective metric ",
            "category": {"name": metric_config['category_name']},
            "subcategory": {"name": metric_config['subcategory_name']},
            "status": {"name": "Open"},
            "template": {"name": "Default Request"},
            "request_type": {"name": "Incident"}
        }
        requests_data.append(request)
    request_data_json = json.dumps(requests_data)
    print(request_data_json)


get_metrics = PythonOperator(
    task_id='get_gce_instance_logs',
    python_callable=get_gce_instance_logs,
    provide_context=True,
    dag=dag,
)

get_metrics
