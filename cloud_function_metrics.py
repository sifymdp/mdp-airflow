from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import json
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


dag = DAG(
    'cloud2_function_metrics',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
)

db_params = {
    'host': '172.16.20.117',
    'database': 'GCP_LOGS',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}


def connect_database(db_params):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    return connection, cursor


def insert_log_db(request_data_json, valuee):
    connection, cursor = connect_database(db_params)
    sql = """
    INSERT INTO logs.metric_logs (subject, item, description, category, subcategory, status, template_name, request_type,created_at,threshold_Reached)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
    """
    sqlj = """
    INSERT INTO logs.metric_logs (data, threshold_reached)
    VALUES (%s, %s)
    """

    # insert as json
    for data in request_data_json:
        json_data = json.dumps(data)  # Convert the dictionary to a JSON string
        # Execute the insertion query
        cursor.execute(sqlj, (json_data, valuee))

        # insert as columns
    # current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # for data in request_data_json:
    #     print("data:", data)
    #     subject = data['subject']
    #     item = data['item']['name']
    #     description = data['description']
    #     category = data['category']['name']
    #     subcategory = data['subcategory']['name']
    #     status = data['status']['name']
    #     template_name = data['template']['name']
    #     request_type = data['request_type']['name']

    #     # Execute the insertion query
    #     cursor.execute(sql, (subject, item, description, category,
    #                    subcategory, status, template_name, request_type, current_time, valuee))

# Commit the transaction and close the connection
    print("inserted successfully")
    connection.commit()
    cursor.close()
    connection.close()

# Function to monitor cloud function metrics


def get_cloud_function_logs():

    # with open('/opt/airflow/secrets/cloud_metrics.json', 'r') as metric_file:
    #     config = json.load(metric_file)

    with open('/opt/airflow/secrets/google_cloud_default.json', 'r') as key_file:
        json_content = key_file.read()
    json_account_info = json.loads(json_content)

    credentials = service_account.Credentials.from_service_account_info(
        json_account_info)
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    requests_data = []  # all the datas that are exceeded the threshold will be stored in this
    config = {
        "metrics_cloudfucntions": [
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/function/instance_count",
                "threshold": 1,
                "category_name": "getImage",
                "subcategory_name": "instance_count"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/function/network_egress",
                "threshold": 3025551,
                "category_name": "getImage",
                "subcategory_name": "network_egress"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/function/user_memory_bytes",
                "threshold": 194826239.0,
                "category_name": "getImage",
                "subcategory_name": "user_memory_bytes"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/function/execution_times",
                "threshold": 572406735.9999936,
                "category_name": "getImage",
                "subcategory_name": "execution_times"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/function/execution_count",
                "threshold": 62,
                "category_name": "getImage",
                "subcategory_name": "execution_count"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "function_name": "getImage",
                "metric_name": "cloudfunctions.googleapis.com/pending_queue/pending_requests",
                "threshold": 6,
                "category_name": "getImage",
                "subcategory_name": "pending_requests"
            }
        ],
        "metrics_gceinstances": [
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/network/received_bytes_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Received bytes"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/network/received_packets_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Received packets"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/network/sent_bytes_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Sent bytes"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/network/sent_packets_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Sent packets"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/memory/balloon/ram_used",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "VM Memory Used"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/memory/balloon/swap_in_bytes_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "VM Swap In"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/memory/balloon/swap_out_bytes_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "VM Swap Out"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/instance/cpu/utilization",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "CPU utilization"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/firewall/dropped_packets_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Dropped packets"
            },
            {
                "project_id": "prj-contentportal-test-389901",
                "metric_name": "compute.googleapis.com/firewall/dropped_bytes_count",
                "threshold": 2,
                "category_name": "ISCA_gce_instance",
                "subcategory_name": "Dropped bytes"
            }
        ]
    }

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
            subcategory_name = metric_config['subcategory_name']
            category_name = metric_config['category_name']
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
                    # Calculate the mean value from the distribution value for now
                    value = distribution_value.mean
                else:
                    value = None
                point_data = {
                    'value': value
                }
                # the result object has many points ..each points has different values for different time..
                # so,appending all the point values separately and storing the max value among them
                points.append(point_data['value'])
                max_value = max(points)
                # stored max value will be appended to data list
                instance_data['value'] = max_value
                instance_data['category_name'] = category_name
                instance_data['subcategory_name'] = subcategory_name
             # all the retrieved datas are in instance data list are checked for threshold reach ..
            # if reached ..they are stored in request_data for alert in mapped to given json structure
            if instance_data['value'] > threshold:
                request = {
                    "subject": "ISCA Anomaly",
                    "item": {"name": instance_data['metric_type']},
                    "description": f"The instance have exceeded the threshold by value:{instance_data['value']} for {instance_data['subcategory_name']} in {instance_data['function_name']}",
                    "category": {"name": instance_data['category_name']},
                    "subcategory": {"name": instance_data['subcategory_name']},
                    "status": {"name": "Open"},
                    "template": {"name": "Default Request"},
                    "request_type": {"name": "Incident"}
                }
                requests_data.append(request)
                # giving the last iterated value instead have to make it dynamic
                valuee = instance_data['value']
    # the datas are then stored as json datas for sending as api request
    request_data_json = json.dumps(requests_data)
    print("raw:", request_data_json)
    request_data_json = json.loads(request_data_json)
    insert_log_db(request_data_json, valuee)


# sent to db mongo as json
# exception for connection and null values
# map the retrieved data with the object given
cloud_function_logs = PythonOperator(
    task_id='get_cloud_function_metrics',
    python_callable=get_cloud_function_logs,
    dag=dag
)

cloud_function_logs
