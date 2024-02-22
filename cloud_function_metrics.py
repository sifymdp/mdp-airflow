from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import monitoring_v3
from google.oauth2 import service_account
import json
import psycopg2
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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


def trigger_api_dag(**kwargs):
    return TriggerDagRunOperator(
        task_id='trigger_api_dag',
        trigger_dag_id='Api_connectivity',
        dag=dag,
    ).execute(context=kwargs)


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


def insert_log_db(request_data_json):
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
    # for data in request_data_json:
    # json_data = json.dumps(data)  # Convert the dictionary to a JSON string
    # Execute the insertion query
    # cursor.execute(sqlj, (json_data, valuee))

    # insert as columns
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for data in request_data_json:
        print("data:", data)
        subject = data['subject']
        item = data['item']['name']
        description = data['description']
        category = data['category']['name']
        subcategory = data['subcategory']['name']
        status = data['status']['name']
        template_name = data['template']['name']
        request_type = data['request_type']['name']
        valuee = data['threshold']

        # Execute the insertion query
        cursor.execute(sql, (subject, item, description, category,
                       subcategory, status, template_name, request_type, current_time, valuee))

# Commit the transaction and close the connection
    print("inserted successfully")
    connection.commit()
    cursor.close()
    connection.close()

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
    requests_data_alert = []
    request_db = []  # all the datas that are exceeded the threshold will be stored in this

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
                # alertjson
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
                # db_thresholdjson
                request_json_db = {
                    "subject": "ISCA Anomaly",
                    "item": {"name": instance_data['metric_type']},
                    "description": f"The instance have exceeded the threshold by value:{instance_data['value']} for {instance_data['subcategory_name']} in {instance_data['function_name']}",
                    "category": {"name": instance_data['category_name']},
                    "subcategory": {"name": instance_data['subcategory_name']},
                    "status": {"name": "Open"},
                    "template": {"name": "Default Request"},
                    "request_type": {"name": "Incident"},
                    "threshold": instance_data['value']
                }
                # alertjson
                requests_data_alert.append(request)
                # dbjson
                request_db.append(request_json_db)

    # db_insert with actual threshold
    insert_log_db(request_db)

    if requests_data_alert is not None:
        print(requests_data_alert)
        logs_file_path = '/opt/airflow/logs_gcp/gce_logs.json'
        with open(logs_file_path, 'w') as logs_file:
            json.dump(requests_data_alert, logs_file)
        print("Logs saved to:", logs_file_path)


cloud_function_logs = PythonOperator(
    task_id='get_cloud_function_metrics',
    python_callable=get_cloud_function_logs,
    dag=dag
)

trigger_api = PythonOperator(
    task_id='trigger_api_task',
    python_callable=trigger_api_dag,
    provide_context=True,
    dag=dag,
)
cloud_function_logs >> trigger_api
