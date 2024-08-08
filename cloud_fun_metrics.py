# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.models import DagRun
# from datetime import datetime, timedelta
# from google.cloud import monitoring_v3
# from google.oauth2 import service_account
# import json
# import psycopg2
# from airflow.exceptions import AirflowException
# import pytz
# import numpy as np
# from airflow.models.connection import Connection


# def get_airflow_connection(connection_id):
#     try:
#         conn = Connection.get_connection_from_secrets(connection_id)
#         db_params = {
#             "host": conn.host,
#             "database": conn.schema,
#             "user": conn.login,
#             "password": conn.password,
#         }
#         return db_params
#     except AirflowException as e:
#         raise AirflowException(f"Error retrieving Airflow connection: {e}")


# connection_id = "postgres_db"
# db_params = get_airflow_connection(connection_id)
# default_args = {
#     "owner": "airflow",
#     "start_date": datetime.now(),
#     "retries": 0,
#     "retry_delay": timedelta(seconds=5),
#     "email": ["managementairflow@gmail.com"],
#     "email_on_failure": True,
#     "email_on_retry": True,
# }


# dag = DAG(
#     "cloud_f_metrics",
#     default_args=default_args,
# )


# def connect_database(db_params):
#     try:
#         connection = psycopg2.connect(**db_params)
#         cursor = connection.cursor()
#         return connection, cursor
#     except Exception as e:
#         print(f"An error occurred in db connection: {e}")


# # this will insert the data for superset


# def insert_log_v(request_db):
#     connection, cursor = connect_database(db_params)
#     sql = """
#     INSERT INTO logs.logs_superset (subject, item, description, category, subcategory, status, template_name, request_type,created_at,Threshold,requester)
#     VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
#     """
#     # insert as columns
#     current_time = datetime.now()
#     for data in request_db:
#         subject = data["subject"]
#         requester = data["requester"]["name"]
#         item = data["item"]["name"]
#         description = data["description"]
#         category = data["category"]["name"]
#         subcategory = data["subcategory"]["name"]
#         status = data["status"]["name"]
#         template_name = data["template"]["name"]
#         request_type = data["request_type"]["name"]
#         threshold = data["threshold"]
#         # Execute the insertion query
#         cursor.execute(
#             sql,
#             (
#                 subject,
#                 item,
#                 description,
#                 category,
#                 subcategory,
#                 status,
#                 template_name,
#                 request_type,
#                 current_time,
#                 threshold,
#                 requester,
#             ),
#         )

#     # Commit the transaction and close the connection
#     print("inserted successfully")
#     connection.commit()
#     cursor.close()
#     connection.close()


# # this function is for storing the data for api dag to send alert


# def insert_log_db(request_data_json):
#     connection, cursor = connect_database(db_params)
#     sql = """
#     INSERT INTO logs.alert_data(data)
#     VALUES (%s)
#     """

#     # insert as json
#     for data in request_data_json:
#         json_data = json.dumps(data)  # Convert the dictionary to a JSON string
#         # Execute the insertion query
#         cursor.execute(sql, (json_data,))

#     # Commit the transaction and close the connection
#     print("inserted successfully")
#     connection.commit()
#     cursor.close()
#     connection.close()


# # Function to monitor cloud function metrics


# def get_cloud_function_logs():

#     with open("/opt/airflow/secrets/cloud_metrics.json", "r") as metric_file:
#         config = json.load(metric_file)

#     with open(
#         "/opt/airflow/secrets/google_cloud_default.json", "r"
#     ) as key_file:
#         json_content = key_file.read()
#     json_account_info = json.loads(json_content)
#     try:
#         credentials = service_account.Credentials.from_service_account_info(
#             json_account_info
#         )
#         client = monitoring_v3.MetricServiceClient(credentials=credentials)
#     except Exception as e:
#         print(f"An error occurred: {e}")
#     requests_data_alert = []
#     request_db = (
#         []
#     )  # all the datas that are exceeded the threshold will be stored in this
#     try:
        
#         for metric_config in config["metrics_cloudfunctions"]:
#             # metrics from config
#             project_id = metric_config["project_id"]
#             function_name = metric_config["function_name"]
#             threshold = metric_config["threshold"]
#             severity = metric_config["severity"]
#             window_func=metric_config["window_function"]
#             dag_id = "cloud_function_metrics"
#             # dag_runs = DagRun.find(dag_id=dag_id, state="success")
#             # if dag_runs:
#             #     dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
#             #     # getting last exec time of dag
#             #     dag_time = dag_runs[0].execution_date
#             #     ist_timezone = pytz.timezone("Asia/Kolkata")
#             #     last_exec_time = dag_time.astimezone(
#             #         ist_timezone
#             #     )  # for last dag time conversion
#             #     # fetch current time in IST
#             #     end_time = datetime.now(ist_timezone)
#             #     start_time = (
#             #         last_exec_time  # set current time to lastdag execution time
#             #     )
#             # else:
#             #     # fetch current time in IST
#             #     end_time = datetime.now(ist_timezone)
#             #     start_time = end_time - timedelta(minutes=15)
#             ist_timezone = pytz.timezone("Asia/Kolkata")
#             end_time = datetime.now(ist_timezone)
#             start_time = end_time - timedelta(minutes=15)


#             interval = monitoring_v3.TimeInterval(
#                 {
#                     "end_time": {"seconds": int(end_time.timestamp())},
#                     "start_time": {"seconds": int(start_time.timestamp())},
#                 }
#             )
#             if window_func == 'mean':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
#                     })
#             elif window_func == '95th percentile':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_95,
#                     }
#                 )
#             elif window_func == 'max':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
#                     })
#             elif window_func == 'count':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_COUNT,
#                     })
#             elif window_func == 'percent change':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENT_CHANGE,
#                     })
#             elif window_func == 'rate':
#                 aggregation = monitoring_v3.Aggregation(
#                     {
#                         "alignment_period": {"seconds": 900},  # 15 minutes
#                         "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
#                     })
#             else:
#                 raise ValueError("Unsupported window function")
            

#             metric_type = metric_config["metric_name"]
#                         #print("metric count::::",count,":",metric_type)
#             project_name = f"projects/{project_id}"
#             # cloud_function_logs = client.list_time_series(
#             #     name=project_name,
#             #     filter=(
#             #         f'metric.type="{metric_type}" '
#             #         f'AND resource.labels.function_name="{function_name}" '
#             #     ),
#             #     interval=interval,
#             #     view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
#             # )
#             cloud_function_logs=client.list_time_series(
#                     request={
#                         "name": project_name,
#                         "filter": (f'metric.type="{metric_type}" '
#                                 f'AND resource.labels.function_name="{function_name}" '
#                                 f'AND resource.labels.project_id="{project_id}" '
#                                     ),
#                         "interval": interval,
#                         "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
#                         "aggregation": aggregation,
#                     }
#                 )
#             print("logsss:::", cloud_function_logs)
#             for ts in cloud_function_logs:
#                 subcategory_name = metric_config["subcategory_name"]
#                 category_name = metric_config["category_name"]
#                 instance_data = {
#                     "project_id": ts.resource.labels["project_id"],
#                     "zone": ts.resource.labels["region"],
#                     "function_name": ts.resource.labels["function_name"],
#                     "severity": severity,
#                     "metric_type": ts.metric.type,
#                     "value_type": ts.value_type,
#                     "value": "",
#                 }
#                 # print("severity:::", severity)
#                 points = []
#                 for point in ts.points:
#                     # value type int64 is mentioned as 2 and double as 3
#                     if ts.value_type == 2:  # int64
#                         value = point.value.int64_value
#                     elif ts.value_type == 5:  # distribution
#                         distribution_value = point.value.distribution_value
#                         # Calculate the mean value from the distribution value for now
#                         value = distribution_value.mean
#                     else:
#                         value = None
#                     point_data = {"value": value}
#                     # the result object has many points ..each points has different values for different time..
#                     # so,appending all the point values separately and storing the max value among them
#                     # if point data has no value it wont be appended
#                     if point_data["value"] is not None:
#                         points.append(point_data["value"])
#                     if points:
#                         instance_data["value"] = points[0]
#                         print("valuee::",instance_data['value'])


#                     #     if window_func=="mean":
#                     #         Exceeded_T=np.mean(points)
#                     #     elif window_func=="95th percentile":
#                     #         Exceeded_T=np.percentile(points,95)
#                     #     elif window_func=="max":
#                     #         Exceeded_T=np.max(points)
#                     #     elif window_func=="count":
#                     #         Exceeded_T=np.max(points)
#                     #     elif window_func=="percent change":
#                     #         for i in range(1,len(points)):
#                     #             if points[i-1]==0:
#                     #                 continue
#                     #             Exceeded_T=((points[i]-points[i-1])/points[i-1])*100
#                     #     elif window_func=="rate":
#                     #             if points[0]==0:
#                     #                 continue
#                     #             else:    
#                     #                 Exceeded_T=(points[-1] - points[0]) / points[0]
#                     # else:
#                     #     Exceeded_T=None
#                     instance_data["category_name"] = category_name
#                     instance_data["subcategory_name"] = subcategory_name
#                 # all the retrieved datas are in instance data list are checked for threshold reach ..
#                 # if reached ..they are stored in request_data for alert in mapped to given json structure
#                 # print(type(instance_data["value"]))
#                 if instance_data["value"]!='':
#                     try:
#                         instance_data["value"] = float(instance_data["value"])
#                     except ValueError as e:
#                         instance_data["value"]=None
#                 else:
#                     instance_data["value"]=None
#                 # all the retrieved datas are in instance data list are checked for threshold reach ..
#                 # if reached ..they are stored in request_data for alert in mapped to given json structure
#                 # print("isntance data :::::", instance_data)
#                 if (
#                     instance_data["value"] is not None
#                     and instance_data["value"] != 0
#                     and instance_data["value"] > threshold
#                 ):

#                     # alertjson
#                     request = {
#                         "subject": "ISCA Anomaly",
#                         "requester": {"name": "ISCA-DEV"},
#                         # "mode": {"name": "isca.helpdesk@timesgroup.com"},
#                         "item": {"name": "Alert"},
#                         "subcategory": {"name": "Cloud"},
#                         "status": {"name": "Open"},
#                         "template": {"name": "Default Request"},
#                         "request_type": {"name": "Incident"},
#                         "description": f"The instance have exceeded the threshold by value:{instance_data['value']} for {instance_data['subcategory_name']} in FUNCTION NAME:{instance_data['function_name']} Severity: {instance_data['severity']}",
#                         "category": {"name": "ISCA-Alerts"},
#                     }
#                     # db_thresholdjson
#                     request_json_db = {
#                         "subject": "ISCA Anomaly",
#                         "requester": {"name": "ISCA-DEV"},
#                         "item": {"name": instance_data["metric_type"]},
#                         "description": f"The instance have exceeded the threshold by value:{instance_data['value']} for {instance_data['subcategory_name']} in FUNCTION NAME:{instance_data['function_name']}",
#                         "category": {"name": instance_data["category_name"]},
#                         "subcategory": {"name": instance_data["subcategory_name"]},
#                         "status": {"name": "Open"},
#                         "template": {"name": "Default Request"},
#                         "request_type": {"name": "Incident"},
#                         "threshold": instance_data["value"],
#                     }
#                     # alertjson
#                     requests_data_alert.append(request)
#                     # dbjson
#                     request_db.append(request_json_db)
#         # print("alert data:::::::", requests_data_alert)
#         if request_db:
#             insert_log_v(request_db)
#             print("db inserted !!!")
#         if requests_data_alert:
#             insert_log_db(requests_data_alert)
#             print("Alerts inserted !!!")
#         else:
#             print("No instances exceeded the thresholds")
#     except Exception as e:
#         raise AirflowException("Error occured:", e)


# cloud_function_logs = PythonOperator(
#     task_id="get_cloud_function_metrics",
#     python_callable=get_cloud_function_logs,
#     dag=dag,
# )


# cloud_function_logs
