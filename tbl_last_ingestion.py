from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2

db_params = {
    'host': '172.16.20.117',
    'database': 'rpt_awll1201',
    'user': 'postgres',
    'password': 'K8V6tOpEn0'
}


def delete_previous_month_data(**kwargs):
    current_date = datetime.now().date()  # apr 1
    current_datee=current_date.replace(day=1)
    if current_datee.day == 1:
        previous_month = current_date.replace(day=1) - timedelta(days=1)  # =march 31
        previous_of_previous_month_last = previous_month.replace(day=1) - timedelta(days=1)  # =feb 29
        previous_of_previous_month_first = previous_of_previous_month_last.replace(day=1)  # =feb1
        conn = psycopg2.connect(db_params)
        # cursor = conn.cursor()
        # sql = f"""delete from rpt_awll1201.tbl_sales_data_last 
        #           where createdtime between {previous_of_previous_month_first} and {previous_of_previous_month_last}"""
        # cursor.execute(sql)
        print(f"Deleting data between{previous_of_previous_month_first} and {previous_of_previous_month_last}")
    else:
        print("This is not a first of a month")

def execute_insert_query(**kwargs):
    print("Executing insert query for the previous day's data.")
    current_date = datetime.now().date()
    query_date=current_date-timedelta(days=1)
    print("query date for insertion::",query_date)
    sql=f"""INSERT INTO rpt_awll1201.tbl_sales_data_last
            (po_num_ref, clustername, discount_amount, cgst_percentage, seller_state_code, scheme_type, cess_amount, salesman_id, uom, listprice, xproduct_category, sales_order_id, sku_name, sgst_amount, product_brand, order_type, invoice_number, longitude, transaction_id, schfreeqtyuom2, baseuom_id, salesman_code, sku_id, total_tax, transaction_type, free_quantity, hsncode, xdistributorpicklist2, transaction_tax_type, xdistributorpicklist3, sales_order_date, uom_conversion, uom3, seller_state, distributor_id, xuniversalid, prohiercode_level2, retailer_name, sku_code, status, base_quantity, salesman_name, schfreeqty, buyer_gstinno, latitude, cess_percentage, transaction_line_id, uom2, schemes_name, beat_id, invoice_date, distributor_code, beat_name, igst_amount, scheme_percentage, prohiername_level2, prohier_id_level2, mobile_orderno, outlet_type, seller_gstinno, invoice_type, igst_percentage, taxable_amount, retailer_city, buyer_state_code, amount, batch_no, quantity, ret_general_class, retailer_code, po_date_ref, retailer_id, manual_discount_amount, pts, createdtime, schemes_code, ptr, tuom_id, pack_type, product_type, sales_order_number, other_tax, beat_code, sgst_percentage, scheme_discount_amount, distributor_name, invoice_level_discount, net_amount, buyer_state, cgst_amount, print_conf_count)
    SELECT po_num_ref ,
	clustername ,
	discount_amount ,
	cgst_percentage ,
	seller_state_code ,
	scheme_type ,
	cess_amount ,
	salesman_id ,
	uom ,
	listprice ,
	xproduct_category ,
	sales_order_id ,
	sku_name ,
	sgst_amount ,
	product_brand ,
	order_type ,
	invoice_number ,
	longitude ,
	transaction_id ,
	schfreeqtyuom2 ,
	baseuom_id ,
	salesman_code ,
	sku_id ,
	total_tax ,
	transaction_type ,
	free_quantity ,
	hsncode ,
	xdistributorpicklist2 ,
	transaction_tax_type ,
	xdistributorpicklist3 ,
	sales_order_date ,
	uom_conversion ,
	uom3 ,
	seller_state ,
	distributor_id ,
	xuniversalid ,
	prohiercode_level2 ,
	retailer_name ,
	sku_code ,
	status ,
	base_quantity ,
	salesman_name ,
	schfreeqty ,
	buyer_gstinno ,
	latitude ,
	cess_percentage ,
	transaction_line_id ,
	uom2 ,
	schemes_name ,
	beat_id ,
	invoice_date ,
	distributor_code ,
	beat_name ,
	igst_amount ,
	scheme_percentage ,
	prohiername_level2 ,
	prohier_id_level2 ,
	mobile_orderno ,
	outlet_type ,
	seller_gstinno ,
	invoice_type ,
	igst_percentage ,
	taxable_amount ,
	retailer_city ,
	buyer_state_code ,
	amount ,
	batch_no ,
	quantity ,
	ret_general_class ,
	retailer_code ,
	po_date_ref ,
	retailer_id ,
	manual_discount_amount ,
	pts ,
	createdtime ,
	schemes_code ,
	ptr ,
	tuom_id ,
	pack_type ,
	product_type ,
	sales_order_number ,
	other_tax ,
	beat_code ,
	sgst_percentage ,
	scheme_discount_amount ,
	distributor_name ,
	invoice_level_discount ,
	net_amount ,
	buyer_state ,
	cgst_amount ,
	print_conf_count  FROM rpt_awll1201.tbl_sales_data x
    WHERE createdtime = {query_date}"""


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'execute_insert_and_delete_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

delete_data_task = PythonOperator(
    task_id='delete_previous_of_previous_month_data_task',
    python_callable=delete_previous_month_data,
    provide_context=True,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='execute_insert_query_task',
    python_callable=execute_insert_query,
    provide_context=True,
    dag=dag,
)

delete_data_task >> insert_data_task
