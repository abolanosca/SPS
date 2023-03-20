import os
import pendulum
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import csv
import yaml
from airflow import DAG
from google.cloud import bigquery
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.email_operator import EmailOperator

#Pending: Send mail, check product file


env_type = os.environ['env_type']
local_tz = pendulum.timezone('America/Denver')  # Locale to match
today = pendulum.now().to_date_string()  # Obtains the current date
bigquery_client = bigquery.Client()
storage_client = storage.Client()

if env_type == 'dev':
    send_email=False
    ds_elcap = 'backcountry-data-team.elcap'
    ds_stg = 'backcountry-data-team.elcap_stg_dev'
    yamlfile = '/home/airflow/gcs/dags/sps_new/yaml/sps.yaml'
    file_root = '/home/airflow/gcs/dags/sps_new'
    email_notification = "abolanos@backcountry.com"


elif env_type == 'prod':
    send_email=True
    ds_elcap = 'backcountry-data-team.elcap'
    ds_stg = 'backcountry-data-team.elcap_stg'
    yamlfile = '/home/airflow/gcs/dags/sps_new/yaml/sps.yaml'
    file_root = '/home/airflow/gcs/dags/sps_new'
    email_notification = "biteam@backcountry.com"


with open(yamlfile, 'r') as key_file:
    configDict = yaml.safe_load(key_file.read())
    config = configDict['sps']
    gcp_bucket = config['gcp_bucket']
    gcp_bucket_hist = config['gcp_bucket_hist']
    first_day = config['first_day']
    last_day = config['last_day']
    activity_file_name = config['activity_file_name']
    product_file_name = config['product_file_name']
    location_file_name = config['location_file_name']


def delete_files(*args, **kwargs):
    bucket = storage_client.bucket(gcp_bucket)
    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()

def create_txt_file(*args, **kwargs):
    fq = open(file_root + '/sql/misc/sps_week_ending_date.sql', 'r')
    sqlTransParameter = fq.read()
    sqlTransParameter = sqlTransParameter.replace('@ds_stg',ds_stg)
    fq.close()

    if (kwargs['header']=='no'):

        result = bigquery_client.query(sqlTransParameter)
        for row in result:
            date1 = str(row[0])

            fd = open(file_root + kwargs['sql'], 'r')
            sqlFile = fd.read()
            fd.close()
            sqlFile = sqlFile.replace('@ds_stg',ds_stg)

            query_job = bigquery_client.query(sqlFile)
            results = query_job.result()

            filename = str(kwargs['file'] )+date1+'.txt'

            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file, delimiter='|')
                writer.writerows(results)

            bucket = storage_client.bucket(kwargs['bucket'])
            blob = bucket.blob(filename)
            blob.upload_from_filename(filename)

            bucket_hist = storage_client.bucket(kwargs['bucket_hist'])
            blob_hist = bucket_hist.blob(f'{date1}/'+filename)
            blob_hist.upload_from_filename(filename)

            os.remove(filename)
    else:
        result = bigquery_client.query(sqlTransParameter)
        for row in result:
            date1 = str(row[0])

            fd = open(file_root + kwargs['sql_header'], 'r')
            sqlFile = fd.read()
            fd.close()
            sqlFile = sqlFile.replace('@ds_stg',ds_stg)

            query_job = bigquery_client.query(sqlFile)
            result_header = query_job.result()

            fd = open(file_root + kwargs['sql'], 'r')
            sqlFile = fd.read()
            fd.close()
            sqlFile = sqlFile.replace('@ds_stg',ds_stg)

            query_job = bigquery_client.query(sqlFile)
            results = query_job.result()

            filename = str(kwargs['file']) + date1 + '.txt'



            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file, delimiter='|')
                writer.writerows(result_header)
                writer.writerows(results)

            bucket = storage_client.bucket(kwargs['bucket'])
            blob = bucket.blob(filename)
            blob.upload_from_filename(filename)

            bucket_hist = storage_client.bucket(kwargs['bucket_hist'])
            blob_hist = bucket_hist.blob(f'{date1}/'+filename)
            blob_hist.upload_from_filename(filename)

            os.remove(filename)



default_args = {
    'owner': 'EDI - SPS',
    'start_date': datetime(2022, 1, 1, tzinfo=local_tz),
    'email': ['datainsights-etl-failure@backcountry.pagerduty.com', 'biteam@backcountry.com'],
    'email_on_failure': send_email,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=6)
}

with DAG('SPS_Process'
        , default_args=default_args
        , max_active_runs=1
        , catchup=False) as dag:


    #Determines the dates of the week to be generated
    execute_sps_dates = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_dates',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/misc/sps_dates.sql',
        params = {'ds_elcap': ds_elcap ,'ds_stg':ds_stg, 'first_day':first_day},
    )

    execute_sps_channel_summary = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_channel_summary',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/misc/sps_channel_summary.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    execute_sps_gross_sales = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_gross_sales',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/sales/sps_gross_sales.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    execute_sps_return_sales = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_return_sales',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/sales/sps_return_sales.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    execute_sps_receipts = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_receipts',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_receipts.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    execute_sps_inventory = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_inventory',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_inventory.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    execute_sps_on_order = BigQueryOperator(
        dag=dag,
        task_id='execute_sps_on_order',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_on_order.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )


    merge_sps_gross_sales = BigQueryOperator(
        dag=dag,
        task_id='merge_sps_gross_sales',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/sales/sps_merge_gross_sales.sql',
        params = {'ds_stg':ds_stg },
    )

    merge_sps_return_sales = BigQueryOperator(
        dag=dag,
        task_id='merge_sps_return_sales',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/sales/sps_merge_return_sales.sql',
        params = {'ds_stg':ds_stg },
    )

    merge_sps_receipts = BigQueryOperator(
        dag=dag,
        task_id='merge_sps_receipts',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_merge_receipts.sql',
        params = {'ds_stg':ds_stg },
    )

    merge_sps_inventory = BigQueryOperator(
        dag=dag,
        task_id='merge_sps_inventory',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_merge_inventory.sql',
        params = {'ds_stg':ds_stg },
    )

    merge_sps_on_order = BigQueryOperator(
        dag=dag,
        task_id='merge_sps_on_order',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/po_inventory/sps_merge_on_order.sql',
        params = {'ds_stg':ds_stg },
    )

    delete_sps_duplicates = BigQueryOperator(
        dag=dag,
        task_id='delete_sps_duplicates',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/misc/sps_delete_duplicates.sql',
        params = {'ds_stg':ds_stg },
    )

    sps_product_prices = BigQueryOperator(
        dag=dag,
        task_id='sps_product_prices',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/taxonomy/sps_product_price.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    sps_channel_activity = BigQueryOperator(
        dag=dag,
        task_id='sps_channel_activity',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/misc/sps_channel_activity.sql',
        params = {'ds_stg':ds_stg },
    )

    sps_locations = BigQueryOperator(
        dag=dag,
        task_id='sps_locations',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/locations/sps_locations.sql',
        params = {'ds_stg':ds_stg },
    )

    sps_product_last_receipt = BigQueryOperator(
        dag=dag,
        task_id='sps_product_last_receipt',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/taxonomy/sps_product_last_receipt.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    sps_product_effective_date = BigQueryOperator(
        dag=dag,
        task_id='sps_product_effective_date',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/taxonomy/sps_product_effective_date.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    sps_channel_product = BigQueryOperator(
        dag=dag,
        task_id='sps_channel_product',
        use_legacy_sql=False,
        priority='BATCH',
        sql='sql/taxonomy/sps_channel_product.sql',
        params = {'ds_elcap': ds_elcap,'ds_stg':ds_stg },
    )

    create_file_chnnl_activity = PythonOperator(
        task_id="create_file_chnnl_activity",
        python_callable=create_txt_file,
        op_kwargs={'sql': '/sql/file/sps_file_channel_activity.sql', 'file': activity_file_name, 'bucket': gcp_bucket,'bucket_hist': gcp_bucket_hist, 'header':'yes','sql_header':'/sql/file/sps_header_channel_activity.sql'},
        provide_context=True,
    )

    create_file_chnnl_products = PythonOperator(
        task_id="create_file_chnnl_products",
        python_callable=create_txt_file,
        op_kwargs={'sql': '/sql/file/sps_file_channel_product.sql', 'file': product_file_name, 'bucket': gcp_bucket, 'bucket_hist': gcp_bucket_hist,'header':'no'},
        provide_context=True,
    )

    create_file_location = PythonOperator(
        task_id="create_file_location",
        python_callable=create_txt_file,
        op_kwargs={'sql': '/sql/file/sps_file_location.sql', 'file': location_file_name, 'bucket': gcp_bucket, 'bucket_hist': gcp_bucket_hist, 'header': 'no'},
        provide_context=True,
    )

    delete_files_sps = PythonOperator(
        task_id="delete_files_sps",
        python_callable=delete_files,
        provide_context=True,
    )

    email_send = EmailOperator(
        task_id='email_send',
        to=email_notification,
        subject=f'SPS Files for {today} have been updated',
        html_content=f"""
          The generated files for the date of {today} have been successfully generated and stored in GCP.
          Use the following <a href="https://console.cloud.google.com/storage/browser/{gcp_bucket}/">link</a> to view files in their respective GCP bucket
          """,
    )



delete_files_sps >> execute_sps_dates >> execute_sps_channel_summary >> execute_sps_gross_sales

delete_files_sps >> execute_sps_dates >> execute_sps_channel_summary >> execute_sps_return_sales

delete_files_sps >> execute_sps_dates >> execute_sps_channel_summary >> execute_sps_receipts

delete_files_sps >> execute_sps_dates >> execute_sps_channel_summary >> execute_sps_inventory

delete_files_sps >> execute_sps_dates >> execute_sps_channel_summary >> execute_sps_on_order

[execute_sps_gross_sales,execute_sps_return_sales,execute_sps_receipts,execute_sps_inventory,execute_sps_on_order] >> merge_sps_gross_sales \
>> merge_sps_return_sales >> merge_sps_receipts >> merge_sps_inventory >> merge_sps_on_order >> delete_sps_duplicates >> sps_product_prices \
>> sps_channel_activity >> create_file_chnnl_activity >> sps_locations >> sps_product_last_receipt >> sps_product_effective_date \
>> sps_channel_product >> create_file_chnnl_products >> create_file_location >> email_send

