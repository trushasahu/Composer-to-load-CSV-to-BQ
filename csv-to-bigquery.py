"""
Example DAG using GCSToBigQueryOperator.
"""

import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'airflow_ds')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'gcs_to_bq_table')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2), #datetime(2019, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='gcs_to_bigquery_operator',
	catchup=False,
	default_args=default_args,
    schedule_interval=None
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_ds_dataset', dataset_id=DATASET_NAME, dag=dag
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='third-campus-303308-bigmart',
    source_objects=['bigmart_data.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
	skip_leading_rows=1,
	field_delimiter=',',
	autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
# [END howto_operator_gcs_to_bigquery]

#delete_test_dataset = BigQueryDeleteDatasetOperator(
#    task_id='delete_airflow_ds_dataset', dataset_id=DATASET_NAME, delete_contents=True, dag=dag
#)

create_dataset >> load_csv #>> delete_test_dataset
