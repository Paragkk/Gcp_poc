
"""
DAG for Ingesting data
"""
import os
import time
from urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
)
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

# Variables to be changed Start
TABLE_NAME = "table_name"
PATH_TO_UPLOAD_FILE_PREFIX =  "file_prefix"
# Variables to be changed End

PROJECT_ID = "project_id"
BQ_LOCATION = "us-central1"
DATASET_STAGE = "dataset_stage"
DATASET_CURATED = "dataset_curated"
DATASET_REPORT = "dataset_report"
BUCKET_1 = "bucket_name"
BUCKET_2 = "bucket_name"
SCHEMA = f"schema_files/SCHEMA_{TABLE_NAME}.json"

INSERT_ROWS_QUERY_CURATED = (
    f"INSERT {DATASET_CURATED}.{TABLE_NAME}_curated "
    f"SELECT * FROM `{PROJECT_ID}.{DATASET_STAGE}.{TABLE_NAME}_ext` ;"
)
INSERT_ROWS_QUERY_REPORT = (
    f"INSERT {DATASET_REPORT}.{TABLE_NAME}_report "
    f"SELECT * FROM `{PROJECT_ID}.{DATASET_CURATED}.{TABLE_NAME}_curated` ;"
)
with models.DAG(
    f"jj_ingest_{TABLE_NAME}",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=[f"tag_{TABLE_NAME}"],
) as dag:

    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_1,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        mode='poke',
        task_id="gcs_file_watcher",
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    list_bucket_files = GCSListObjectsOperator(
        task_id="list_bucket_files", bucket=BUCKET_1, prefix=PATH_TO_UPLOAD_FILE_PREFIX)

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_1,
        source_objects=list_bucket_files.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{TABLE_NAME}_ext",
        skip_leading_rows=1,
        allow_quoted_newlines =True ,
        allow_jagged_rows=True,
        schema_object=SCHEMA,
    )

    create_curated_table = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table",
        dataset_id=DATASET_CURATED,
        table_id=f"{TABLE_NAME}_curated",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA}",
        location=BQ_LOCATION,
    )

    execute_insert_curated = BigQueryInsertJobOperator(
        task_id="execute_insert_curated",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_CURATED,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    create_reporting_table = BigQueryCreateEmptyTableOperator(
        task_id="create_reporting_table",
        dataset_id=DATASET_REPORT,
        table_id=f"{TABLE_NAME}_report",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA}",
        location=BQ_LOCATION,
    )

    execute_insert_report = BigQueryInsertJobOperator(
        task_id="execute_insert_report",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_REPORT,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    archieve_file = GCSToGCSOperator(
        task_id="archieve_file",
        source_bucket=BUCKET_1,
        source_object=f"{PATH_TO_UPLOAD_FILE_PREFIX}*",
        destination_bucket=BUCKET_2,
        destination_object=f"archieve/{PATH_TO_UPLOAD_FILE_PREFIX}",
    )

    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=BUCKET_1, objects=list_bucket_files.output
    )

gcs_object_with_prefix_exists >> list_bucket_files >> create_external_table >> create_curated_table >> execute_insert_curated >> create_reporting_table >> execute_insert_report >> archieve_file >> delete_files

if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
