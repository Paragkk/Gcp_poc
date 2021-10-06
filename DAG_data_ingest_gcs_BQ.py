
"""
DAG for Ingesting Data from CSV file and dumping in native tables
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
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
)
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

PROJECT_ID = "gcp-project-id"
BQ_LOCATION = "us-central1"
DATASET_STAGE = "data_stage"
DATASET_CURATED = "data_curated"
TABLE_NAME = "app_data"
PATH_TO_UPLOAD_FILE_PREFIX = "file_name_pattern"
BUCKET_1 = "bucket_1"
BUCKET_2 = "bucket_2"
SCHEMA = [
    {
        "name": "signature",
                "type": "STRING"
    },
    {
        "name": "Alertmanager",
                "type": "STRING"
    },
]

INSERT_ROWS_QUERY = (
    f"INSERT {DATASET_CURATED}.{TABLE_NAME}_curated "
    f"SELECT * FROM `{PROJECT_ID}.{DATASET_STAGE}.{TABLE_NAME}_ext` ;"
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
        task_id="list_bucket_files", bucket=BUCKET_1, prefix="AIML-ETQ-Alert-Incident-moogsoft")

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_1,
        source_objects=list_bucket_files.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{TABLE_NAME}_ext",
        skip_leading_rows=1,
        schema_fields=SCHEMA,
    )

    create_curated_table = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table",
        dataset_id=DATASET_CURATED,
        table_id=f"{TABLE_NAME}_curated",
        schema_fields=SCHEMA,
        location=BQ_LOCATION,
    )

    execute_insert_query = BigQueryInsertJobOperator(
        task_id="execute_insert_curated",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY,
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

gcs_object_with_prefix_exists >> list_bucket_files >> create_external_table >> create_curated_table >> execute_insert_query >> archieve_file >> delete_files

if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
