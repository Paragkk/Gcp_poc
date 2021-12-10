from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.utils.state import State
from airflow.utils.dates import days_ago


# Variables to be changed Start
PROJECT_ID = "project_id"
BQ_LOCATION = "us-east4"
TABLE_NAME = "Service_Requests_from"
DATA_FILE_PREFIX =  "Service Requests from"
# Variables to be changed End

DATASET_STAGE = "demo_stage"
DATASET_CURATED = "demo_curated"
DATASET_REPORT = "demo_reports"
BUCKET_1 = "pot-data-itops"
BUCKET_2 = "pot-data-itops"
SCHEMA_EXT = f"schema_files/SCHEMA_EXT_{TABLE_NAME}.json"
SCHEMA_NATIVE = f"schema_files/SCHEMA_NATIVE_{TABLE_NAME}.json"
QUERY_CURATED = f"SELECT *,REGEXP_EXTRACT(_FILE_NAME, r'/([^/]+)/?$') as file_name FROM `{PROJECT_ID}.{DATASET_STAGE}.{TABLE_NAME}_ext`"
QUERY_REPORT = f"SELECT * FROM `{PROJECT_ID}.{DATASET_CURATED}.{TABLE_NAME}_curated`"
INSERT_ROWS_QUERY_CURATED = (f"INSERT INTO {DATASET_CURATED}.{TABLE_NAME}_curated {QUERY_CURATED}")
INSERT_ROWS_QUERY_REPORT = (f"INSERT INTO {DATASET_REPORT}.{TABLE_NAME}_report {QUERY_REPORT}" )

with models.DAG(
    f"Ingest_{TABLE_NAME}",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=[f"batch_ingestion_pipeline"],
) as dag:

    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_1,
        prefix=DATA_FILE_PREFIX,
        mode='poke',
        task_id="gcs_file_watcher",
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    list_bucket_files = GCSListObjectsOperator(
        task_id="list_bucket_files", bucket=BUCKET_1, prefix=DATA_FILE_PREFIX)

    archive_old_file = GCSToGCSOperator(
        task_id="archive_old_file",
        source_bucket=BUCKET_1,
        source_object=f"stage/{DATA_FILE_PREFIX}*",
        destination_bucket=BUCKET_2,
        destination_object=f"archive/{DATA_FILE_PREFIX}",
        move_object=True,
        retries=0,
    )

    stage_file = GCSToGCSOperator(
        task_id="stage_file",
        source_bucket=BUCKET_1,
        source_object=f"{DATA_FILE_PREFIX}*",
        destination_bucket=BUCKET_2,
        destination_object=f"stage/{DATA_FILE_PREFIX}",
        trigger_rule="all_done",
        move_object=False,
    )
    list_stage_files = GCSListObjectsOperator(
        task_id="list_stage_files", bucket=BUCKET_1, prefix=f"stage/{DATA_FILE_PREFIX}")

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_1,
        source_objects= list_stage_files.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{TABLE_NAME}_ext",
        skip_leading_rows=1,
        allow_quoted_newlines =True ,
        allow_jagged_rows=True,
        schema_object=SCHEMA_EXT,
    )

    create_curated_table = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table",
        dataset_id=DATASET_CURATED,
        table_id=f"{TABLE_NAME}_curated",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_NATIVE}",
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
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_NATIVE}",
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
    

    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=BUCKET_1, objects=list_bucket_files.output
    )

gcs_object_with_prefix_exists >> list_bucket_files >> archive_old_file >> stage_file >> list_stage_files >> create_external_table >> create_curated_table >> execute_insert_curated >> create_reporting_table >> execute_insert_report >> delete_files

if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
