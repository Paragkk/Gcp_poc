
"""
DAG for spliting file
"""
from airflow import models
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.operators.python_operator import PythonOperator
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
TABLE_NAME = "1_month_report"
PATH_TO_UPLOAD_FILE_PREFIX = "1_month_report.csv"
TABLE_NAME_1 = 'business_summary'
TABLE_NAME_2 = 'Availability_and_Load_Over_Time'
TABLE_NAME_3 = 'Performance_and_Load_Over_Time'
DATA_FILE_PREFIX_1 =  "1_month_report_Performance_and_Load_Over_Time"
DATA_FILE_PREFIX_2 =  "1_month_report_business_summary"
DATA_FILE_PREFIX_3 =  "1_month_report_Availability_and_Load_Over_Time"
PROJECT_ID = "project_id"
BQ_LOCATION = "us-east4"
# Variables to be changed End
DATASET_STAGE = "demo_stage"
DATASET_CURATED = "demo_curated"
DATASET_REPORT = "demo_reports"
BUCKET_1 = "jj-pot-data-itops"
BUCKET_2 = "jj-pot-data-itops"
SCHEMA_EXT_1 = f"schema_files/SCHEMA_EXT_{DATA_FILE_PREFIX_1}.json"
SCHEMA_EXT_2 = f"schema_files/SCHEMA_EXT_{DATA_FILE_PREFIX_2}.json"
SCHEMA_EXT_3 = f"schema_files/SCHEMA_EXT_{DATA_FILE_PREFIX_3}.json"
QUERY_CURATED_1 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_STAGE}.{DATA_FILE_PREFIX_1}_ext`"
QUERY_REPORT_1 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_CURATED}.{DATA_FILE_PREFIX_1}_curated`"
INSERT_ROWS_QUERY_CURATED_1 = (f"INSERT INTO {DATASET_CURATED}.{DATA_FILE_PREFIX_1}_curated {QUERY_CURATED_1}")
INSERT_ROWS_QUERY_REPORT_1 = (f"INSERT INTO {DATASET_REPORT}.{DATA_FILE_PREFIX_1}_report {QUERY_REPORT_1}" )
QUERY_CURATED_2 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_STAGE}.{DATA_FILE_PREFIX_2}_ext`"
QUERY_REPORT_2 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_CURATED}.{DATA_FILE_PREFIX_2}_curated`"
INSERT_ROWS_QUERY_CURATED_2 = (f"INSERT INTO {DATASET_CURATED}.{DATA_FILE_PREFIX_2}_curated {QUERY_CURATED_2}")
INSERT_ROWS_QUERY_REPORT_2 = (f"INSERT INTO {DATASET_REPORT}.{DATA_FILE_PREFIX_2}_report {QUERY_REPORT_2}" )
QUERY_CURATED_3 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_STAGE}.{DATA_FILE_PREFIX_3}_ext`"
QUERY_REPORT_3 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_CURATED}.{DATA_FILE_PREFIX_3}_curated`"
INSERT_ROWS_QUERY_CURATED_3 = (f"INSERT INTO {DATASET_CURATED}.{DATA_FILE_PREFIX_3}_curated {QUERY_CURATED_3}")
INSERT_ROWS_QUERY_REPORT_3 = (f"INSERT INTO {DATASET_REPORT}.{DATA_FILE_PREFIX_3}_report {QUERY_REPORT_3}" )



with models.DAG(
    f"split_{TABLE_NAME}",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=[f"file_split_job"],
) as dag:

    def split_file_func(**kwargs):
        from google.cloud import storage
        from smart_open import open

        def write_file(file_object, content):
            file_object.write(str(content))

        def get_tab_name(test_string):
            if test_string == '':
                set_flag = 'NT'
            elif test_string == 'BT Name':
                set_flag = TABLE_NAME_1
            elif test_string == 'Transaction Availability (%)':
                set_flag = TABLE_NAME_2
            elif test_string == 'Ok (%)':
                set_flag = TABLE_NAME_3
            else:
                set_flag = 'NA'
            return set_flag

        try:
            write = 'N'
            # stream from GCS
            for line in open(f"gs://{kwargs['key1']}/{kwargs['key2']}", 'r'):
                items = line.split(',')
                if items[1] in ['BT Name', 'Transaction Availability (%)', 'Ok (%)']:
                    tab_name = get_tab_name(items[1])
                    fout = open(
                        f"gs://{kwargs['key1']}/{kwargs['key3']}_{tab_name}.csv", 'w')
                    write = 'Y'
                if items[0] == '':
                    write = 'N'
                if write == 'Y':
                    write_file(fout, line)
        except Exception as er:
            print('Error:', er)

    split_file = PythonOperator(
        task_id='split_file',
        python_callable=split_file_func,
        op_kwargs={'key1': BUCKET_1,
                   'key2': PATH_TO_UPLOAD_FILE_PREFIX, 'key3': TABLE_NAME},
    )

    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_1,
        prefix=TABLE_NAME,
        mode='poke',
        task_id="gcs_file_watcher",
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    list_bucket_files_1 = GCSListObjectsOperator(
        task_id="list_bucket_files_1", bucket=BUCKET_1, prefix=DATA_FILE_PREFIX_1)

    archive_old_file_1 = GCSToGCSOperator(
        task_id="archive_old_file_1",
        source_bucket=BUCKET_1,
        source_object=f"stage/{DATA_FILE_PREFIX_1}*",
        destination_bucket=BUCKET_2,
        destination_object=f"archive/{DATA_FILE_PREFIX_1}",
        move_object=True,
        retries=0,
    )

    stage_file_1 = GCSToGCSOperator(
        task_id="stage_file_1",
        source_bucket=BUCKET_1,
        source_object=f"{DATA_FILE_PREFIX_1}*",
        destination_bucket=BUCKET_2,
        destination_object=f"stage/{DATA_FILE_PREFIX_1}",
        trigger_rule="all_done",
        move_object=False,
    )
    list_stage_files_1 = GCSListObjectsOperator(
        task_id="list_stage_files_1", bucket=BUCKET_1, prefix=f"stage/{DATA_FILE_PREFIX_1}")

    create_external_table_1 = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_1",
        bucket=BUCKET_1,
        source_objects= list_stage_files_1.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{DATA_FILE_PREFIX_1}_ext",
        skip_leading_rows=1,
        allow_quoted_newlines =True ,
        allow_jagged_rows=True,
        schema_object=SCHEMA_EXT_1,
    )

    create_curated_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table_1",
        dataset_id=DATASET_CURATED,
        table_id=f"{DATA_FILE_PREFIX_1}_curated",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_EXT_1}",
        location=BQ_LOCATION,
    )

    execute_insert_curated_1 = BigQueryInsertJobOperator(
        task_id="execute_insert_curated_1",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_CURATED_1,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    create_reporting_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_reporting_table_1",
        dataset_id=DATASET_REPORT,
        table_id=f"{DATA_FILE_PREFIX_1}_report",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_EXT_1}",
        location=BQ_LOCATION,
    )

    execute_insert_report_1 = BigQueryInsertJobOperator(
        task_id="execute_insert_report_1",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_REPORT_1,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    

    delete_files_1 = GCSDeleteObjectsOperator(
        task_id="delete_files_1", bucket_name=BUCKET_1, objects=list_bucket_files_1.output
    )
    
    list_bucket_files_2 = GCSListObjectsOperator(
        task_id="list_bucket_files_2", bucket=BUCKET_2, prefix=DATA_FILE_PREFIX_2)

    archive_old_file_2 = GCSToGCSOperator(
        task_id="archive_old_file_2",
        source_bucket=BUCKET_2,
        source_object=f"stage/{DATA_FILE_PREFIX_2}*",
        destination_bucket=BUCKET_2,
        destination_object=f"archive/{DATA_FILE_PREFIX_2}",
        move_object=True,
        retries=0,
    )

    stage_file_2 = GCSToGCSOperator(
        task_id="stage_file_2",
        source_bucket=BUCKET_2,
        source_object=f"{DATA_FILE_PREFIX_2}*",
        destination_bucket=BUCKET_2,
        destination_object=f"stage/{DATA_FILE_PREFIX_2}",
        trigger_rule="all_done",
        move_object=False,
    )
    list_stage_files_2 = GCSListObjectsOperator(
        task_id="list_stage_files_2", bucket=BUCKET_2, prefix=f"stage/{DATA_FILE_PREFIX_2}")

    create_external_table_2 = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_2",
        bucket=BUCKET_2,
        source_objects= list_stage_files_2.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{DATA_FILE_PREFIX_2}_ext",
        skip_leading_rows=1,
        allow_quoted_newlines =True ,
        allow_jagged_rows=True,
        schema_object=SCHEMA_EXT_2,
    )

    create_curated_table_2 = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table_2",
        dataset_id=DATASET_CURATED,
        table_id=f"{DATA_FILE_PREFIX_2}_curated",
        gcs_schema_object=f"gs://{BUCKET_2}/{SCHEMA_EXT_2}",
        location=BQ_LOCATION,
    )

    execute_insert_curated_2 = BigQueryInsertJobOperator(
        task_id="execute_insert_curated_2",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_CURATED_2,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    create_reporting_table_2 = BigQueryCreateEmptyTableOperator(
        task_id="create_reporting_table_2",
        dataset_id=DATASET_REPORT,
        table_id=f"{DATA_FILE_PREFIX_2}_report",
        gcs_schema_object=f"gs://{BUCKET_2}/{SCHEMA_EXT_2}",
        location=BQ_LOCATION,
    )

    execute_insert_report_2 = BigQueryInsertJobOperator(
        task_id="execute_insert_report_2",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_REPORT_2,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    

    delete_files_2 = GCSDeleteObjectsOperator(
        task_id="delete_files_2", bucket_name=BUCKET_2, objects=list_bucket_files_2.output
    )
    list_bucket_files_3 = GCSListObjectsOperator(
        task_id="list_bucket_files_3", bucket=BUCKET_1, prefix=DATA_FILE_PREFIX_3)

    archive_old_file_3 = GCSToGCSOperator(
        task_id="archive_old_file_3",
        source_bucket=BUCKET_1,
        source_object=f"stage/{DATA_FILE_PREFIX_3}*",
        destination_bucket=BUCKET_1,
        destination_object=f"archive/{DATA_FILE_PREFIX_3}",
        move_object=True,
        retries=0,
    )

    stage_file_3 = GCSToGCSOperator(
        task_id="stage_file_3",
        source_bucket=BUCKET_1,
        source_object=f"{DATA_FILE_PREFIX_3}*",
        destination_bucket=BUCKET_1,
        destination_object=f"stage/{DATA_FILE_PREFIX_3}",
        trigger_rule="all_done",
        move_object=False,
    )
    list_stage_files_3 = GCSListObjectsOperator(
        task_id="list_stage_files_3", bucket=BUCKET_1, prefix=f"stage/{DATA_FILE_PREFIX_3}")

    create_external_table_3 = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_3",
        bucket=BUCKET_1,
        source_objects= list_stage_files_3.output,
        destination_project_dataset_table=f"{DATASET_STAGE}.{DATA_FILE_PREFIX_3}_ext",
        skip_leading_rows=1,
        allow_quoted_newlines =True ,
        allow_jagged_rows=True,
        schema_object=SCHEMA_EXT_3,
    )

    create_curated_table_3 = BigQueryCreateEmptyTableOperator(
        task_id="create_curated_table_3",
        dataset_id=DATASET_CURATED,
        table_id=f"{DATA_FILE_PREFIX_3}_curated",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_EXT_3}",
        location=BQ_LOCATION,
    )

    execute_insert_curated_3 = BigQueryInsertJobOperator(
        task_id="execute_insert_curated_3",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_CURATED_3,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    create_reporting_table_3 = BigQueryCreateEmptyTableOperator(
        task_id="create_reporting_table_3",
        dataset_id=DATASET_REPORT,
        table_id=f"{DATA_FILE_PREFIX_3}_report",
        gcs_schema_object=f"gs://{BUCKET_1}/{SCHEMA_EXT_3}",
        location=BQ_LOCATION,
    )

    execute_insert_report_3 = BigQueryInsertJobOperator(
        task_id="execute_insert_report_3",
        configuration={
                "query": {
                    "query": INSERT_ROWS_QUERY_REPORT_3,
                    "useLegacySql": False,
                }
        },
        location=BQ_LOCATION,
    )
    

    delete_files_3 = GCSDeleteObjectsOperator(
        task_id="delete_files_3", bucket_name=BUCKET_1, objects=list_bucket_files_3.output
    )


gcs_object_with_prefix_exists >> split_file >> list_bucket_files_1 >> archive_old_file_1 >> stage_file_1 >> list_stage_files_1 >> create_external_table_1 >> create_curated_table_1 >> execute_insert_curated_1 >> create_reporting_table_1 >> execute_insert_report_1 >> delete_files_1 >> list_bucket_files_2 >> archive_old_file_2 >> stage_file_2 >> list_stage_files_2 >> create_external_table_2 >> create_curated_table_2 >> execute_insert_curated_2 >> create_reporting_table_2 >> execute_insert_report_2 >> delete_files_2 >> list_bucket_files_3 >> archive_old_file_3 >> stage_file_3 >> list_stage_files_3 >> create_external_table_3 >> create_curated_table_3 >> execute_insert_curated_3 >> create_reporting_table_3 >> execute_insert_report_3 >> delete_files_3


if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
