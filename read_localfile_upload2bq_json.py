import os
from smart_open import open
from google.cloud import storage
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "jso_file_path"
LOCAL_FILE = "Y"
LOCAL_FILE_PATH = "C:\\Users\\parag_p\\Document\\data files\\"
FILE_PATTERN = "test"
BUCKET_NAME = "poc-data"
PROJECT_NAME = "db-migration-service"
DATASET_NAME = "stage"
TABLE_NAME = "test_json"

def write_to_bq(data_string):
    #df=pd.DataFrame(data_string)
    df=data_string
    job_config = bigquery.LoadJobConfig(
    autodetect = True)
    client = bigquery.Client(project=PROJECT_NAME)
    table_ref = client.dataset(DATASET_NAME).table(TABLE_NAME)
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_ref)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_ref
        )
    )

def keyCleaner(d):
    if type(d) is dict:
        #d_copy = copy.copy(d)
        d_copy={}
        for key, value in d.items():
            if '-' in key:
                d_copy[key.replace('-', '_')] = value
            else:
                d_copy[key] = value   
        return d_copy

    if type(d) is list:
        return map(keyCleaner, d)
    if type(d) is tuple:
        return tuple(map(keyCleaner, d))
    return d

def read_json_file(file_path):
    with open(file_path, 'rb') as file_r:
        pd.set_option('display.max_columns', None)
        data = pd.read_json(file_r, lines=True)
        return data

total_files = 0
Failed_files =0
if LOCAL_FILE == "Y":
    for filename in os.scandir(LOCAL_FILE_PATH):
        if filename.name.startswith(FILE_PATTERN):
            print((filename.path))
            data = read_json_file(filename.path)
            try: 
             total_files=total_files+1
             write_to_bq(data)
            except:
             Failed_files=Failed_files+1
else:
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    for blob_name in storage_client.list_blobs(BUCKET_NAME, prefix=FILE_PATTERN):
        print(str(blob_name.name))
        data = read_json_file(f"gs://{BUCKET_NAME}//{blob_name.name}")
        try: 
         total_files=total_files+1
         write_to_bq(data)
        except:
         Failed_files=Failed_files+1
print("{} files failed out of total {} files".format(Failed_files, total_files))
