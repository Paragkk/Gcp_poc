import os
from smart_open import open
from google.cloud import storage
import shlex
import pandas as pd
from google.cloud import bigquery
from csv import reader
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = file_apth
LOCAL_FILE = "Y"
LOCAL_FILE_PATH = file_path
FILE_PATTERN = "secure"
BUCKET_NAME = "poc-data"
PROJECT_NAME = "migration-service"
DATASET_NAME = "stage"
TABLE_NAME = "tablename"

def write_to_bq(data_string):
    df=pd.DataFrame(data_string)
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

def read_file_lines(file_path):
    data=[]
    for line in open(file_path, 'r'):
        patt_server = re.compile(r'itsu[^\s]+\b')
        patt_time = re.compile(r'\b.+:\d{2}\b')
        patt_key_val = re.compile(r'\b[^\s(]+=[^\s]+\b')
        data_server=patt_server.findall(line)
        for x in range (0,len(data_server)):
            data_server[x]="server="+data_server[x]
        data_time=patt_time.findall(line)
        for x in range (0,len(data_time)):
            data_time[x]="time="+data_time[x]
        data_key_val=patt_key_val.findall(line)
        data_c=data_time+data_server+data_key_val
        line2json = dict(item.split("=",1) for item in data_c)
        data.append(line2json)
    return data
if LOCAL_FILE == "Y":
    for filename in os.scandir(LOCAL_FILE_PATH):
        if filename.name.startswith(FILE_PATTERN):
            print((filename.path))
            data = read_file_lines(filename.path)
            write_to_bq (data)
else:
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    for blob_name in storage_client.list_blobs(BUCKET_NAME, prefix=FILE_PATTERN):
        print(str(blob_name.name))
        data = read_file_lines(f"gs://{BUCKET_NAME}//{blob_name.name}")
