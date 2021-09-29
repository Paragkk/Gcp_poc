import glob
import os
from google.cloud import storage
abs_path =''
def upload_local_directory_to_gcs(local_path, bucket_name):
    assert os.path.isdir(local_path)
    # Setting credentials using the downloaded JSON file
    client = storage.Client.from_service_account_json(json_credentials_path='credentials-python-storage.json')
    # Creating bucket object
    bucket = client.get_bucket(bucket_name)      
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
           global abs_path
           abs_path = local_file[1 + len(local_path):]
           upload_local_directory_to_gcs(local_file, bucket_name)
        else:
           remote_path = abs_path+'/'+local_file[1 + len(local_path):]
           
           #remote_path = f'{"/".join(local_file.split(os.sep)[1:])}'
           print(remote_path)
           # Name of the object to be stored in the bucket
           blob = bucket.blob(remote_path)
           #blob = bucket.blob(local_file[1 + len(local_path):])
           blob.upload_from_filename(local_file)
           
upload_local_directory_to_gcs("C:\\Pythonwork\\data", "kdp-actifiogo-backup")