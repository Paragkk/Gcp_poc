import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=file_path
import json
import csv
from google.cloud import pubsub_v1
from google.cloud import storage
from concurrent.futures import TimeoutError
import time

# GCP topic, project & subscription ids
PUB_SUB_TOPIC = "stream-data-topic"
PUB_SUB_PROJECT = "db-migration-service"
PUB_SUB_SUBSCRIPTION = "stream-data-topic-sub"
bucket_name = "poc-data"
destination_blob_name = "stream_data/stream.txt"
# Pub/Sub consumer timeout
timeout = 5.0
def read_file_blob(bucket_name, destination_blob_name):
    """Read a file from the bucket."""
 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
 
    # read as string
    read_output = blob.download_as_string()
 
    print(
        "File {} read successfully  from Bucket  {}.".format(
            destination_blob_name, bucket_name
        )
    )
    return read_output.decode()
# callback function for processing consumed payloads 
# prints recieved payload
def process_payload(message):
    print(f"Received {message.data}.")
    message.ack()    

# producer function to push a message to a topic
def push_payload(payload, topic, project):        
        publisher = pubsub_v1.PublisherClient() 
        topic_path = publisher.topic_path(project, topic)        
        data = json.dumps(payload).encode("utf-8")           
        future = publisher.publish(topic_path, data=data)
        print("Pushed message to topic.")   

# consumer function to consume messages from a topics for a given timeout period
def consume_payload(project, subscription, callback, period):
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project, subscription)
        print(f"Listening for messages on {subscription_path}..\n")
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.                
                streaming_pull_future.result(timeout=period)
            except TimeoutError:
                streaming_pull_future.cancel()

# loop to test producer and consumer functions with a 3 second delay
#file1 = open('C:\\Users\\parag_p\\Documents\\jnj\\stream_data.txt', 'r')
#Lines = file1.readlines()
read_blob=read_file_blob(bucket_name, destination_blob_name)
lines = read_blob.split('\n')
for line in lines:    
    print("===================================")
    payload = {"message" : line, "timestamp": time.time()}
    print(f"Sending payload: {payload}.")
    push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
    consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
    time.sleep(10)