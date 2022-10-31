import pandas as pd
import os
import json

from google.cloud import storage
from google.oauth2 import service_account

bucket_name = "servier_test_technique"
path_to_token = "gcp_account_service_key.json"


def exportFileGcpStorage(df, bucket_name, file_name, path_to_token):
    storage_credentials = service_account.Credentials.from_service_account_file(
        path_to_token)
    storage_client = storage.Client(
        project=bucket_name, credentials=storage_credentials)
    destination_bucket = storage_client.bucket(bucket_name)
    blob = destination_bucket.blob(
        file_name).upload_from_string(df.to_csv(), 'text/csv')


def importFileGcpStorage(bucket_name, file_name, path_to_token):
    return pd.read_csv('gs://'+bucket_name+'/'+file_name, storage_options={"token": path_to_token})


def importFileGcpStorageJSON(bucket_name, file_name, path_to_token):
    storage_credentials = service_account.Credentials.from_service_account_file(
        path_to_token)
    storage_client = storage.Client(
        project=bucket_name, credentials=storage_credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    dicte = json.loads(blob.download_as_string(client=None))
    return dicte


def exportFileGcpStorageJSON(dicte, bucket_name, file_name, path_to_token):
    storage_credentials = service_account.Credentials.from_service_account_file(
        path_to_token)
    storage_client = storage.Client(
        project=bucket_name, credentials=storage_credentials)
    destination_bucket = storage_client.bucket(bucket_name)
    blob = destination_bucket.blob(file_name).upload_from_string(
        str(dicte), 'application/json')
