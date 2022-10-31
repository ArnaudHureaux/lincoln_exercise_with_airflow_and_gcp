from email import utils
import pandas as pd
import os
import json

from google.cloud import storage
from google.oauth2 import service_account

import utils

bucket_name = "servier_test_technique"
path_to_token = "gcp_account_service_key.json"


def _drugs_clean():
    # IMPORT
    df = utils.importFileGcpStorage(bucket_name, 'drugs.csv', path_to_token)
    # TRANSFORMATION
    df = df[df['drug'].notna()]
    df['pk'] = df['drug']
    # EXPORT
    utils.exportFileGcpStorage(
        df, bucket_name, 'clean/drugs_clean.csv', path_to_token)
