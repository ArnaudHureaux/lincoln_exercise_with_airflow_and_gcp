from email import utils
import pandas as pd
import os
import json

from google.cloud import storage
from google.oauth2 import service_account

import utils

bucket_name = "servier_test_technique"
path_to_token = "gcp_account_service_key.json"


def _pubmed_clean():
    # IMPORT
    df1 = utils.importFileGcpStorage(
        bucket_name, 'datasources/pubmed.csv', path_to_token)
    dicte = utils.importFileGcpStorageJSON(
        bucket_name, 'datasources/pubmed.json', path_to_token)
    # TRANSFORMATION
    df2 = pd.DataFrame(dicte['data'])
    df = pd.concat((df1, df2)).reset_index(drop=True)
    df = df.drop_duplicates()
    df['pk'] = df['title']+'_'+df['journal']
    # EXPORT
    utils.exportFileGcpStorage(
        df, bucket_name, 'clean/pubmed_clean.csv', path_to_token)
