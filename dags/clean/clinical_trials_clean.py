from email import utils
import pandas as pd
import os
import json

from google.cloud import storage
from google.oauth2 import service_account

import utils

bucket_name = "servier_test_technique"
path_to_token = "gcp_account_service_key.json"


def _clinical_trials_clean():
    # IMPORT
    df = utils.importFileGcpStorage(
        bucket_name, 'datasources/clinical_trials.csv', path_to_token)
    # TRANSFORMATION
    df = df[(df['scientific_title'].notna()) & (
        df['scientific_title'].apply(lambda x: x.replace(' ', '')) != '')]
    df = df[df['journal'].notna()]
    df['journal'] = df['journal'].apply(lambda x: x.replace('\xc3\x28', ''))
    df = df.rename(columns={'scientific_title': 'title'})
    df['pk'] = df['title']+'_'+df['journal']
    # EXPORT
    utils.exportFileGcpStorage(
        df, bucket_name, 'clean/clinical_trials_clean.csv', path_to_token)
