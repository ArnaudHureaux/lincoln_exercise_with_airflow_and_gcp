from email import utils
import pandas as pd
import os
import json

from google.cloud import storage
from google.oauth2 import service_account

import utils

bucket_name = "servier_test_technique"
path_to_token = "gcp_account_service_key.json"


def _drugs_json_extraction():
    # IMPORT
    drug = utils.importFileGcpStorage(
        bucket_name, 'clean/drugs_clean.csv', path_to_token)
    pubmed = utils.importFileGcpStorage(
        bucket_name, 'clean/pubmed_clean.csv', path_to_token)
    clin = utils.importFileGcpStorage(
        bucket_name, 'clean/clinical_trials_clean.csv', path_to_token)
    # TRANSFORM
    drugs = drug['drug'].unique()
    sources = ['pubmed', 'clinical_trial']
    sources_df = {sources[0]: pubmed, sources[1]: clin}
    dicte = {}
    for drug in drugs:
        dicte[drug] = {}
        for source in sources:
            dicte[drug][source] = {}
            df = sources_df[source]
            df['drug_in_title'] = df['title'].apply(
                lambda x: drug.upper() in x.upper())
            df_filter = df[df['drug_in_title']][['title', 'date', 'journal']]
            list_for_dicte = df_filter.values.tolist()
            for k in range(len(list_for_dicte)):
                dicte[drug][source][k] = list_for_dicte[k]
    # EXPORT
    utils.exportFileGcpStorageJSON(
        dicte, bucket_name, 'aggregate/drugs_journal_link_graph.json', path_to_token)
