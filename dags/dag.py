from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta
from clean.drugs_clean import _drugs_clean
from clean.pubmed_clean import _pubmed_clean
from clean.clinical_trials_clean import _clinical_trials_clean
from aggregation.drugs_json_extraction import _drugs_json_extraction

default_args = {
    'retry': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(dag_id='drug_dags',
         schedule_interval="@daily",
         start_date=days_ago(2),
         catchup=True,
         max_active_runs=2,
         default_args=default_args
         ) as dag:

    drugs_clean_task = PythonOperator(
        task_id='drugs_clean',
        python_callable=_drugs_clean)

    pubmed_clean_task = PythonOperator(
        task_id='pubmed_clean',
        python_callable=_pubmed_clean)

    clinical_trials_clean_task = PythonOperator(
        task_id='clinical_trials_clean',
        python_callable=_clinical_trials_clean)

    drugs_json_extraction_task = PythonOperator(
        task_id='drugs_json_extraction',
        python_callable=_drugs_json_extraction)

    [drugs_clean_task, pubmed_clean_task,
        clinical_trials_clean_task] >> drugs_json_extraction_task
