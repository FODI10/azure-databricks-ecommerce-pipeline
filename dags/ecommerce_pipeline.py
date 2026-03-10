from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# On dit à Airflow d'aller chercher la fonction dans le dossier 'src'
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from src.extract import extract_data_from_api

default_args = {
    'owner': 'dimitri',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'azure_ecommerce_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Pipeline de bout en bout pour extraire des donnees e-commerce'
) as dag:

    task_extract = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_data_from_api
    )

    task_extract