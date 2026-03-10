from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# 1. Fonction Python qui simule l'extraction (Extract)
def extract_data_from_api():
    # URL d'une API e-commerce (qui renvoie des infos sur des produits)
    url = "https://fakestoreapi.com/products"
    
    # On utilise Pandas pour lire les données JSON directement depuis l'API
    df = pd.read_json(url)

    # On s'assure que le dossier 'data' existe sur notre machine
    os.makedirs("data", exist_ok=True)

    # On sauvegarde les données brutes dans notre dossier local (Couche Bronze)
    file_path = "data/raw_products.csv"
    df.to_csv(file_path, index=False)
    
    print(f"Succès : {len(df)} produits extraits et sauvegardés dans {file_path}")

# 2. Configuration par défaut de notre pipeline (DAG)
default_args = {
    'owner': 'dimitri',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# 3. Définition du DAG (Le chef d'orchestre)
with DAG(
    'azure_ecommerce_pipeline',
    default_args=default_args,
    schedule_interval='@daily', # S'exécute tous les jours
    catchup=False,
    description='Pipeline de bout en bout pour extraire des donnees e-commerce'
) as dag:

    # 4. Définition de notre première Tâche (Task)
    task_extract = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_data_from_api
    )

    # L'ordre d'exécution (pour l'instant on a qu'une tâche, on en ajoutera d'autres !)
    task_extract