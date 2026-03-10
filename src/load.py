import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# On charge les variables du fichier .env
load_dotenv()

def upload_to_azure():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    
    # On se connecte au Data Lake
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    
    # On définit le nom de notre conteneur 'bronze'
    file_system_client = service_client.get_file_system_client(file_system="bronze")
    
    # On définit le chemin dans le cloud
    directory_client = file_system_client.get_directory_client("raw_data")
    file_client = directory_client.get_file_client("products_api.csv")
    
    # On lit le fichier local et on l'envoie
    local_file_path = "data/raw_products.csv"
    with open(local_file_path, "rb") as data:
        file_client.upload_data(data, overwrite=True)
        print(f"Succès : {local_file_path} a été téléversé vers Azure dans le conteneur 'bronze'.")

if __name__ == "__main__":
    upload_to_azure()