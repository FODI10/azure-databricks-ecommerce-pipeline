import pandas as pd
import os
import requests

def extract_data_from_api():
    url = "https://fakestoreapi.com/products"
    
    # On ajoute des headers pour simuler un navigateur réel (évite l'erreur 403)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    print(f"Tentative d'extraction depuis {url}...")
    
    # On utilise requests pour obtenir la donnée avec nos headers
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # On transforme le JSON reçu en DataFrame Pandas
        df = pd.DataFrame(response.json())

        # On s'assure que le dossier 'data' existe
        os.makedirs("data", exist_ok=True)

        # On sauvegarde
        file_path = "data/raw_products.csv"
        df.to_csv(file_path, index=False)
        print(f"Succès : {len(df)} produits sauvegardés dans {file_path}")
    else:
        print(f"Erreur d'extraction : Code {response.status_code}")

if __name__ == "__main__":
    extract_data_from_api()