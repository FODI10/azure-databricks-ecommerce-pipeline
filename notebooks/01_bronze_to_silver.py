# Databricks notebook source
# 1. Configuration de la connexion à Azure
storage_account_name = "datalakefodi2026" 
storage_account_key = "MA_CLE_SECRETE_AZURE" 

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# 2. On définit le chemin vers ton fichier dans le conteneur 'bronze'
file_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/raw_data/products_api.csv"

# 3. On lit le fichier CSV avec PySpark
df_bronze = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# 4. On affiche les données sous forme de tableau
display(df_bronze)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. On supprime les doublons stricts
df_cleaned = df_bronze.dropDuplicates()

# 2. On supprime les lignes où il manque un ID (on ne veut pas de produits sans ID)
df_cleaned = df_cleaned.dropna(subset=["id"])

# 3. On ajoute une colonne technique avec la date et l'heure du nettoyage (très pro !)
df_silver = df_cleaned.withColumn("processed_at", current_timestamp())

# 4. On affiche le résultat propre pour vérifier
display(df_silver)

# COMMAND ----------

# 1. On définit le chemin vers ton dossier 'silver' sur Azure
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/products_cleaned"

# 2. On écrit la donnée propre dans Azure au format DELTA
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("✅ SUCCÈS TOTAL : Les données sont nettoyées et stockées dans la couche Silver !")

# COMMAND ----------

from pyspark.sql.functions import count, avg, round

# 1. On regroupe par catégorie pour compter les produits et faire la moyenne des prix
df_gold = df_silver.groupBy("category") \
    .agg(
        count("id").alias("nombre_de_produits"),
        round(avg("price"), 2).alias("prix_moyen")
    )

# 2. On affiche ce magnifique tableau résumé
display(df_gold)

# COMMAND ----------

# 1. Chemin vers ton dossier 'gold' sur Azure
gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/category_stats"

# 2. Sauvegarde au format Delta
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("🏆 ARCHITECTURE COMPLÈTE : La couche Gold est générée et sauvegardée !")

# COMMAND ----------

