# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Pipeline d'Ingestion Ventes : Bronze vers Silver (Delta Lake)
# MAGIC Ce notebook simule un pipeline DataOps d'ingestion PySpark en production.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("SalesDataOpsPipeline").getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Étape 1 : Initialisation de la table Delta Cible (Silver)
# MAGIC La table cible attend des prix unitaires typés en DOUBLE.

# COMMAND ----------
# Schéma nominal strict de la table Silver
silver_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("quantity", IntegerType(), False)
])

# Création des données de référence (Lot 1)
initial_data = [
    ("TXN_1001", 10, 24.50, 2),
    ("TXN_1002", 12, 110.00, 1),
    ("TXN_1003", 10, 15.99, 4)
]

df_initial = spark.createDataFrame(initial_data, silver_schema)
df_initial.write.format("delta").mode("overwrite").save("/tmp/delta/silver_sales")
print("Table Delta /tmp/delta/silver_sales initialisée avec succès.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Étape 2 : Ingestion du Lot 2 (Erreur de Schema Drift injectée)
# MAGIC Le système source envoie accidentellement des chaînes de caractères ('Utf8') au lieu de Double sur 'unit_price'.

# COMMAND ----------
# Données corrompues en amont (la source a envoyé des strings)
bad_batch_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("unit_price", StringType(), False),  # <-- ERREUR ICI : Type String au lieu de Double
    StructField("quantity", IntegerType(), False)
])

bad_data = [
    ("TXN_1004", 10, "34.99", 1),
    ("TXN_1005", 14, "INVALID_PRICE", 3)  # Valeur impossible à convertir
]

df_bad = spark.createDataFrame(bad_data, bad_batch_schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Étape 3 : Tentative d'écriture (Déclenchement du crash)
# MAGIC L'écriture Delta Lake va échouer avec une AnalysisException de schéma incompatible.

# COMMAND ----------
print("Tentative d'écriture du lot corrompu dans Delta Lake...")

# Cette opération va échouer avec AnalysisException (Schema Mismatch) que notre agent Foundry va analyser
df_bad.write.format("delta").mode("append").save("/tmp/delta/silver_sales")
