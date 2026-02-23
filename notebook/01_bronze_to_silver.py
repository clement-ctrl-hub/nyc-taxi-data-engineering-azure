# ============================================
# NYC Green Taxi - Bronze → Silver
# Azure Databricks Notebook
# ============================================

from pyspark.sql.functions import date_format

# --------------------------------------------
# 1️Configuration de l'accès au Data Lake
# --------------------------------------------

# 
# 
storage_account = "datalaketpdataeng3"

spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    "<YOUR_CLIENT_ID>"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    "<YOUR_CLIENT_SECRET>"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    "https://login.microsoftonline.com/<YOUR_TENANT_ID>/oauth2/token"
)

# --------------------------------------------
# Lecture des données Bronze
# --------------------------------------------

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/trip-data/"

df = spark.read.parquet(bronze_path)

display(df)

# --------------------------------------------
#  Transformation vers Silver
# --------------------------------------------

df_silver = (
    df.withColumn("lpep_pickup_datetime", 
                  date_format("lpep_pickup_datetime", "yyyy-MM-dd"))
      .withColumn("lpep_dropoff_datetime", 
                  date_format("lpep_dropoff_datetime", "yyyy-MM-dd"))
)

# --------------------------------------------
# Écriture dans la couche Silver
# --------------------------------------------

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/trip-data/"

df_silver.write.mode("overwrite").parquet(silver_path)

print(" Données écrites avec succès dans la couche Silver.")