# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading, Writing and Creating Delta Tables#

# COMMAND ----------

# MAGIC %md
# MAGIC **Authorization**

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.jpprojectdatastorageadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.jpprojectdatastorageadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.jpprojectdatastorageadls.dfs.core.windows.net", "b6996712-9cc3-4769-87fd-e136fcda79da")
spark.conf.set("fs.azure.account.oauth2.client.secret.jpprojectdatastorageadls.dfs.core.windows.net", 'M9H8Q~62GzMbpcOuNxVGITAeZCxUH~mDqP63~bB~')
spark.conf.set("fs.azure.account.oauth2.client.endpoint.jpprojectdatastorageadls.dfs.core.windows.net", "https://login.microsoftonline.com/ed6f6a72-88ea-486c-90d9-12cf3bea00d2/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Database Creation_Goldlayer**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold

# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variable**

# COMMAND ----------

silver = 'abfss://silverlayer@jpprojectdatastorageadls.dfs.core.windows.net'
gold = 'abfss://goldlayer@jpprojectdatastorageadls.dfs.core.windows.net'

# COMMAND ----------

df_trip_zone =  spark.read.format('parquet')\
    .option('inferSchema',True)\
        .option('header',True)\
            .load(f'{silver}/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing data to gold layer as delta table**

# COMMAND ----------

# MAGIC %md
# MAGIC *Creating external location*

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION gold_loc
# MAGIC     URL 'abfss://goldlayer@jpprojectdatastorageadls.dfs.core.windows.net/'
# MAGIC     WITH (STORAGE CREDENTIAL gold_storage_cred);

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS

# COMMAND ----------

# MAGIC %md
# MAGIC *Writing TRIP ZONE data to gold layer*

# COMMAND ----------

df_trip_zone.write.format("delta") \
    .mode("append") \
    .option("path", f'{gold}/trip_zone') \
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC *Writing TRIP DATA to gold layer*

# COMMAND ----------

df_trip =  spark.read.format('parquet')\
    .option('inferSchema',True)\
        .option('header',True)\
            .load(f'{silver}/trip_data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format("delta") \
    .mode("append") \
    .option("path", f'{gold}/trip_data') \
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold.trip_data
# MAGIC USING delta
# MAGIC LOCATION 'abfss://goldlayer@jpprojectdatastorageadls.dfs.core.windows.net/trip_data';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_data

# COMMAND ----------

