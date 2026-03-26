# Databricks notebook source
# MAGIC %md
# MAGIC ##Versions tracking##

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone 
# MAGIC SET Borough = 'EMR' WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip_zone WHERE LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC #Time Travel in Delta Lake#

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.trip_zone to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC *Trip Data*

# COMMAND ----------

