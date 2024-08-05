# Databricks notebook source
access_key = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-accountKey")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.datalakeforprojformula1.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

dbutils.fs.ls('abfss://test@datalakeforprojformula1.dfs.core.windows.net')

# COMMAND ----------


