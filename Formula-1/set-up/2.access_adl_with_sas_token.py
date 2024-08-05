# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="formula1-scope", key="sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakeforprojformula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.datalakeforprojformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.datalakeforprojformula1.dfs.core.windows.net", sas_token)

# COMMAND ----------

dbutils.fs.ls('abfss://test@datalakeforprojformula1.dfs.core.windows.net')

# COMMAND ----------

display(spark.read.csv("abfss://test@datalakeforprojformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


