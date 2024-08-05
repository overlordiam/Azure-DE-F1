# Databricks notebook source
dbutils.secrets.list("formula1-scope")

# COMMAND ----------

CLIENT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-id")
TENANT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-tenant-id")
CLIENT_SECRET = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.datalakeforprojformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakeforprojformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakeforprojformula1.dfs.core.windows.net", CLIENT_ID)
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakeforprojformula1.dfs.core.windows.net", CLIENT_SECRET)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakeforprojformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls('abfss://test@datalakeforprojformula1.dfs.core.windows.net')

# COMMAND ----------

display(spark.read.csv('abfss://test@datalakeforprojformula1.dfs.core.windows.net/circuits.csv'))

# COMMAND ----------


