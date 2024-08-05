# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-accountKey")

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

display(spark.read.csv('/FileStore/tables/circuits.csv'))

# COMMAND ----------


