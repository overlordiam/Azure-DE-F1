# Databricks notebook source
dbutils.secrets.list("formula1-scope")

# COMMAND ----------

CLIENT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-id")
TENANT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-tenant-id")
CLIENT_SECRET = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-secret")

# COMMAND ----------

def mount_storage(storage_name, container_name):
    CLIENT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-id")
    TENANT_ID = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-tenant-id")
    CLIENT_SECRET = dbutils.secrets.get(scope="formula1-scope", key="formula1-dl-client-secret")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": CLIENT_ID,
          "fs.azure.account.oauth2.client.secret": CLIENT_SECRET,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_name}/{container_name}" for mount in dbutils.fs.mounts()):
        print("Already mounted....")
        return
    
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())
    return

# COMMAND ----------

mount_storage('datalakeforprojformula1', 'raw')

# COMMAND ----------

mount_storage('datalakeforprojformula1', 'raw')

# COMMAND ----------

mount_storage('datalakeforprojformula1', 'processed')

# COMMAND ----------

mount_storage('datalakeforprojformula1', 'presentation')

# COMMAND ----------

display(spark.read.csv('/mnt/datalakeforprojformula1/raw/circuits.csv'))
