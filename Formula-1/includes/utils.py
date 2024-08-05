# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(df):
    return df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

def set_overwrite_conf():
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def rearrange_cols(df, partition_column):
    cols = [col for col in df.columns if col != partition_column]
    cols.append(partition_column)
    df = df.select(cols)
    return df

def overwrite_partition(df, database, table, partition_column):
    set_overwrite_conf()
    df = rearrange_cols(df, partition_column)
    try: 
        if spark._jsparkSession.catalog().tableExists(f"{database}.{table}"):
            df.write.mode("overwrite").insertInto(f"{database}.{table}")
        else:
            df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{database}.{table}")
    except Exception as e:
        print(e)
    print("Overwritten successfully")
    return df

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df, database, table, partition_column, table_container_path, merge_condition):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    if spark._jsparkSession.catalog().tableExists(f"{database}.{table}"):
        delta_table = DeltaTable.forPath(spark, f"{table_container_path}/{table}")
        delta_table.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode("overwrite").format("delta").partitionBy(partition_column).saveAsTable(f"{database}.{table}")

# COMMAND ----------

def df_column_to_list(df, column):
    records = df.select(column).distinct().collect()
    col_list = [row[column] for row in records]
    return col_list
