# Databricks notebook source
# MAGIC %run "../includes/utils"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add 'data_src' widget

# COMMAND ----------

dbutils.widgets.text('data_src', '')

# COMMAND ----------

dbutils.widgets.text('file_date', '')

# COMMAND ----------

data_src = dbutils.widgets.get('data_src')
file_date = dbutils.widgets.get('file_date')
data_src, file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema for 'qualifying' table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read from data lake

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add 'ingestion_date'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('data_source', lit(data_src)) \
.withColumn('file_date', lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to data lake

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.qualify_id = src.qualify_id"
merge_delta_data(final_df, "f1_processed", "qualifying", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(race_id)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id desc

# COMMAND ----------

# display(spark.read.parquet('/mnt/datalakeforprojformula1/processed/lap_times'))

# COMMAND ----------

dbutils.notebook.exit("Success!!")
