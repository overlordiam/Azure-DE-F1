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
# MAGIC #### Schema for pit_stops table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read from data lake

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add 'ingestion_date'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn('data_source', lit(data_src)) \
.withColumn('file_date', lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to data lake

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(final_df, "f1_processed", "pit_stops", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(race_id)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success!!")
