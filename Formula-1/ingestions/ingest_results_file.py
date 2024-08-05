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
# MAGIC #### Schema for results table

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('fastestLap', IntegerType(), True),
                                    StructField('fastestLapSpeed', FloatType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('grid', IntegerType(), True),
                                    StructField('laps', IntegerType(), True),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('points', FloatType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionOrder', IntegerType(), True),
                                    StructField('positionText', StringType(), True),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('rank', IntegerType(), True),
                                    StructField('statusId', IntegerType(), True),
                                    StructField('time', StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read json from data lake

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{file_date}/results.json', schema=results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add 'ingestion_date'

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn('data_source', lit(data_src)) \
                                    .withColumn('file_date', lit(file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop 'status_id'column

# COMMAND ----------

final_results_df = results_with_columns_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop duplicates from table

# COMMAND ----------

final_results_df = final_results_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to datalake

# COMMAND ----------

# MAGIC %md
# MAGIC #### The schema for the table created using 'saveAsTable', is that of the dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1: Drop the partitions of incoming data and append the incoming data freshly. Not the most efficient

# COMMAND ----------


# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     for result_row in final_results_df.select("race_id").distinct().collect():
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id={result_row.race_id})")

# COMMAND ----------

# final_results_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2: Dynamically overwrite the partitions of the new data only.

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(final_results_df, "f1_processed", "results", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, count(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING count(*) > 1
# MAGIC ORDER BY race_id, driver_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(race_id)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success!!")

# COMMAND ----------


