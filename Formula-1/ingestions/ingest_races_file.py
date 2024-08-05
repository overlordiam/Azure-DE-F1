# Databricks notebook source
# MAGIC %run "../includes/utils"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add 'data_src' widget

# COMMAND ----------

dbutils.widgets.text('data_src', '')
data_src = dbutils.widgets.get('data_src')

# COMMAND ----------

dbutils.widgets.text('file_date', '')

# COMMAND ----------

data_src = dbutils.widgets.get('data_src')
file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema for race table

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("year", IntegerType(), True),
                                 StructField("round", IntegerType(), True),
                                 StructField("circuitId", IntegerType(), True),
                                 StructField("name", StringType(), True),
                                 StructField("date", DateType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read csv file from mounted datalake

# COMMAND ----------

race_df = spark.read.csv(f"{raw_folder_path}/{file_date}/races.csv", header=True, schema=race_schema)
race_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

from pyspark.sql.functions import lit, col

renamed_race_df = race_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
        .withColumnRenamed("circuitId", "circuit_id") \
            .withColumn('data_source', lit(data_src)) \
                .withColumn('file_date', lit(file_date))

renamed_race_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add columns called ingestion_date and race_timestamp(combination of date and time)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat
final_race_df = renamed_race_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

display(final_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove unncessary columns from race table

# COMMAND ----------

final_race_df.columns

# COMMAND ----------

selected_race_df = final_race_df.select('race_id',
 'race_year',
 'round',
 'circuit_id',
 'name',
 'ingestion_date',
 'race_timestamp',
 'file_date')

# COMMAND ----------

display(selected_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake

# COMMAND ----------

selected_race_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

# display(spark.read.parquet("/mnt/datalakeforprojformula1/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success!!")

# COMMAND ----------


