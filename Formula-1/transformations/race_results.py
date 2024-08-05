# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/utils"

# COMMAND ----------

dbutils.widgets.text("file_date", "")

# COMMAND ----------

file_date = dbutils.widgets.get('file_date')
file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read required tables (races, drivers, circuits, constructors, results) from data lake

# COMMAND ----------

races_df = spark.read.format("delta").load(f'{processed_folder_path}/races')
drivers_df = spark.read.format("delta").load(f'{processed_folder_path}/drivers')
circuits_df = spark.read.format("delta").load(f'{processed_folder_path}/circuits')
constructors_df = spark.read.format("delta").load(f'{processed_folder_path}/constructors')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Only select the data to be re-processed

# COMMAND ----------

results_df = spark.read.format("delta").load(f'{processed_folder_path}/results') \
                .filter(f"file_date = '{file_date}'")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename some columns from the tables

# COMMAND ----------

races_df = races_df.withColumnRenamed('name', 'race_name') \
                            .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('name', 'driver_name') \
                                .withColumnRenamed('number', 'driver_number') \
                                 .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('name', 'team')

# COMMAND ----------

results_df = results_df.withColumnRenamed('time', 'race_time') \
                                .withColumnRenamed('fastest_lap_time', 'fastest_time') \
                                    .withColumnRenamed('race_id', 'results_race_id') \
                                        .withColumnRenamed('file_date', 'results_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join the races dataframe and the circuits dataframe

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, on=races_df["circuit_id"] == circuits_df["circuit_id"], how="inner")

# COMMAND ----------

race_circuits_df = race_circuits_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join all the other tables with the race_circuits dataframe

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, on=results_df.results_race_id == race_circuits_df.race_id, how="inner") \
                            .join(drivers_df, on=results_df.driver_id == drivers_df.driver_id, how="inner") \
                                .join(constructors_df, on=results_df.constructor_id == constructors_df.constructor_id, how="inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
    .withColumn("created_at", current_timestamp()) \
        .withColumnRenamed("results_file_date", "file_date")


# COMMAND ----------

# %sql
# drop table f1_presentation.race_results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write final dataframe to data lake

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "f1_presentation", "race_results", "race_id", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id desc
