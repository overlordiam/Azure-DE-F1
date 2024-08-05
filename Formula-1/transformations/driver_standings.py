# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/utils"

# COMMAND ----------

dbutils.widgets.text('file_date', '')

# COMMAND ----------

file_date = dbutils.widgets.get('file_date')
file_date

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read race_results parquet from data lake (only the records that has to be re-processed)

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                                .filter(f"file_date = '{file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add 2 new aggregate columns 'total_points' and 'wins'

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum(col("points")).alias("total_points"), 
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020").orderBy("total_points", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add a 'rank' column using windows method

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, desc

rank_func = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(rank_func))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to data lake

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, "f1_presentation", "driver_standings", "race_year", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings ORDER BY race_year DESC

# COMMAND ----------


