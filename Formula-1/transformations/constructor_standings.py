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
# MAGIC #### Read race_results parquet from data lake

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                                .filter(f"file_date = '{file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, desc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add 2 new aggregate columns 'total_points' and 'wins'

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(count(when(col("position") == 1, True)).alias("wins"),
     sum(col("points")).alias("points"))

# COMMAND ----------

display(constructor_standings_df.orderBy(desc("race_year"), desc("points")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add a 'rank' column using windows method

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, desc

rank_func = Window.partitionBy("race_year").orderBy(desc("points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(rank_func))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to data lake

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.team = src.team"
merge_delta_data(final_df, "f1_presentation", "constructor_standings", "race_year", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(*)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year desc

# COMMAND ----------


