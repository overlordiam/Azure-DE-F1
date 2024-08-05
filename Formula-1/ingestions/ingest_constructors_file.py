# Databricks notebook source
# MAGIC %run "../includes/utils"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add 'data_src' widget

# COMMAND ----------

dbutils.widgets.text('data_src', '')

# COMMAND ----------

dbutils.widgets.text('file_date', '')

# COMMAND ----------

data_src = dbutils.widgets.get('data_src')
file_date = dbutils.widgets.get('file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema for constructors table

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data from json file

# COMMAND ----------

constructor_df = spark.read.json(path=f'{raw_folder_path}/{file_date}/constructors.json', schema=constructors_schema)
display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop the 'url' column

# COMMAND ----------

dropped_constructor_df = constructor_df.drop('url')
dropped_constructor_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit
final_constructor_df = dropped_constructor_df.select(col("constructorId").alias("constructor_id"),
                                                     col("constructorRef").alias("constructor_ref"),
                                                     col("name"), col("nationality")) \
                     .withColumn("ingestion_date", current_timestamp()) \
                         .withColumn('data_source', lit(data_src)) \
                            .withColumn('file_date', lit(file_date))

display(final_constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to data lake

# COMMAND ----------

final_constructor_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success!!")
