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
# MAGIC #### Schema for the drivers table

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/{file_date}/drivers.json").columns

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])
                         
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("number", IntegerType(), True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the driver table from the datalake

# COMMAND ----------

driver_df = spark.read.json(f"{raw_folder_path}/{file_date}/drivers.json", schema=drivers_schema)

# COMMAND ----------

display(driver_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add 'ingestion_date'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col

# COMMAND ----------

renamed_driver_df = driver_df.withColumnRenamed("driverId", "driver_id") \
                              .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn('data_source', lit(data_src)) \
                                      .withColumn('file_date', lit(file_date))


# COMMAND ----------

display(renamed_driver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop 'url' column

# COMMAND ----------

final_driver_df = renamed_driver_df.drop('url')
final_driver_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write into data lake

# COMMAND ----------

final_driver_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success!!")

# COMMAND ----------


