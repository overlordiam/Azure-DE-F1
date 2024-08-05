# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/utils"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add data source widget

# COMMAND ----------

dbutils.widgets.text("data_src", "")

# COMMAND ----------

dbutils.widgets.text("file_date", "")

# COMMAND ----------

data_src = dbutils.widgets.get('data_src')
file_date = dbutils.widgets.get('file_date')
data_src, file_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a schema for the circuits table

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ### We do not use the inferSchema parameter because:
# MAGIC - spark goes through the entire table to infer the datatypes, which is time consuming in a sense and slows down the read process.
# MAGIC - better to have a fixed schema for the incoming data as a way to perform static checks

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder_path}/{file_date}/circuits.csv', header=True, schema=circuit_schema)
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop the 'url' column as it does not add any value to the table

# COMMAND ----------

from pyspark.sql.functions import col, lit
selected_circuit_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"),
                                         col("country"), col("lat"), col("lng"), col("alt"))
display(selected_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns to a more apt name

# COMMAND ----------

renamed_circuits_df = selected_circuit_df.withColumnRenamed("circuitId", "circuit_id") \
  .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
      .withColumnRenamed("lng", "longitude") \
        .withColumnRenamed("alt", "altitude") \
          .withColumn("data_source", lit(data_src)) \
            .withColumn("file_date", lit(file_date))
display(renamed_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a column called 'ingestion_date'. 
# MAGIC Note: We can use the 'lit' method to add a constant or a literal to all rows of a column

# COMMAND ----------

final_circuits_df = add_ingestion_date(renamed_circuits_df)
display(final_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write processed data to data lake as a parquet file

# COMMAND ----------

final_circuits_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success!!")
