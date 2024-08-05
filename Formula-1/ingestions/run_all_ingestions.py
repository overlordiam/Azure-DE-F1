# Databricks notebook source
result = dbutils.notebook.run('ingest_circuits_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_constructors_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_drivers_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_lap_times_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_pit_stops_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_qualifying_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_races_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

result = dbutils.notebook.run('ingest_results_file', 0, {'data_src': 'Ergast API', 'file_date': '2021-03-21'})

# COMMAND ----------

result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
