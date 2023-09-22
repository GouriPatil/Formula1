# Databricks notebook source
v_result=dbutils.notebook.run("1.ingest_circuits_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_constructors_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_results_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_races_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingestion_drivers_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_laptimes_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_pitstops_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("ingest_qualifying_file",0, {"p_data_source":"Ergest API"})

# COMMAND ----------

v_result

# COMMAND ----------

