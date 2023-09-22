# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingest constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 1: Read the JSON using spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

container_name = "raw"
constructor_df=spark.read.schema(constructor_schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####drop unwanted columns

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####renamecolumns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
                                             .withColumnRenamed("constructorRef","constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")