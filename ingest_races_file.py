# Databricks notebook source
# MAGIC %md
# MAGIC ###ingest races_file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False ),
                                 StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                       StructField("url", StringType(), True),
])    


# COMMAND ----------

container_name = "raw"
races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit, current_timestamp

# COMMAND ----------

races_ingestion_df = races_df.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

races_timestamp_df = races_ingestion_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##renaming and selecting columns
# MAGIC

# COMMAND ----------

races_renamed_df = races_timestamp_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitid", "circuit_id").withColumn("data_source", lit(v_data_source))


# COMMAND ----------

races_selected_df = races_renamed_df.select(col("race_id"),col("race_year"), 
                                          col("round"),col("circuit_id"), col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"))

# COMMAND ----------

# MAGIC %md
# MAGIC #write the output to processed container
# MAGIC

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

#mounting with aad(not availble for student subscription. )
#races_selected_df.write.mode("overwrite").parquet("/mnt/formula1dlforcourse/processed/races")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

