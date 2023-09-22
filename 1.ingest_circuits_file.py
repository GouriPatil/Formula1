# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

#md
### ingest circuits.csv file

# COMMAND ----------

#storage_account_name = "formula1dlforcourse"
#storage_account_key  = "y/U+Uv4GT7IW1ISQ+o1fBGh8gV1xckkftpgh67buA5pMKbFINwnb6tT1VrMO9fbwihKDBqmfE68n+AStTkejMg=="

# COMMAND ----------

#spark.conf.set(
   # f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    #f"{storage_account_key}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", DoubleType(), True),
                                     StructField("url", StringType(), True),
    
])

# COMMAND ----------

container_name = "raw"
circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required coloumns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef","name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef, circuits_df.name,circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"], 
                                          circuits_df["name"],circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"), 
                                          col("name"),col("location"), col("country").alias("race_country"),col("lat"),col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename coloumns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df_renamed = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumnRenamed("race_country", "country").withColumn("data_source", lit(v_data_source))


                        

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_df_final = circuits_df_renamed.withColumn("Ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_df_final)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Write data to datalake as parquet

# COMMAND ----------

#circuits_df_final.write.mode("overwrite").parquet("/mnt/formula1dlforcourse/processed/circuits")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dlforcourse/processed/circuits"))


# COMMAND ----------

circuits_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")