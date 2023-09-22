# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1: Read CSV file using spark dataframereader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

storage_account_name = "formula1dlforcourse"
storage_account_key  = "y/U+Uv4GT7IW1ISQ+o1fBGh8gV1xckkftpgh67buA5pMKbFINwnb6tT1VrMO9fbwihKDBqmfE68n+AStTkejMg=="

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False ),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                
])    

# COMMAND ----------

container_name = "raw"
lap_times_df = spark.read.schema(laptimes_schema).csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/lap_times")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id") \
                                                   .withColumnRenamed("driverId","driver_id") \
                                                    .withColumn("data_source", lit(v_data_source)) \
                                                   .withColumn("ingestion_date", current_timestamp())
                                                   

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/lap_times")

# COMMAND ----------

#display(spark.read.parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/lap_times"))
display(lap_times_final_df)

# COMMAND ----------

dbutils.notebook.exit("success")