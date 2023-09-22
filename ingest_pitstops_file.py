# Databricks notebook source
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

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False ),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                
])    

# COMMAND ----------

container_name = "raw"
pit_stops_df = spark.read.option("multiLine", True).schema(pitstops_schema).json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/pit_stops.json")


# COMMAND ----------

display(pit_stops_df)


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

final_pitstops_renamed = pit_stops_df.withColumnRenamed("raceId","race_id") \
                                                   .withColumnRenamed("driverId","driver_id") \
                                                   .withColumn("data_source", lit(v_data_source)) \
                                                   .withColumn("ingestion_date", current_timestamp())
                                                   

# COMMAND ----------

final_pitstops_renamed.write.mode("overwrite").parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/pit_stops")

# COMMAND ----------

display(spark.read.parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")