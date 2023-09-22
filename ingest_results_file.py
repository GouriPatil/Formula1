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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False ),
                                 StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("grid", DateType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     StructField("fastestLap", IntegerType(), True),
                                     StructField("rank", IntegerType(), True),
                                     StructField("fastestLapTime", StringType(), True),
                                     StructField("fastestLapSpeed", FloatType(), True),
                                     StructField("statusId", StringType(), True)
])    
 

# COMMAND ----------

container_name = "raw"
results_df = spark.read.option("header", True).schema(results_schema).json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_dropped_df = results_df.drop("statusId")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_dropped_df.withColumnRenamed("resultId","result_id") \
                                                   .withColumnRenamed("raceId","race_id") \
                                                   .withColumnRenamed("driverId","driver_id") \
                                                   .withColumnRenamed("constructorId","constructor_id") \
                                                   .withColumnRenamed("fastestLap","fastest_lap") \
                                                   .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                                   .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
                                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

 results_renamed_df=  results_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy('race_id').parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/results")

# COMMAND ----------

display(spark.read.parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/results"))

# COMMAND ----------

dbutils.notebook.exit("success")