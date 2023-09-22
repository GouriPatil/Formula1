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

qualify_schema = StructType(fields=[StructField("qualifyID", IntegerType(), True),
                                     StructField("raceId", IntegerType(), False ),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                
])    

# COMMAND ----------

container_name = "raw"
qualify_df = spark.read.option("multiLine", True).schema(qualify_schema).json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/qualifying")


# COMMAND ----------

display(qualify_df)


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

qualify_df_final = qualify_df.withColumnRenamed("qualifyID","qualify_id") \
                                                    .withColumnRenamed("raceId","race_id") \
                                                   .withColumnRenamed("driverId","driver_id") \
                                                    .withColumnRenamed("constructorId","constructor_id") \
                                                    .withColumn("data_source", lit(v_data_source)) \
                                                   .withColumn("ingestion_date", current_timestamp())
                                                   

# COMMAND ----------

qualify_df_final.write.mode("overwrite").parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

