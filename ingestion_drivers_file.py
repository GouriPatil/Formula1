# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 1 :Read JSON using spark reader API

# COMMAND ----------

storage_account_name = "formula1dlforcourse"
storage_account_key  = "y/U+Uv4GT7IW1ISQ+o1fBGh8gV1xckkftpgh67buA5pMKbFINwnb6tT1VrMO9fbwihKDBqmfE68n+AStTkejMg=="
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True),
                                StructField("surname", StringType(), True)
                                ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", StringType(),True),
                                StructField("driverRef", StringType(), True),
                                StructField("number", IntegerType(), True),
                                StructField("code", StringType(), True),
                                StructField("name", name_schema, True),
                                StructField("nationality", StringType(), True),  
                                StructField("url", StringType(), True),
                                ])

# COMMAND ----------

container_name = "raw"
drivers_df = spark.read.option("header", True).schema(drivers_schema).json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename column and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,concat, lit

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId","driver_id") \
                               .withColumnRenamed("driverRef","driver_ref") \
                               .withColumn("ingestion_date",current_timestamp()) \
                               .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3: Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url")) 

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/drivers")

# COMMAND ----------

display(spark.read.parquet(f"abfss://processed@{storage_account_name}.dfs.core.windows.net/drivers"))

# COMMAND ----------

dbutils.notebook.exit("success")