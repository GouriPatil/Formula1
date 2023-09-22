# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import sum, when,col,when,count
driver_standings_df = race_results_df \
                         .groupBy("race_year", "driver_name","driver_nationality","team")\
                          .agg(sum("points").alias("total_points"),
                              count(when(col("position")==1, True)).alias("wins"))
display(driver_standings_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank, asc
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))
display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")