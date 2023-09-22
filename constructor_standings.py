# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import desc,col,sum,when,count
construrtor_standings_df = race_results_df \
                        .groupBy("race_year","team")\
                         .agg(sum("points").alias("total_points"),
                           count(when(col("position")==1, True)).alias("wins"))
display(construrtor_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = construrtor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))
display(final_df.filter("race_year==2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")