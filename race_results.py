# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("race_timestamp", "race_date")\
.withColumnRenamed("name", "race_name")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")
results_df = spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time", "race_time")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

display(results_df)
       

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id,"inner")\
                    .select(races_df.race_name, races_df.race_year, races_df.race_date, races_df.race_id, circuits_df.circuit_location)

                

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id,"inner").join(drivers_df, drivers_df.driver_id == results_df.driver_id,"inner").join(constructors_df, constructors_df.constructor_id  == results_df.constructor_id,"inner")
                    
                    

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

final_df = race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").withColumn("created_time", current_timestamp())

# COMMAND ----------

race_results_df

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")