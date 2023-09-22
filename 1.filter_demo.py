# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_foler_path}.races")

# COMMAND ----------

