# Databricks notebook source
storage_account_name = "formula1dlforcourse"
storage_account_key  = "y/U+Uv4GT7IW1ISQ+o1fBGh8gV1xckkftpgh67buA5pMKbFINwnb6tT1VrMO9fbwihKDBqmfE68n+AStTkejMg=="

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    f"{storage_account_key}")

# COMMAND ----------

container_name = "raw"
raw_folder_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
processed_folder_path = f"abfss://processed@{storage_account_name}.dfs.core.windows.net"
presentation_folder_path = f"abfss://presentation@{storage_account_name}.dfs.core.windows.net"

# COMMAND ----------

