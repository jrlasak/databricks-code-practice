# Databricks notebook source
# MAGIC %md
# MAGIC # 03: Cleanup
# MAGIC Run this notebook to drop the `delta_optimization_project` catalog and every table in it. Run it only when you are done: afterwards `00_Setup_Environment` has to write the 10M-row `sales_raw` again.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Drop the catalog and everything in it
spark.sql(f"DROP CATALOG IF EXISTS {CATALOG_NAME} CASCADE")
print(f"Dropped catalog {CATALOG_NAME}")
