# Databricks notebook source
# 00_Setup_Environment.py
# Environment Setup: Real-Time Fintech Transaction Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC # Environment Setup
# MAGIC This notebook creates the Unity Catalog infrastructure needed for the lab and generates the initial synthetic data.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Catalog and Schemas
# MAGIC Setting up the three-level namespace structure.

# COMMAND ----------

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")

# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SYSTEM_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Volumes
# MAGIC Volumes are needed for raw file storage and streaming checkpoints.

# COMMAND ----------

# Landing volume for raw files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.raw_data")

# System volume for checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SYSTEM_SCHEMA}.checkpoints")

print(f"✅ Unity Catalog structure created for {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean and Populate Data
# MAGIC Removing any previous data and running the generator.

# COMMAND ----------

# Clean up existing tables from previous runs
for table in [TABLE_BRONZE_TRANSACTIONS, TABLE_SILVER_TRANSACTIONS, TABLE_SILVER_ALERTS,
              TABLE_SILVER_CUSTOMERS, TABLE_SILVER_MERCHANTS,
              TABLE_GOLD_DAILY_SUMMARY, TABLE_GOLD_SAR_PREFILL]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

# Clean up existing data in volumes if any
dbutils.fs.rm(LANDING_VOLUME_PATH, recurse=True)
dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)

# Create landing subdirectories
dbutils.fs.mkdirs(TRANSACTIONS_RAW_PATH)
dbutils.fs.mkdirs(CUSTOMERS_RAW_PATH)
dbutils.fs.mkdirs(MERCHANTS_RAW_PATH)

# Run the data generator in bootstrap mode
dbutils.widgets.text("action", "bootstrap")

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final Confirmation
# MAGIC Verify that the infrastructure is ready.

# COMMAND ----------

# Quick check on landing files
try:
    files = dbutils.fs.ls(TRANSACTIONS_RAW_PATH)
    print(f"✅ Found {len(files)} transaction batch files in landing.")
except:
    print("❌ Transaction landing files not found!")

print("🎉 Environment setup complete!")
