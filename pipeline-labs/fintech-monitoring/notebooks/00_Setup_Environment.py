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
# MAGIC Create the catalog and the five schemas of the three-level namespace.

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
# MAGIC Drop previous tables and files, then run the generator.

# COMMAND ----------

# Clean up existing tables from previous runs
for table in [TABLE_BRONZE_TRANSACTIONS, TABLE_SILVER_TRANSACTIONS, TABLE_SILVER_ALERTS,
              TABLE_SILVER_VELOCITY_SPIKES, TABLE_SILVER_AMOUNT_SPIKES,
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
dbutils.fs.mkdirs(MERCHANT_UPDATES_RAW_PATH)

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# Generate the customer and merchant dimensions, the merchant updates file, the suspicious-customer payments and 5 transaction batches
bootstrap()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final Confirmation
# MAGIC Verify that the infrastructure is ready. The cell below should print 110,000 customers (ids `0`-`109999`, so the special customers you meet later, `9990`-`9995`, `9998` and `9999`, are all in the dimension) and 5,000 merchants. It stops with an error if any input is missing.

# COMMAND ----------

# Check the landing data: fail loudly instead of letting notebooks 01-04 run on empty inputs
txn_files = [f for f in dbutils.fs.ls(TRANSACTIONS_RAW_PATH) if not f.name.startswith("_")]
customer_count = spark.read.option("header", "true").csv(CUSTOMERS_RAW_PATH).count()
merchant_count = spark.read.json(MERCHANTS_RAW_PATH).count()
merchant_update_count = spark.read.json(MERCHANT_UPDATES_RAW_PATH).count()

print(f"Transaction files in landing: {len(txn_files)}")
print(f"Customers in landing: {customer_count}")
print(f"Merchants in landing: {merchant_count}")
print(f"Merchant updates in landing: {merchant_update_count}")

if len(txn_files) == 0 or customer_count == 0 or merchant_count == 0 or merchant_update_count == 0:
    raise RuntimeError("❌ Setup incomplete: transaction files, customers, merchants or merchant updates are missing. Re-run this notebook from the top.")

print("🎉 Environment setup complete!")
