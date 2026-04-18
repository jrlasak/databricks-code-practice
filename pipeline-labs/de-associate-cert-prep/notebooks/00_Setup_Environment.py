# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup for Databricks Data Engineer Certification Lab
# MAGIC This notebook automates the setup of all Unity Catalog objects required for the Databricks Data Engineer Associate certification preparation lab.
# MAGIC ## What This Notebook Does
# MAGIC - Creates a Unity Catalog (`cert_prep_catalog`)
# MAGIC - Creates schemas following the Medallion Architecture (Landing, Bronze, Silver, Gold)
# MAGIC - Creates volumes for raw data storage and system checkpoints
# MAGIC - Verifies the environment is correctly configured
# MAGIC ## Prerequisites
# MAGIC - Databricks Free Edition with Unity Catalog enabled
# MAGIC - Appropriate permissions to create catalogs, schemas, and volumes
# MAGIC - Variables notebook in the same directory
# MAGIC ## Expected Runtime
# MAGIC ~1-2 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Import Configuration Variables

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Create Catalog
# MAGIC Creates the main Unity Catalog container for all lab objects.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} COMMENT 'Catalog for Databricks Data Engineer Associate certification lab'")

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Create Schemas
# MAGIC Creates schemas following the Medallion Architecture pattern:
# MAGIC - **_system**: System objects (checkpoints, metadata)
# MAGIC - **00_landing**: Raw data landing zone
# MAGIC - **01_bronze**: Raw, unprocessed data tables
# MAGIC - **02_silver**: Cleaned and conformed data tables
# MAGIC - **03_gold**: Business-level aggregations and analytics tables
# MAGIC - **dlt_pipeline**: Delta Live Tables pipeline objects

# COMMAND ----------

# Create schemas following Medallion Architecture
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SYSTEM_SCHEMA} COMMENT 'Schema for system objects: checkpoints, metadata'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA} COMMENT 'Landing zone for raw data files'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA} COMMENT 'Bronze layer: raw, unprocessed data'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA} COMMENT 'Silver layer: cleaned, conformed data'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA} COMMENT 'Gold layer: business-level aggregations'")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DLT_SCHEMA} COMMENT 'Schema for Delta Live Tables pipeline'")

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Create Volumes for Data Storage
# MAGIC Creates Unity Catalog volumes for:
# MAGIC - Raw data files (customers, products, sales, events)
# MAGIC - Streaming checkpoints and system metadata

# COMMAND ----------

# Landing volumes for raw data
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.customers COMMENT 'Raw customer JSON files'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.products COMMENT 'Raw product CSV files'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.sales COMMENT 'Raw sales JSON files (streaming)'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.events COMMENT 'Raw web event JSON files (streaming)'")

# System volume for checkpoints
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SYSTEM_SCHEMA}.checkpoints COMMENT 'Checkpoint locations for streaming queries'")

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Verify Environment Setup
# MAGIC Confirms all Unity Catalog objects were created successfully.

# COMMAND ----------

# Verify catalogs
print("✅ Catalog:")
spark.sql(f"SHOW CATALOGS LIKE '{CATALOG_NAME}'").show()

# Verify schemas
print("\n✅ Schemas:")
spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").show()

# Verify volumes
print(f"\n✅ Volumes in {LANDING_SCHEMA}:")
spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{LANDING_SCHEMA}").show()

print(f"\n✅ Volumes in {SYSTEM_SCHEMA}:")
spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.{SYSTEM_SCHEMA}").show()

print("\n" + "="*70)
print("✅ ENVIRONMENT SETUP COMPLETE")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC # ## Generate Synthetic Data
# MAGIC Initialize the landing volumes with synthetic e-commerce data. This step creates realistic customer, product, sales, and event data with intentional quality issues for learning purposes.
# MAGIC
# MAGIC **What will be generated:**
# MAGIC - Customer data (JSON format)
# MAGIC - Product data (CSV format)
# MAGIC - Sales transactions (JSON format, streaming batches)
# MAGIC - Web events (JSON format, streaming batches)
# MAGIC **Estimated Runtime:** ~5-10 minutes

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Generated Data
# MAGIC Check that all data files were successfully created in the landing volumes.

# COMMAND ----------

# Quick verification of generated data
print("📊 Data Generation Summary\n")
print("="*70)

# Check customer data
print("\n✅ Customer Data:")
customer_count = spark.read.format("json").load(CUSTOMERS_LANDING_PATH).count()
print(f"   Total records: {customer_count:,}")

# Check product data  
print("\n✅ Product Data:")
product_count = spark.read.format("csv").option("header", "true").load(PRODUCTS_LANDING_PATH).count()
print(f"   Total records: {product_count:,}")

# Check sales data
print("\n✅ Sales Data:")
sales_count = spark.read.format("json").load(SALES_LANDING_PATH).count()
print(f"   Total records: {sales_count:,}")

# Check events data
print("\n✅ Web Events Data:")
events_count = spark.read.format("json").load(EVENTS_LANDING_PATH).count()
print(f"   Total records: {events_count:,}")

print("\n" + "="*70)
print("✅ ALL DATA SUCCESSFULLY GENERATED!")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Instructions (Optional)
# MAGIC To completely tear down the environment and start fresh, uncomment and run the following SQL command.
# MAGIC ⚠️ **WARNING**: This will delete ALL data, tables, and objects in the catalog. This action cannot be undone.
# MAGIC ```sql
# MAGIC -- To tear down the environment (WARNING: Deletes all data)
# MAGIC -- DROP CATALOG IF EXISTS ${CATALOG_NAME} CASCADE;
# MAGIC ```
# MAGIC After dropping the catalog, you can re-run this notebook to recreate the environment from scratch.
