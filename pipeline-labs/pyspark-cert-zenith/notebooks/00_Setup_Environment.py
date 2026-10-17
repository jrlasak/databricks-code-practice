# Databricks notebook source
# MAGIC %md
# MAGIC # 00: Environment Setup and Data Generation
# MAGIC
# MAGIC This notebook prepares everything the Zenith Online pipeline needs: it creates the `zenith_online` catalog, its five schemas (`_system`, `00_landing`, `01_bronze`, `02_silver`, `03_gold`), the landing and system volumes, and then generates the raw data (1,000 customers as CSV, 500 products as Parquet, one JSON Lines file of 50,000 user events plus 500 retried copies). Run it once, top to bottom, before notebook 01.
# MAGIC
# MAGIC One setup run writes a compressed sample: 50,000 events stamped within 30 seconds stand in for a day of storefront traffic from 1,000 customers. The sample is also denser than a real storefront: 1 event in 10 is a purchase, so nearly every customer appears in the Gold summary. Read the Gold numbers for their proportions, not as real daily revenue.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Create the catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# Create the schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SYSTEM_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# Create the landing volumes for the raw data
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.batch")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{LANDING_SCHEMA}.streaming")

# COMMAND ----------

# Create the landing directories for the raw data
for path in [RAW_STREAMING_PATH, RAW_BATCH_CUSTOMERS_PATH, RAW_BATCH_PRODUCTS_PATH]:
    try:
        dbutils.fs.ls(path)
    except Exception:
        dbutils.fs.mkdirs(path)

# COMMAND ----------

# Create the volumes for checkpoints and schema metadata
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SYSTEM_SCHEMA}.checkpoints")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SYSTEM_SCHEMA}.schemas")

print(f"Unity Catalog environment '{CATALOG_NAME}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the Raw Data
# MAGIC
# MAGIC The next cell loads the data generator; the one after it writes the customer CSV, the product Parquet file and one JSON Lines file of user events into the landing volumes: 50,000 events plus 500 client retries re-sent as exact copies, so 50,500 lines.
# MAGIC
# MAGIC Re-running this notebook rewrites the two batch files and appends one more 50,500-line JSON file: that new file is what your Bronze stream picks up on a re-run.

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

generate_batch_data()
generate_streaming_events(infinite=False, stream_events_per_batch=50000)
# infinite=False writes one batch of events and stops, instead of looping until cancelled.

print("✅ Setup complete: catalog, schemas, volumes and raw data are ready. Continue with 01_Bronze_Ingestion.")
