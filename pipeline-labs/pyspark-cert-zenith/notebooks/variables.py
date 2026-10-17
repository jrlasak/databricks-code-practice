# Databricks notebook source
# Define the three-level namespace for Unity Catalog
CATALOG_NAME = "zenith_online"

SYSTEM_SCHEMA = "_system"
LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# The data generator writes the raw files to these UC Volume paths
RAW_STREAMING_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/streaming/user_events"
RAW_BATCH_CUSTOMERS_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/batch/customers"
RAW_BATCH_PRODUCTS_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/batch/products"

# Define UC Volume paths for streaming checkpoints and schema metadata
CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}/checkpoints"
SCHEMA_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}/schemas"

# Full table names
BRONZE_EVENTS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_user_events"
BRONZE_CUSTOMERS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_customer_profiles"
BRONZE_PRODUCTS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_product_details"
SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_enriched_activity"
GOLD_DAILY_PRODUCT_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_daily_product_performance"
GOLD_CUSTOMER_SUMMARY_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.customer_purchase_summary"