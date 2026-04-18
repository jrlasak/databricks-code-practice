# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Variables
# MAGIC
# MAGIC Central configuration file for the Databricks Data Engineer Certification Lab.
# MAGIC
# MAGIC **Usage**: Import this file in all notebooks to maintain consistent naming.
# MAGIC
# MAGIC ```python
# MAGIC %run ./variables
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Configuration

# COMMAND ----------

# Catalog name (modify if there's a naming conflict in your workspace)
CATALOG_NAME = "cert_prep_catalog"

# Schema names following Medallion Architecture
SYSTEM_SCHEMA = "_system"
LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# DLT-specific schema (for Delta Live Tables pipeline)
DLT_SCHEMA = "dlt_pipeline"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Paths

# COMMAND ----------

# Landing volumes for raw data
LANDING_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}"
CUSTOMERS_LANDING_PATH = f"{LANDING_BASE_PATH}/customers"
PRODUCTS_LANDING_PATH = f"{LANDING_BASE_PATH}/products"
SALES_LANDING_PATH = f"{LANDING_BASE_PATH}/sales"
EVENTS_LANDING_PATH = f"{LANDING_BASE_PATH}/events"

# System volume for checkpoints and metadata
SYSTEM_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}"
CHECKPOINT_BASE_PATH = f"{SYSTEM_BASE_PATH}/checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint Locations

# COMMAND ----------

# Checkpoint locations for streaming queries
CUSTOMERS_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/customers_stream"
PRODUCTS_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/products_stream"
SALES_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/sales_stream"
EVENTS_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/events_stream"

# Silver layer checkpoints
SALES_SILVER_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/sales_silver_stream"
EVENTS_SILVER_CHECKPOINT_PATH = f"{CHECKPOINT_BASE_PATH}/events_silver_stream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Names

# COMMAND ----------

# Bronze tables (raw ingestion)
CUSTOMERS_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.customers_raw"
PRODUCTS_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products_raw"
SALES_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.sales_raw"
EVENTS_BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.events_raw"

# Silver tables (cleaned and conformed)
CUSTOMERS_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.customers_clean"
PRODUCTS_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.products_clean"
SALES_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.sales_clean"
EVENTS_SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.events_clean"
CUSTOMERS_SCD2_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.dim_customers_scd2"

# Gold tables (business aggregations)
DAILY_SALES_SUMMARY_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.daily_sales_summary"
CUSTOMER_LTV_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.customer_lifetime_value"
PRODUCT_PERFORMANCE_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.product_performance"
FUNNEL_ANALYSIS_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.funnel_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generator Configuration

# COMMAND ----------

# Data generation settings
DATA_GEN_CONFIG = {
    # Volume settings
    "num_customers": 10000,
    "num_products": 1000,
    "num_sales": 50000,
    "num_events": 100000,

    # Data quality settings (percentage of issues to inject)
    "duplicate_rate": 0.02,  # 2% duplicates
    "null_rate_low": 0.05,   # 5% nulls for less critical fields
    "null_rate_medium": 0.10, # 10% nulls
    "null_rate_high": 0.20,   # 20% nulls

    # Streaming settings
    "streaming_batch_size": 5000,  # Records per batch (optimized for ~2 min generation)
    "streaming_delay_seconds": 0,  # No delay between batches

    # Skew settings (Pareto distribution)
    "product_skew_factor": 0.8,  # 80% of sales from 20% of products
    "customer_skew_factor": 0.7, # 70% of revenue from 20% of customers
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Categories

# COMMAND ----------

PRODUCT_CATEGORIES = {
    "Electronics": ["Audio", "Computers", "Cameras", "Mobile", "Wearables"],
    "Clothing": ["Men", "Women", "Kids", "Accessories"],
    "Home & Garden": ["Furniture", "Kitchen", "Decor", "Garden", "Storage"],
    "Sports & Outdoors": ["Fitness", "Camping", "Cycling", "Team Sports"],
    "Books": ["Fiction", "Non-Fiction", "Educational", "Comics"]
}

# Category distribution (weights for random selection)
CATEGORY_WEIGHTS = {
    "Electronics": 0.30,
    "Clothing": 0.25,
    "Home & Garden": 0.20,
    "Sports & Outdoors": 0.15,
    "Books": 0.10
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Types

# COMMAND ----------

EVENT_TYPES = {
    "view_product": 0.60,      # 60% of events
    "search": 0.15,            # 15% of events
    "add_to_cart": 0.10,       # 10% of events
    "remove_from_cart": 0.03,  # 3% of events
    "checkout_start": 0.07,    # 7% of events
    "checkout_complete": 0.05  # 5% of events
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Loyalty Tiers

# COMMAND ----------

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]

# Tier distribution
LOYALTY_TIER_WEIGHTS = {
    "Bronze": 0.40,
    "Silver": 0.35,
    "Gold": 0.20,
    "Platinum": 0.05
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment Methods

# COMMAND ----------

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]

PAYMENT_METHOD_WEIGHTS = {
    "credit_card": 0.50,
    "debit_card": 0.25,
    "paypal": 0.15,
    "apple_pay": 0.05,
    "google_pay": 0.05
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Device Types

# COMMAND ----------

DEVICE_TYPES = ["desktop", "mobile", "tablet"]

DEVICE_TYPE_WEIGHTS = {
    "mobile": 0.60,
    "desktop": 0.30,
    "tablet": 0.10
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browser Types

# COMMAND ----------

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]

BROWSER_WEIGHTS = {
    "Chrome": 0.50,
    "Safari": 0.30,
    "Firefox": 0.10,
    "Edge": 0.08,
    "Opera": 0.02
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locations (US Cities)

# COMMAND ----------

US_LOCATIONS = [
    "New York, NY, USA",
    "Los Angeles, CA, USA",
    "Chicago, IL, USA",
    "Houston, TX, USA",
    "Phoenix, AZ, USA",
    "Philadelphia, PA, USA",
    "San Antonio, TX, USA",
    "San Diego, CA, USA",
    "Dallas, TX, USA",
    "San Jose, CA, USA",
    "Austin, TX, USA",
    "Jacksonville, FL, USA",
    "Fort Worth, TX, USA",
    "Columbus, OH, USA",
    "San Francisco, CA, USA",
    "Charlotte, NC, USA",
    "Indianapolis, IN, USA",
    "Seattle, WA, USA",
    "Denver, CO, USA",
    "Boston, MA, USA",
    "Portland, OR, USA",
    "Miami, FL, USA",
    "Atlanta, GA, USA",
    "Detroit, MI, USA",
    "Minneapolis, MN, USA"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_full_table_name(schema, table):
    """
    Generate full three-level table name.

    Args:
        schema (str): Schema name (e.g., '01_bronze')
        table (str): Table name (e.g., 'customers_raw')

    Returns:
        str: Full table name (e.g., 'cert_prep_catalog.01_bronze.customers_raw')
    """
    return f"{CATALOG_NAME}.{schema}.{table}"

def get_checkpoint_path(stream_name):
    """
    Generate checkpoint path for a streaming query.

    Args:
        stream_name (str): Name of the stream (e.g., 'customers')

    Returns:
        str: Full checkpoint path
    """
    return f"{CHECKPOINT_BASE_PATH}/{stream_name}_stream"

def get_volume_path(volume_name):
    """
    Generate volume path in Unity Catalog.

    Args:
        volume_name (str): Name of the volume (e.g., 'customers')

    Returns:
        str: Full volume path
    """
    return f"{LANDING_BASE_PATH}/{volume_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

def validate_configuration():
    """
    Validate that all configuration values are properly set.
    Returns True if valid, raises ValueError otherwise.
    """
    # Check catalog name
    if not CATALOG_NAME or CATALOG_NAME == "":
        raise ValueError("CATALOG_NAME must be set")

    # Check category weights sum to 1.0
    category_sum = sum(CATEGORY_WEIGHTS.values())
    if abs(category_sum - 1.0) > 0.01:
        raise ValueError(f"CATEGORY_WEIGHTS must sum to 1.0 (current: {category_sum})")

    # Check event type weights sum to 1.0
    event_sum = sum(EVENT_TYPES.values())
    if abs(event_sum - 1.0) > 0.01:
        raise ValueError(f"EVENT_TYPES weights must sum to 1.0 (current: {event_sum})")

    print("✅ Configuration validation passed")
    return True

# Run validation (comment out if not needed)
# validate_configuration()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Configuration Summary

# COMMAND ----------

def print_config_summary():
    """
    Print a summary of the current configuration.
    """
    print("=" * 60)
    print("DATABRICKS DATA ENGINEER CERTIFICATION LAB")
    print("Configuration Summary")
    print("=" * 60)
    print(f"\nCatalog: {CATALOG_NAME}")
    print(f"\nSchemas:")
    print(f"  - System:  {SYSTEM_SCHEMA}")
    print(f"  - Landing: {LANDING_SCHEMA}")
    print(f"  - Bronze:  {BRONZE_SCHEMA}")
    print(f"  - Silver:  {SILVER_SCHEMA}")
    print(f"  - Gold:    {GOLD_SCHEMA}")
    print(f"  - DLT:     {DLT_SCHEMA}")
    print(f"\nLanding Paths:")
    print(f"  - Customers: {CUSTOMERS_LANDING_PATH}")
    print(f"  - Products:  {PRODUCTS_LANDING_PATH}")
    print(f"  - Sales:     {SALES_LANDING_PATH}")
    print(f"  - Events:    {EVENTS_LANDING_PATH}")
    print(f"\nData Generation Config:")
    print(f"  - Customers: {DATA_GEN_CONFIG['num_customers']:,}")
    print(f"  - Products:  {DATA_GEN_CONFIG['num_products']:,}")
    print(f"  - Sales:     {DATA_GEN_CONFIG['num_sales']:,}")
    print(f"  - Events:    {DATA_GEN_CONFIG['num_events']:,}")
    print("=" * 60)

# Uncomment to display configuration
# print_config_summary()
