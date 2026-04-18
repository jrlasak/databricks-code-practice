# Define the three-level namespace for Unity Catalog
CATALOG_NAME = "zenith_online"

SYSTEM_SCHEMA = "_system"
LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# The data generator wrote data to these UC Volume paths
RAW_STREAMING_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/streaming/user_events"
RAW_BATCH_CUSTOMERS_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/batch/customers"
RAW_BATCH_PRODUCTS_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/batch/products"

# Define UC Volume paths for streaming checkpoints and schema metadata
CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}/checkpoints"
SCHEMA_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SYSTEM_SCHEMA}/schemas"