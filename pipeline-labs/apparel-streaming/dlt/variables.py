# --- Unified Path and Table Definitions ---

CATALOG_NAME = "apparel_store"

LANDING_SCHEMA = "00_landing"
BRONZE_SCHEMA = "01_bronze"
SILVER_SCHEMA = "02_silver"
GOLD_SCHEMA = "03_gold"

# Volume base paths
RAW_STREAMING_VOLUME = f"{CATALOG_NAME}.{LANDING_SCHEMA}.streaming"
RAW_STREAMING_PATH = f"/Volumes/{CATALOG_NAME}/{LANDING_SCHEMA}/streaming"

# Raw data directories
RAW_SALES_PATH = f"{RAW_STREAMING_PATH}/sales"
RAW_CUSTOMERS_PATH = f"{RAW_STREAMING_PATH}/customers"
RAW_PRODUCTS_PATH = f"{RAW_STREAMING_PATH}/items"
RAW_STORES_PATH = f"{RAW_STREAMING_PATH}/stores"

# -----------------------------------------
# All table names
# -----------------------------------------

BRONZE_SALES = f"{BRONZE_SCHEMA}.bronze_sales"
BRONZE_CUSTOMERS = f"{BRONZE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{BRONZE_SCHEMA}.bronze_products"
BRONZE_STORES = f"{BRONZE_SCHEMA}.bronze_stores"

CUSTOMERS_CLEANED_STREAM = "customers_cleaned_stream"
PRODUCTS_CLEANED_STREAM = "products_cleaned_stream"
STORES_CLEANED_STREAM = "stores_cleaned_stream"
SALES_CLEANED_STREAM = "sales_cleaned_stream"

SILVER_CUSTOMERS = f"{SILVER_SCHEMA}.silver_customers"
SILVER_PRODUCTS = f"{SILVER_SCHEMA}.silver_products"
SILVER_STORES = f"{SILVER_SCHEMA}.silver_stores"
SILVER_SALES_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_sales_transactions"
SILVER_RETURNS_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_returns_transactions"

SILVER_CUSTOMERS_CURRENT = "silver_customers_current"
SILVER_PRODUCTS_CURRENT = "silver_products_current"
SILVER_STORES_CURRENT = "silver_stores_current"

GOLD_DENORMALIZED_SALES_FACTS = f"{GOLD_SCHEMA}.denormalized_sales_facts"
GOLD_DAILY_SALES_BY_STORE = f"{GOLD_SCHEMA}.gold_daily_sales_by_store"
GOLD_PRODUCT_PERFORMANCE = f"{GOLD_SCHEMA}.gold_product_performance"
GOLD_CUSTOMER_LIFETIME_VALUE = f"{GOLD_SCHEMA}.gold_customer_lifetime_value"