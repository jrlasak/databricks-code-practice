from variables import *
import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *

# -----------------------------------------
# 02B SILVER: Materialized SCD2 Business Dimension Tables
# (using create_auto_cdc_flow)
# -----------------------------------------

"""
Task 9: Create the `02_silver.silver_customers` Table
======================================================

Business Logic:
    Track customer attribute changes over time using SCD2, enabling historical analysis 
    and compliance. Only business-relevant columns are tracked for history, and technical 
    columns are excluded. Null updates are ignored to prevent accidental overwrites.

Requirements:
    [ ] Use `dlt.create_auto_cdc_flow` with `live.customers_cleaned_stream` as the source.
    
    [ ] Set `keys` to `["customer_id"]`.
        Business logic: Customer ID is the unique identifier for tracking changes.
    
    [ ] Set `sequence_by` to `last_update_time`.
        Business logic: Ensures the most recent changes are applied correctly.
    
    [ ] Set `track_history_column_list` to `['name', 'email', 'address', 'phone_number', 'gender']`.
        Business logic: These columns are important for customer analytics and compliance.
    
    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: Exclude technical columns from history tracking.
    
    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_CUSTOMERS)
# dlt.create_auto_cdc_flow(
#     target=SILVER_CUSTOMERS,
#     source=f"live.{CUSTOMERS_CLEANED_STREAM}",
#     keys=["customer_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender'],
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     stored_as_scd_type=2
# )


##########################################################################################
##########################################################################################
##########################################################################################
"""
Task 10: Create the `02_silver.silver_products` Table
======================================================

Business Logic:
    Track product attribute changes over time using SCD2, enabling analysis of pricing, 
    branding, and inventory trends. Only business-relevant columns are tracked for history, 
    and technical columns are excluded. Null updates are ignored.

Requirements:
    [ ] Use `dlt.create_auto_cdc_flow` with `live.products_cleaned_stream` as the source.
    
    [ ] Set `keys` to `["product_id"]`.
        Business logic: Product ID is the unique identifier for tracking changes.
    
    [ ] Set `sequence_by` to `last_update_time`.
        Business logic: Ensures the most recent changes are applied correctly.
    
    [ ] Set `track_history_column_list` to `['name', 'category', 'brand', 'price', 'description', 'color', 'size']`.
        Business logic: These columns are important for product analytics and compliance.
    
    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: Exclude technical columns from history tracking.
    
    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state.

Tip:
    If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_PRODUCTS)
# dlt.create_auto_cdc_flow(
#     target=SILVER_PRODUCTS,
#     source=f"live.{PRODUCTS_CLEANED_STREAM}",
#     keys=["product_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size'],
#     stored_as_scd_type=2
# )

##########################################################################################
##########################################################################################
##########################################################################################

"""
Task 11: Create the `02_silver.silver_stores` Table
====================================================

Business Logic:
    Track store attribute changes over time using SCD2, enabling analysis of operational 
    changes and performance. Only business-relevant columns are tracked for history, and 
    technical columns are excluded. Null updates are ignored.

Requirements:
    [ ] Use `dlt.create_auto_cdc_flow` with `live.stores_cleaned_stream` as the source.
    
    [ ] Set `keys` to `["store_id"]`.
        Business logic: Store ID is the unique identifier for tracking changes.
    
    [ ] Set `sequence_by` to `last_update_time`.
        Business logic: Ensures the most recent changes are applied correctly.
    
    [ ] Set `track_history_column_list` to `['name', 'address', 'manager', 'status']`.
        Business logic: These columns are important for store analytics and compliance.
    
    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: Exclude technical columns from history tracking.
    
    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state.

Tip:
    If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dlt.create_streaming_table(SILVER_STORES)
# dlt.create_auto_cdc_flow(
#     target=SILVER_STORES,
#     source=f"live.{STORES_CLEANED_STREAM}",
#     keys=["store_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     track_history_column_list=['name', 'address', 'manager', 'status'],
#     stored_as_scd_type=2
# )