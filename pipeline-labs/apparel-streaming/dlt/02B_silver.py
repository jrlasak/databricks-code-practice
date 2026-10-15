from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *

"""
✅ Silver Layer: SCD Type 2 Dimensions
======================================

Goal:
    Turn the cleaned customer, product and store views from 02A into SCD Type 2
    history tables, so a past sale can be reported against the attributes valid at
    the time.
"""

# -----------------------------------------
# 02B SILVER: SCD2 Dimension Streaming Tables
# (using dp.create_auto_cdc_flow)
# -----------------------------------------

"""
Task 9: Create the `02_silver.silver_customers` Table
======================================================

Business Logic:
    Track customer attribute changes over time using SCD2, enabling historical analysis
    and compliance. Only business-relevant columns open a new version, and technical
    columns are left out of the table. Null updates are ignored to prevent accidental
    overwrites.

Requirements:
    [ ] Declare the target first with `dp.create_streaming_table(SILVER_CUSTOMERS)`.
        Business logic: An AUTO CDC flow writes into a streaming table that must already
        exist in the pipeline graph; the flow itself creates nothing.

    [ ] Use `dp.create_auto_cdc_flow` with the `CUSTOMERS_CLEANED_STREAM` variable
        (`customers_cleaned_stream`) as the source, `keys` set to `["customer_id"]` and
        `sequence_by` set to `last_update_time`.
        Business logic: Customer ID identifies the record whose changes are tracked;
        `last_update_time` orders the changes, so the most recent change wins even when
        an older one arrives later.

    [ ] Set `track_history_column_list` to `['name', 'email', 'address', 'phone_number', 'gender']`.
        Business logic: These columns are important for customer analytics and compliance.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: `except_column_list` keeps these technical columns out of the
        target table entirely. History tracking is a separate setting
        (`track_history_column_list` above); a column that is in the table but not in
        that list is updated in place without opening a new version.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state.

Tip:
    - If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dp.create_streaming_table(SILVER_CUSTOMERS)
# dp.create_auto_cdc_flow(
#     target=SILVER_CUSTOMERS,
#     source=CUSTOMERS_CLEANED_STREAM,
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
    branding, and inventory trends. Only business-relevant columns open a new version,
    and technical columns are left out of the table. Null updates are ignored.

Requirements:
    [ ] Declare the target first with `dp.create_streaming_table(SILVER_PRODUCTS)`.
        Business logic: An AUTO CDC flow writes into a streaming table that must already
        exist in the pipeline graph; the flow itself creates nothing.

    [ ] Use `dp.create_auto_cdc_flow` with the `PRODUCTS_CLEANED_STREAM` variable
        (`products_cleaned_stream`) as the source, `keys` set to `["product_id"]` and
        `sequence_by` set to `last_update_time`.
        Business logic: Product ID identifies the record whose changes are tracked;
        `last_update_time` orders the changes, so the most recent change wins even when
        an older one arrives later.

    [ ] Set `track_history_column_list` to `['name', 'category', 'brand', 'price', 'description', 'color', 'size']`.
        Business logic: These columns are important for product analytics and compliance.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: `except_column_list` keeps these technical columns out of the
        target table entirely. History tracking is a separate setting
        (`track_history_column_list` above); a column that is in the table but not in
        that list is updated in place without opening a new version.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state.

Tip:
    If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dp.create_streaming_table(SILVER_PRODUCTS)
# dp.create_auto_cdc_flow(
#     target=SILVER_PRODUCTS,
#     source=PRODUCTS_CLEANED_STREAM,
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
    changes and performance. Only business-relevant columns open a new version, and
    technical columns are left out of the table. Null updates are ignored.

Requirements:
    [ ] Declare the target first with `dp.create_streaming_table(SILVER_STORES)`.
        Business logic: An AUTO CDC flow writes into a streaming table that must already
        exist in the pipeline graph; the flow itself creates nothing.

    [ ] Use `dp.create_auto_cdc_flow` with the `STORES_CLEANED_STREAM` variable
        (`stores_cleaned_stream`) as the source, `keys` set to `["store_id"]` and
        `sequence_by` set to `last_update_time`.
        Business logic: Store ID identifies the record whose changes are tracked;
        `last_update_time` orders the changes, so the most recent change wins even when
        an older one arrives later.

    [ ] Set `track_history_column_list` to `['name', 'address', 'manager', 'status']`.
        Business logic: These columns are important for store analytics and compliance.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: `except_column_list` keeps these technical columns out of the
        target table entirely. History tracking is a separate setting
        (`track_history_column_list` above); a column that is in the table but not in
        that list is updated in place without opening a new version.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: SCD2 tracks full history, not just current state. A vacant
        manager still opens a new version: Task 7 already turned the NULL into 'Unknown',
        so `ignore_null_updates` never sees it, and a vacancy is a business event.

Tip:
    If stuck, see the solution below.
"""
### ---------------------
### Write Your Code Here ###


### ---------------------
### Solution Is Below 
# dp.create_streaming_table(SILVER_STORES)
# dp.create_auto_cdc_flow(
#     target=SILVER_STORES,
#     source=STORES_CLEANED_STREAM,
#     keys=["store_id"],
#     sequence_by=F.col("last_update_time"),
#     ignore_null_updates=True,
#     except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
#     track_history_column_list=['name', 'address', 'manager', 'status'],
#     stored_as_scd_type=2
# )

##########################################################################################
# Done with this file? Run a pipeline update, do the Silver SCD2 checks and the customer,
# product and store part of the Silver data quality checks in ProjectPlan.md > Check
# your work, then open `02C_silver.py`.
##########################################################################################