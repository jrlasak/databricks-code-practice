from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *

"""
✅ Silver Layer: SCD Type 2 Dimensions
======================================

Goal:
    Turn the cleaned customer, product and store views from 02A into SCD Type 2
    history tables, so a past sale can be reported against the attributes valid at
    the time. The gold tables in this lab report current names (Tasks 17-20); the
    history answers point-in-time questions, such as who managed a store on a given
    day, which you can query yourself (ProjectPlan.md > Check your work > Silver SCD2
    dimensions).
"""

# -----------------------------------------
# 02B SILVER: SCD2 Dimension Streaming Tables
# (using dp.create_auto_cdc_flow)
# -----------------------------------------

"""
Task 9: Create the `02_silver.silver_customers` Table
======================================================

Business Logic:
    Keep every version of a customer's contact details, so a past sale can be read
    against the details valid at the time. Only the columns in
    `track_history_column_list` open a new version, technical columns stay out of the
    table, and a NULL in a change never overwrites a stored value.

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
        Business logic: A new address, email or phone number opens a version, so the
        history shows the contact details valid at any point in time; `loyalty_points`,
        `age` and `join_date` update in place.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: `except_column_list` keeps these technical columns out of the
        target table entirely. History tracking is a separate setting
        (`track_history_column_list` above); a column that is in the table but not in
        that list is updated in place without opening a new version.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: `stored_as_scd_type=2` keeps every version; `ignore_null_updates`
        keeps the stored value when an incoming change carries a NULL.

Tip: if stuck, see the solution below.
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
    Keep every price and description a product has had, so you can see what it cost on
    any date. Stock changes update the current version in place, so this table holds no
    stock history. Technical columns stay out of the table; NULL updates are ignored.

Requirements:
    [ ] Declare the target first with `dp.create_streaming_table(SILVER_PRODUCTS)`.
        Business logic: As in Task 9.

    [ ] Use `dp.create_auto_cdc_flow` with the `PRODUCTS_CLEANED_STREAM` variable
        (`products_cleaned_stream`) as the source, `keys` set to `["product_id"]` and
        `sequence_by` set to `last_update_time`.
        Business logic: As in Task 9.

    [ ] Set `track_history_column_list` to `['name', 'category', 'brand', 'price', 'description', 'color', 'size']`.
        Business logic: A price change (mostly a markdown) or a new description opens a
        version, so the history shows what a product cost at any point in time;
        `stock_quantity` updates in place, since a version per stock movement would bury
        the real changes.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: As in Task 9.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: As in Task 9, every version is kept and a NULL in an incoming
        change leaves the stored value in place.

Tip: if stuck, see the solution below.
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
    Keep every manager and status a store has had, so you can see who ran it and
    whether it was trading on any date. Technical columns stay out of the table; NULL
    updates are ignored.

Requirements:
    [ ] Declare the target first with `dp.create_streaming_table(SILVER_STORES)`.
        Business logic: As in Task 9.

    [ ] Use `dp.create_auto_cdc_flow` with the `STORES_CLEANED_STREAM` variable
        (`stores_cleaned_stream`) as the source, `keys` set to `["store_id"]` and
        `sequence_by` set to `last_update_time`.
        Business logic: As in Task 9.

    [ ] Set `track_history_column_list` to `['name', 'address', 'manager', 'status']`.
        Business logic: A new manager or status opens a version, so the history shows who
        ran each store and whether it was trading at any point in time; `phone_number`
        and `open_date` update in place.

    [ ] Set `except_column_list` to exclude `["last_update_time", "ingest_timestamp", "source_file_path"]`.
        Business logic: As in Task 9.

    [ ] Ensure `ignore_null_updates` is `True` and `stored_as_scd_type` is `2`.
        Business logic: As in Task 9, every version is kept and a NULL in an incoming
        change leaves the stored value in place. A vacant manager still opens a new
        version: Task 7 already turned the NULL into 'Unknown', so `ignore_null_updates`
        never sees it, and a vacancy is a business event.

Tip: if stuck, see the solution below.
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