# Databricks notebook source
# MAGIC %md
# MAGIC # 02: Silver Layer - Clean, Enrich, and De-duplicate
# MAGIC
# MAGIC **Zenith Online pipeline, notebook 2 of 3.** This notebook turns the raw Bronze events into one conformed Silver table, `silver_enriched_activity`: de-duplicated with a watermark, joined to the product and customer dimensions, and tagged with a region by a Pandas UDF. Run `01_Bronze_Ingestion` first.
# MAGIC
# MAGIC **Duration**: ~45 min
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Write a vectorized Pandas UDF and check it on real data before using it in a pipeline
# MAGIC - De-duplicate a stream with a watermark and enrich it with stream-static joins, one of them broadcast
# MAGIC - Run an availableNow stream to completion before the Gold layer reads its table
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Initialization
# MAGIC
# MAGIC Import the libraries this notebook uses, then load the shared catalog, schema, path and table names from `variables`.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import pandas as pd

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Overview
# MAGIC
# MAGIC **Goal:** Combine raw data sources, clean the data, remove duplicates, and create an enriched, queryable table for deeper analysis.
# MAGIC
# MAGIC **Input:** Bronze layer tables.
# MAGIC
# MAGIC **Output:** A central, enriched table: `silver_enriched_activity`.
# MAGIC
# MAGIC **Target Schema:** `02_silver` (use `SILVER_SCHEMA` from `variables.py`)
# MAGIC
# MAGIC ---
# MAGIC ### Define Region Categorization Logic
# MAGIC Create a Pandas UDF to categorize customer locations into regions for downstream analytics.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 4: Create a Pandas UDF for Region Categorization
# MAGIC
# MAGIC **Why:** Marketing segments customers by 3 regions, not 8 cities, so Silver carries a `region` column the Gold customer summary groups by.
# MAGIC
# MAGIC **When not to**: in production, a mapping this simple belongs in a built-in `when`/`isin` expression, which Spark evaluates in its own engine (Photon on serverless) with no Python round-trip. The exam expects you to write and apply a Pandas UDF, so this task uses one on logic you can check by eye. Reach for a Pandas UDF when the logic needs a Python library with no built-in equivalent (a geocoder, a scoring model, a parsing library).
# MAGIC
# MAGIC **End Goal:** A reusable function (`categorize_region`) producing a standardized region column used for customer and revenue rollups in Silver and Gold tables.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Pandas UDF**: Enables vectorized operations, faster than a row-at-a-time Python UDF; a built-in expression beats both
# MAGIC - **Series Processing**: the function receives a whole pandas Series per batch; write the body with vectorized Series methods (`isin`, `where`). A per-row `.apply` inside a Pandas UDF keeps the Arrow transfer but gives up the vectorized body
# MAGIC - **Type Hints**: Required for Pandas UDFs (`pd.Series` input/output)
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `categorize_region` is a Pandas UDF (built with `pandas_udf`, string return type) that takes and returns a pandas Series
# MAGIC - New York and Philadelphia map to `East`, Los Angeles, San Diego and Phoenix to `West`, any other location to `Central`, and a null location (an event whose customer the join did not find) stays null
# MAGIC - It is defined under the name `categorize_region`, because Task 5 calls it on the `location` column

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Declare a Pandas UDF with a StringType return type, named categorize_region,
#         that takes locations (pd.Series) and returns a pd.Series (the type hints are required).
# TODO 2: Inside it, start from a Series of 'Central' with the same index as locations, then set 'East' where
#         locations is New York or Philadelphia and 'West' where it is Los Angeles, San Diego or Phoenix (Series.isin).
# TODO 3: Return it with null wherever locations is null (Series.where), so a missing customer stays visible.
# TODO 4: Print "Silver Layer: Pandas UDF `categorize_region` created."


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create a Pandas UDF for Region Categorization
# @F.pandas_udf(StringType())
# def categorize_region(locations: pd.Series) -> pd.Series:
#     region = pd.Series("Central", index=locations.index, dtype="object")
#     region[locations.isin(["New York", "Philadelphia"])] = "East"
#     region[locations.isin(["Los Angeles", "San Diego", "Phoenix"])] = "West"
#     # A null location (no matching customer) stays null, so Task 7's check can see it.
#     return region.where(locations.notna(), None)
#
# print("Silver Layer: Pandas UDF `categorize_region` created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to apply `categorize_region` to the `location` column of `bronze_customer_profiles` (your Task 2 table), so you know the UDF works on real data before Task 5 builds on it. You should see:
# MAGIC - **Three regions, 1,000 customers in total**: `East` (New York, Philadelphia), `West` (Los Angeles, San Diego, Phoenix) and `Central` (Chicago, Houston, San Antonio).
# MAGIC - **What to look for**: no null region and no fourth value (every customer in this file has a location). The generator draws the 8 cities evenly, so `West` and `Central` each hold roughly three-eighths of the customers and `East` roughly a quarter: about 383 `West`, 375 `Central` and 242 `East`.
# MAGIC - **The null rule**: this file cannot test it, because every customer here has a location. Task 5 checks it: its 🔍 cell must print 249 rows without a region after one setup run; 0 means your UDF turned nulls into `Central`.
# MAGIC
# MAGIC **What this means**: the UDF collapses 8 city strings into the 3 regions marketing segments by. Task 5 applies it to every event, and the Gold customer summary groups by its output.

# COMMAND ----------

# 🔍 Verification: apply the UDF to the customer locations from Task 2
region_check = (
    spark.read.table(BRONZE_CUSTOMERS_TABLE)
    .withColumn("region", categorize_region(F.col("location")))
)
print(f"✅ Customers with categorize_region applied: {region_check.count():,} (expect 1,000)")
print(f"Customers with a region: {region_check.filter(F.col('region').isNotNull()).count():,} (expect 1,000)")
display(region_check.groupBy("region").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Transformation
# MAGIC Deduplicate streaming events, join with customer and product data, enrich with region, and write to the Silver Delta table.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 5: Create `silver_enriched_activity` Streaming Table
# MAGIC
# MAGIC **Why:** Silver de-duplicates and joins once, so every Gold aggregation reads one enriched table instead of re-implementing the joins.
# MAGIC - **Products: inner join.** An event for a product missing from the catalog has no price, name or category, and would land in Gold as a null-category row. The cost is that the few real purchases of delisted products (about 10 per setup run) drop out of revenue, so in production you would keep delisted products in the product dimension or route unmatched events to a quarantine table instead of losing them.
# MAGIC - **Customers: left join.** An event from a customer missing from the latest CSV snapshot is still a real view or purchase, so it is kept with a null region, which Task 7's data quality check counts.
# MAGIC - **3-minute watermark.** It bounds the de-duplication state: because `event_dt` is part of the dedup key, Spark can forget a key once the watermark has passed its event time. A client retry here is an exact copy (same `event_id`, same timestamp), and the generator's events arrive within 30 seconds of each other, so 3 minutes catches every retry while keeping the state small.
# MAGIC
# MAGIC **End Goal:** A single conformed Silver table (`silver_enriched_activity`), the one table both Gold tables are built from.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Watermarking**: tells Spark when it may forget old keys. Dedup state is evicted only when the watermarked column is part of the dedup key; with `event_id` alone the state grows forever. When copies of one event can carry different timestamps, `dropDuplicatesWithinWatermark` is the operator
# MAGIC - **Stream-Static Joins**: Join streaming fact table with static dimension tables
# MAGIC - **Broadcast Join**: Optimize small dimension table joins
# MAGIC - **Deduplication**: Remove duplicate events on their key, `event_id` plus the event time (`event_dt`), so a client retry that is an exact copy is dropped
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `silver_enriched_activity` holds each Bronze event once: duplicates are dropped on `event_id` with `event_dt` in the key, so the 3-minute watermark can evict old keys from the dedup state
# MAGIC - Each event carries its product (inner join on the broadcast product dimension) and its customer's region (left join, then `categorize_region` on `location`), with `event_type` renamed to `action` and an `event_date` column added
# MAGIC - The availableNow stream has finished before the cell ends, so the table is complete when notebook 03 reads it

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Print "Starting Silver Layer: Enriching and Cleaning Events".
# TODO 2: Read BRONZE_EVENTS_TABLE as a stream (bronze_events_stream_df), and
#         BRONZE_CUSTOMERS_TABLE and BRONZE_PRODUCTS_TABLE as batch tables (customers_df, products_df).
# TODO 3: Build deduplicated_stream_df: a watermark of "3 minutes" on event_dt, then drop duplicates on event_id AND event_dt
#         (the watermarked column must be in the key, or the dedup state is never cleaned up).
# TODO 4: Inner join the broadcast products_df on product_id, then left join customers_df
#         on deduplicated_stream_df.user_id == customers_df.customer_id.
# TODO 5: Add region (categorize_region on location), rename event_type to action,
#         and add event_date (the date part of event_dt).
# TODO 6: Select event_id, event_dt, event_date, action, user_id, region, product_id,
#         product_name, category, price. Take user_id and product_id from
#         deduplicated_stream_df so the column references are not ambiguous.
# TODO 7: Write the stream to SILVER_TABLE: format "delta", output mode "append",
#         option "checkpointLocation" = f"{CHECKPOINT_BASE_PATH}/silver_activity",
#         trigger availableNow=True.
# TODO 8: Keep the StreamingQuery the write returns, wait for it to finish,
#         then print a completion message.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `silver_enriched_activity` Streaming Table
# print("Starting Silver Layer: Enriching and Cleaning Events")
#
# bronze_events_stream_df = spark.readStream.table(BRONZE_EVENTS_TABLE)
# customers_df = spark.read.table(BRONZE_CUSTOMERS_TABLE)
# products_df = spark.read.table(BRONZE_PRODUCTS_TABLE)
#
# deduplicated_stream_df = (
#     bronze_events_stream_df
#         .withWatermark("event_dt", "3 minutes")
#         .dropDuplicates(["event_id", "event_dt"])
# )
#
# enriched_df = (
#     deduplicated_stream_df
#     .join(
#         F.broadcast(products_df),
#         deduplicated_stream_df.product_id == products_df.product_id,
#         "inner"
#     )
#     .join(
#         customers_df,
#         deduplicated_stream_df.user_id == customers_df.customer_id,
#         "left"
#     )
#     .withColumn("region", categorize_region(F.col("location")))
#     .withColumnRenamed("event_type", "action")
#     .withColumn("event_date", F.to_date(F.col("event_dt")))
#     .select(
#         "event_id", "event_dt", "event_date", "action",
#         deduplicated_stream_df.user_id, "region",
#         deduplicated_stream_df.product_id, "product_name", "category", "price"
#     )
# )
#
# # toTable returns as soon as the stream starts: wait for the availableNow run to
# # finish, or notebook 03 reads a Silver table that is still being filled.
# silver_query = (
#     enriched_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/silver_activity")
#     .trigger(availableNow=True)
#     .toTable(SILVER_TABLE)
# )
# silver_query.awaitTermination()
#
# print(f"Silver Layer: Enriched activity stream processing complete. Table `{SILVER_TABLE}` is updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to read `silver_enriched_activity` back as a batch table. You should see:
# MAGIC - **Row count**: 49,900 after one setup run, against 50,500 in `bronze_user_events`. De-duplication removed the 500 client retries (exact copies, same `event_id` and `event_dt`), and the inner join dropped the 100 events for delisted products (`product_id` 501-505), which cannot be priced.
# MAGIC - **Key columns**: `event_id`, `event_dt`, `event_date`, `action`, `user_id`, `region`, `product_id`, `product_name`, `category`, `price`.
# MAGIC - **What to look for**: 249 rows without a region. They are the events from late sign-ups (`user_id` 1001-1050) that the customer snapshot does not have yet: the left join kept them on purpose, and the UDF left their region null instead of booking them as `Central`. The region and action table below shows them as the null-region rows.
# MAGIC
# MAGIC **What this means**: this is the one behavioral dataset every Gold KPI is computed from. Notebook 03 aggregates it into daily product performance and a customer purchase summary.

# COMMAND ----------

# 🔍 Verification: read the Silver table back as a batch table
silver_check = spark.read.table(SILVER_TABLE)
n_files = len([f for f in dbutils.fs.ls(RAW_STREAMING_PATH) if f.name.endswith(".json")])
print(f"✅ {SILVER_TABLE} rows: {silver_check.count():,} (expect {49900 * n_files:,}: 49,900 per setup run)")
print(f"Rows without a region: {silver_check.filter(F.col('region').isNull()).count()} (expect {249 * n_files})")
display(silver_check.groupBy("region", "action").count().orderBy("region", "action"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC `silver_enriched_activity` is in place: de-duplicated, enriched with product and customer context, and tagged with a region.
# MAGIC
# MAGIC ### ✅ Exam checklist
# MAGIC Without scrolling up, can you:
# MAGIC - [ ] Write a Pandas UDF with the right type hints and a vectorized body, and name one case where a built-in expression is the better choice?
# MAGIC - [ ] Explain why the event-time column has to be in the `dropDuplicates` key before a watermark can bound the dedup state?
# MAGIC - [ ] Say which side of a stream-static join can be broadcast, and why the customer join is a left join?
# MAGIC
# MAGIC **Next: notebook 03_Gold_Aggregations**, where you measure the skew on the hot products, aggregate it into two Gold tables (with the two-stage salting pattern and when it pays off) and answer three business questions with SQL.
