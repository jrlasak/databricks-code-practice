# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC **Zenith Online pipeline, notebook 1 of 3.** This notebook ingests the raw files that `00_Setup_Environment` generated into three managed Delta tables in Unity Catalog: `bronze_user_events` (streaming JSON through Auto Loader), `bronze_customer_profiles` (CSV) and `bronze_product_details` (Parquet). Run `00_Setup_Environment` first.
# MAGIC
# MAGIC **Duration**: ~1 h
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Ingest a stream of JSON files incrementally with Auto Loader, an explicit schema and a checkpoint
# MAGIC - Run an availableNow stream to completion before the next step reads its table
# MAGIC - Load CSV and Parquet files into Delta tables with batch reads and overwrite mode
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Initialization
# MAGIC
# MAGIC Import the libraries this notebook uses, then load the shared catalog, schema, path and table names from `variables`.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Overview
# MAGIC
# MAGIC **Goal:** Ingest raw data from landing zone volumes into managed Delta tables with minimal transformation, adding necessary metadata for traceability.
# MAGIC
# MAGIC **Input:** Raw JSON, CSV, and Parquet files.
# MAGIC
# MAGIC **Output:**
# MAGIC - `bronze_user_events` (from streaming JSON)
# MAGIC - `bronze_customer_profiles` (from CSV)
# MAGIC - `bronze_product_details` (from Parquet)
# MAGIC
# MAGIC **Target Schema:** `01_bronze` (use `BRONZE_SCHEMA` from `variables.py`)
# MAGIC
# MAGIC ---
# MAGIC ### Streaming User Events
# MAGIC Ingest raw user event data from JSON files using Auto Loader and write to a Delta table in the Bronze schema.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 1: Create `bronze_user_events` Streaming Table
# MAGIC
# MAGIC **Why:** Bronze keeps every raw event, append-only and barely transformed, so later layers can always trace back to it. Auto Loader reads only the files it has not ingested yet; the explicit schema fixes each column's type instead of inferring it.
# MAGIC
# MAGIC **End Goal:** A Delta table (`bronze_user_events`) in the Bronze schema that holds every raw JSON event plus two added columns, `ingestion_timestamp` and `event_dt`, which notebook 02 reads as a stream.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Auto Loader (`cloudFiles`)**: Incrementally processes new files as they arrive
# MAGIC - **Schema location (`cloudFiles.schemaLocation`)**: Where Auto Loader stores the schema it infers and tracks how it evolves. Required only when Auto Loader infers the schema. With an explicit schema it is optional; setting it anyway lets you switch to inference later without changing the stream
# MAGIC - **Explicit Schema**: Fixes each column's type up front instead of inferring it (it does not reject nulls)
# MAGIC - **Checkpointing**: Enables fault-tolerant stream processing
# MAGIC - **Metadata Columns**: Add timestamps for data lineage
# MAGIC - **availableNow Trigger**: Processes every file already in the landing volume, then stops on its own. Starting the stream does not block, though: the write returns a running StreamingQuery at once, so you wait for it with `awaitTermination` before anything reads the table
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `bronze_user_events` holds every event from the landing JSON files, read incrementally with Auto Loader and an explicit six-field schema
# MAGIC - Each row carries two added columns: `ingestion_timestamp` (when it was loaded) and `event_dt` (the event time as a real timestamp)
# MAGIC - The availableNow stream has finished before the cell ends, so the table is complete when the next task and notebook 02 read it

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Print "Starting Bronze Layer: Streaming Events Ingestion".
# TODO 2: Define event_schema as a StructType with six fields, all nullable:
#         event_id (StringType), event_timestamp (StringType), user_id (IntegerType),
#         event_type (StringType), product_id (IntegerType), session_id (StringType).
# TODO 3: Read RAW_STREAMING_PATH as a stream with Auto Loader: format "cloudFiles",
#         option "cloudFiles.format" = "json",
#         option "cloudFiles.schemaLocation" = f"{SCHEMA_BASE_PATH}/bronze_events",
#         and event_schema as the schema.
# TODO 4: Add ingestion_timestamp (the current timestamp) and event_dt
#         (event_timestamp converted to a timestamp).
# TODO 5: Write the stream to BRONZE_EVENTS_TABLE: format "delta", output mode "append",
#         option "checkpointLocation" = f"{CHECKPOINT_BASE_PATH}/bronze_events",
#         trigger availableNow=True.
# TODO 6: Keep the StreamingQuery the write returns and wait for it to finish.
# TODO 7: Print a completion message with the table name.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `bronze_user_events` Streaming Table
# print("Starting Bronze Layer: Streaming Events Ingestion")
#
# event_schema = StructType([
#     StructField("event_id", StringType(), True),
#     StructField("event_timestamp", StringType(), True),
#     StructField("user_id", IntegerType(), True),
#     StructField("event_type", StringType(), True),
#     StructField("product_id", IntegerType(), True),
#     StructField("session_id", StringType(), True)
# ])
#
# bronze_events_df = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE_PATH}/bronze_events")
#     .schema(event_schema)
#     .load(RAW_STREAMING_PATH)
#     .withColumn("ingestion_timestamp", F.current_timestamp())
#     .withColumn("event_dt", F.to_timestamp("event_timestamp"))
# )
#
# # toTable starts the stream and returns a StreamingQuery at once. The availableNow
# # trigger stops on its own after the files already landed, so wait for that here.
# bronze_events_query = (
#     bronze_events_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/bronze_events")
#     .trigger(availableNow=True)
#     .toTable(BRONZE_EVENTS_TABLE)
# )
# bronze_events_query.awaitTermination()
#
# print(f"Bronze Layer: Streaming events processing complete. Table `{BRONZE_EVENTS_TABLE}` is updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to read `bronze_user_events` back as a batch table. You should see:
# MAGIC - **Row count**: 50,500 rows, one per line of the JSON file `00_Setup_Environment` generated: 50,000 events plus 500 client retries, exact copies Bronze keeps on purpose (notebook 02 removes them). Each extra setup run lands one more 50,500-line file, and because the checkpoint remembers which files were already ingested, re-running this task adds only the new file.
# MAGIC - **Key columns**: the six source fields plus `ingestion_timestamp` and `event_dt`. The null `event_dt` line should print 0: a wrong timestamp conversion nulls the whole column.
# MAGIC - **What to look for**: the `event_type` split follows the generator's weights, roughly 70% `view_product`, 20% `add_to_cart` and 10% `purchase`.
# MAGIC
# MAGIC **What this means**: every user interaction is now in a governed, append-only table. Notebook 02 reads this table as a stream, de-duplicates it and enriches it with the customer and product dimensions you load next.

# COMMAND ----------

# 🔍 Verification: read the Bronze events back as a batch table
bronze_events = spark.read.table(BRONZE_EVENTS_TABLE)
n_files = len([f for f in dbutils.fs.ls(RAW_STREAMING_PATH) if f.name.endswith(".json")])
print(f"✅ {BRONZE_EVENTS_TABLE} rows: {bronze_events.count():,} (expect {50500 * n_files:,}: 50,500 per setup run)")
print(f"Rows with a null event_dt: {bronze_events.filter(F.col('event_dt').isNull()).count()} (expect 0)")
display(bronze_events.groupBy("event_type").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Customer & Product Data
# MAGIC Ingest customer profiles (CSV) and product details (Parquet) into Bronze tables for further enrichment.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 2: Create `bronze_customer_profiles` Batch Table
# MAGIC
# MAGIC **Why:** Converts a periodically refreshed dimension snapshot (customers) into a managed Delta table so it can be efficiently joined with streaming facts. Without a schema the CSV reader types every column as a string (`inferSchema` is off by default, and turning it on costs an extra pass over the file). Declaring `customer_id` as an integer up front gives the join key the same type as `user_id` in the events, so the Silver join compares integers instead of relying on an implicit cast.
# MAGIC
# MAGIC **End Goal:** A typed customer dimension (`bronze_customer_profiles`) that the Silver layer joins to the events on `customer_id` = `user_id`.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Batch Loading**: Read static files with explicit schemas
# MAGIC - **Explicit Schema**: declare the column types up front so the CSV reader does not type every column as a string (Delta's schema enforcement is a different feature: it checks writes against the table's schema)
# MAGIC - **Delta Overwrite**: Replace existing data with new snapshots
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `bronze_customer_profiles` is a Delta table loaded from the customer CSV file, with the file's header row used for column names
# MAGIC - The schema is set explicitly (`customer_id` an integer, `signup_date` and `location` strings), never inferred
# MAGIC - A re-run replaces the table with the latest snapshot instead of appending duplicates

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Print "Starting Bronze Layer: Batch Ingestion".
# TODO 2: Define customer_schema as a StructType: customer_id (IntegerType, nullable=False),
#         signup_date (StringType, nullable=True), location (StringType, nullable=True).
# TODO 3: Read RAW_BATCH_CUSTOMERS_PATH as format "csv" with customer_schema and
#         the option "header" = "true".
# TODO 4: Write it to BRONZE_CUSTOMERS_TABLE: format "delta", mode "overwrite".
# TODO 5: Print a completion message with the table name.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `bronze_customer_profiles` Batch Table
# print("Starting Bronze Layer: Batch Ingestion")
#
# customer_schema = StructType([
#     StructField("customer_id", IntegerType(), False),
#     StructField("signup_date", StringType(), True),
#     StructField("location", StringType(), True)
# ])
# customers_df = spark.read.format("csv").schema(customer_schema).option("header", "true").load(RAW_BATCH_CUSTOMERS_PATH)
# customers_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CUSTOMERS_TABLE)
# print(f"Bronze Layer: Wrote customer profiles to `{BRONZE_CUSTOMERS_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect the customer dimension. You should see:
# MAGIC - **Row count**: exactly 1,000 rows, one per generated customer (`customer_id` 1 to 1,000). Overwrite mode keeps it at 1,000 however often you re-run.
# MAGIC - **Key columns**: `customer_id` (integer), `signup_date` (a YYYY-MM-DD string), `location` (one of 8 US cities). `printSchema` shows `customer_id` as `nullable = true` even though your schema said `False`: Spark makes every column of a file read nullable, so nullability in a read schema is not a NOT NULL check. A guaranteed non-null key needs a table constraint (`ALTER TABLE ... ALTER COLUMN customer_id SET NOT NULL`).
# MAGIC - **What to look for**: `customer_id` typed as an integer, not a string; that is what lets notebook 02 join it to `user_id` in the events.
# MAGIC
# MAGIC **What this means**: most events' `user_id` has a matching customer here, so the Silver join can attach a location (and from it a region) to them. About 0.5% of events come from accounts created after this CRM export (`user_id` 1001-1050), which this snapshot does not have yet; notebook 02 keeps those events with a null region.

# COMMAND ----------

# 🔍 Verification: read the customer dimension back
bronze_customers = spark.read.table(BRONZE_CUSTOMERS_TABLE)
print(f"✅ {BRONZE_CUSTOMERS_TABLE} rows: {bronze_customers.count():,} (expect 1,000)")
bronze_customers.printSchema()
display(bronze_customers.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 TASK 3: Create `bronze_product_details` Batch Table
# MAGIC
# MAGIC **Why:** Turns each event's `product_id` into a name, a category and a price, which the Gold revenue and category metrics need.
# MAGIC
# MAGIC **End Goal:** A Delta product dimension (`bronze_product_details`) that notebook 02 joins to every event.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - `bronze_product_details` is a Delta table loaded from the product Parquet file (Parquet carries its own schema, so you do not define one here)
# MAGIC - A re-run replaces the table instead of appending duplicates

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO 1: Read RAW_BATCH_PRODUCTS_PATH as format "parquet".
# TODO 2: Write it to BRONZE_PRODUCTS_TABLE: format "delta", mode "overwrite".
# TODO 3: Print a completion message with the table name.


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create `bronze_product_details` Batch Table
# products_df = spark.read.format("parquet").load(RAW_BATCH_PRODUCTS_PATH)
# products_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_PRODUCTS_TABLE)
# print(f"Bronze Layer: Wrote product details to `{BRONZE_PRODUCTS_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below to inspect the product dimension. You should see:
# MAGIC - **Row count**: exactly 500 rows, one per generated product (`product_id` 1 to 500).
# MAGIC - **Key columns**: `product_id`, `product_name` (unique, ending in a model code such as `Z101`), `category` (Furniture, Home Decor, Electronics or Office Supplies) and `price` (between 15.50 and 899.99; each category has its own price band, so a stapler never costs more than a sofa).
# MAGIC - **What to look for**: four categories with 105 to 141 products each; the printed schema shows the column types the Parquet file carries, with no schema of your own (`product_id` as `long`, `price` as `double`); the min and max price come out at about 15.66 and 885.70.
# MAGIC
# MAGIC **What this means**: almost every event's `product_id` exists here, so notebook 02 can turn a bare ID into a name, a category and a price, and the Gold layer can compute revenue from it. About 0.2% of events point at products delisted before this catalog export (`product_id` 501-505); notebook 02 drops them, because they cannot be priced.

# COMMAND ----------

# 🔍 Verification: read the product dimension back
bronze_products = spark.read.table(BRONZE_PRODUCTS_TABLE)
print(f"✅ {BRONZE_PRODUCTS_TABLE} rows: {bronze_products.count():,} (expect 500)")
bronze_products.printSchema()
display(bronze_products.agg(F.min("price").alias("min_price"), F.max("price").alias("max_price")))
display(bronze_products.groupBy("category").count().orderBy("category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC The three Bronze tables are in place: `bronze_user_events`, `bronze_customer_profiles` and `bronze_product_details`.
# MAGIC
# MAGIC ### ✅ Exam checklist
# MAGIC Without scrolling up, can you:
# MAGIC - [ ] Name the Auto Loader option that sets the file format and the one that stores the inferred schema, and say why the second is optional when you pass an explicit schema?
# MAGIC - [ ] Explain why an availableNow write still needs `awaitTermination` before the next cell reads the table?
# MAGIC - [ ] Say why the CSV got an explicit schema and the Parquet file did not?
# MAGIC
# MAGIC **Next: notebook 02_Silver_Enrichment**, where you de-duplicate the event stream, enrich it with the two dimensions and write `silver_enriched_activity`.
