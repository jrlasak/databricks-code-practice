# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Auto Loader & Incremental Ingestion
# MAGIC
# MAGIC **Exam Coverage**: Section 2 (Development and Ingestion)
# MAGIC
# MAGIC **Duration**: about 1 hour
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Use Auto Loader to incrementally ingest JSON files
# MAGIC - Configure schema inference and evolution in Auto Loader
# MAGIC - Implement streaming ingestion for CSV files
# MAGIC - Use COPY INTO for batch incremental loads
# MAGIC - Find values that did not fit the schema with the `_rescued_data` column
# MAGIC - Configure checkpointing for streaming queries
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction to Auto Loader
# MAGIC Auto Loader is an optimized file ingestion mechanism that incrementally processes new data files as they arrive in cloud storage.
# MAGIC ### Key Features
# MAGIC - **Incremental Processing**: Only processes new files
# MAGIC - **Schema Inference**: Automatically detects schema from data
# MAGIC - **Schema Evolution**: Adapts to schema changes over time
# MAGIC - **Scalability**: Efficiently handles millions of files
# MAGIC - **Checkpointing**: Tracks processed files for exactly-once semantics
# MAGIC ### Auto Loader vs Traditional Approaches
# MAGIC | Feature | Auto Loader | spark.read | COPY INTO |
# MAGIC |---------|-------------|------------|----------|
# MAGIC | **Streaming** | Yes | No | No |
# MAGIC | **Incremental** | Yes | Manual | Yes |
# MAGIC | **Schema Evolution** | Automatic | Manual | Manual |
# MAGIC | **Performance** | Optimized | Scans all | Good |
# MAGIC | **Use Case** | Continuous | One-time | Batch incremental |
# MAGIC ### The Auto Loader shape
# MAGIC An Auto Loader pipeline has two halves. A streaming **read** with the `cloudFiles` source needs the file format and a schema location. A streaming **write** to a Delta table needs a checkpoint location and a trigger. You write the read in Exercise 1 and the write in Exercise 2.

# COMMAND ----------

# MAGIC %md
# MAGIC Import shared variables and configuration

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Set current catalog and schema
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")

print(f"Current Catalog: {spark.catalog.currentCatalog()}")
print(f"Current Schema: {spark.catalog.currentDatabase()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Auto Loader for JSON Files (Customers)
# MAGIC We'll start with JSON - the most common format for semi-structured data.
# MAGIC ### Key Auto Loader Options
# MAGIC | Option | Purpose |
# MAGIC |--------|--------|
# MAGIC | `cloudFiles.format` | Source file format (json, csv, parquet) |
# MAGIC | `cloudFiles.schemaLocation` | Where to store inferred schema |
# MAGIC | `cloudFiles.inferColumnTypes` | Enable type inference (vs all strings) |
# MAGIC | `cloudFiles.schemaEvolutionMode` | How to handle schema changes |
# MAGIC | `checkpointLocation` | Track processing state |
# MAGIC ### Understanding Checkpoints
# MAGIC Checkpoints store:
# MAGIC 1. **Processed file list** - Which files have been ingested
# MAGIC 2. **Schema information** - Current inferred schema
# MAGIC 3. **Offsets** - Position in the stream
# MAGIC
# MAGIC This enables exactly-once processing.

# COMMAND ----------

# Verify source data exists
print(f"Customer landing path: {CUSTOMERS_LANDING_PATH}")
print(f"Checkpoint path: {CUSTOMERS_CHECKPOINT_PATH}")

# List files in landing zone
display(dbutils.fs.ls(CUSTOMERS_LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Create Auto Loader Stream for JSON
# MAGIC **Your task**: Build an Auto Loader stream to read JSON customer data.
# MAGIC
# MAGIC **Why**: The checkpoint records every file ingested, so tomorrow's run reads only tomorrow's files instead of re-scanning the folder.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read `CUSTOMERS_LANDING_PATH` with Auto Loader as JSON, into the variable `customers_stream`
# MAGIC - Store the inferred schema in `CUSTOMERS_CHECKPOINT_PATH`
# MAGIC - Infer column types (instead of reading every column as a string)
# MAGIC
# MAGIC **Hint**: Auto Loader is the `cloudFiles` source; every Auto Loader option starts with `cloudFiles.`.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create Auto Loader stream for JSON customers

customers_stream = spark.readStream \




# Print schema to verify
customers_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Auto Loader for JSON Customers

# customers_stream = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "json") \
#     .option("cloudFiles.schemaLocation", CUSTOMERS_CHECKPOINT_PATH) \
#     .option("cloudFiles.inferColumnTypes", "true") \
#     .load(CUSTOMERS_LANDING_PATH)

# customers_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Write Stream to Bronze Table
# MAGIC **Your task**: Write the streaming DataFrame to a Delta table.
# MAGIC
# MAGIC **Why**: Without the checkpoint, a restart after a failure loads every file again and doubles Bronze.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Append to `CUSTOMERS_BRONZE_TABLE` as Delta, with the checkpoint in `CUSTOMERS_CHECKPOINT_PATH`
# MAGIC - Process everything available, then stop
# MAGIC - Keep the query in `customers_query` (the cell then waits for it to finish)
# MAGIC
# MAGIC **Hint**: a streaming write needs somewhere to remember what it processed, and a trigger that stops once the backlog is drained.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Write streaming data to bronze table

customers_query = customers_stream.writeStream \





# Wait for stream to complete
customers_query.awaitTermination()

print(f"Data loaded into: {CUSTOMERS_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Write Stream to Bronze Table

# customers_query = customers_stream.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", CUSTOMERS_CHECKPOINT_PATH) \
#     .trigger(availableNow=True) \
#     .table(CUSTOMERS_BRONZE_TABLE)

# customers_query.awaitTermination()

# print(f"✅ Data loaded into: {CUSTOMERS_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see **10,200** customers: 10,000 customers + 200 planted duplicates. Bronze keeps them all; notebook 03 removes the duplicates.
# MAGIC
# MAGIC To start over, use the cleanup command at the end of notebook 00 (drop the catalog), then run notebook 00 and this notebook again. Do not start over by re-running notebook 00 alone: it lands every file again under new names, Auto Loader ingests the copies, and every Bronze count in this notebook doubles.

# COMMAND ----------

# 🔍 Verify data was loaded
customer_count = spark.table(CUSTOMERS_BRONZE_TABLE).count()
print(f"Total customers loaded: {customer_count:,} (10,200 on a first run)")

# Display sample records
display(spark.table(CUSTOMERS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Checkpoints
# MAGIC The cell below lists the checkpoint folder:

# COMMAND ----------

# Explore checkpoint directory structure
display(dbutils.fs.ls(CUSTOMERS_CHECKPOINT_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC **What you see:**
# MAGIC - `sources/` - Tracks which files have been processed
# MAGIC - `offsets/` - Stream position information
# MAGIC - `commits/` - Transaction log for the stream
# MAGIC - Schema information files
# MAGIC
# MAGIC This ensures exactly-once processing even if the stream fails and restarts. You will see it in action after Exercise 4, when a new sales file lands.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Auto Loader for CSV with Schema Evolution
# MAGIC CSV files require additional configuration:
# MAGIC - Header row handling
# MAGIC - Delimiter (`sep`) when it is not a comma
# MAGIC - Schema evolution strategy
# MAGIC ### Schema Evolution Modes
# MAGIC | Mode | Behavior |
# MAGIC |------|----------|
# MAGIC | `addNewColumns` | Stream stops on a new column, adds it, continues after restart (default when no schema is given) |
# MAGIC | `failOnNewColumns` | Fail the stream if schema changes |
# MAGIC | `rescue` | Never evolve the schema; new columns land in `_rescued_data`, the stream never fails on schema change |
# MAGIC | `none` | Ignore new columns (default when a schema is given) |
# MAGIC
# MAGIC **Best practice**: `addNewColumns` with the stream in a job that retries, so a new column costs one automatic restart.

# COMMAND ----------

# Verify product source data
print(f"Product landing path: {PRODUCTS_LANDING_PATH}")
display(dbutils.fs.ls(PRODUCTS_LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Auto Loader for CSV Files
# MAGIC **Your task**: Create an Auto Loader stream for CSV product data with schema evolution.
# MAGIC
# MAGIC **Why**: CSV carries no schema: without the header option the first row lands as data. Setting the evolution mode explicitly states what a new column does (with `addNewColumns` the stream stops once, records the column and continues on restart) instead of relying on a default that becomes `none` the day someone passes a schema.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read `PRODUCTS_LANDING_PATH` with Auto Loader as CSV whose first row is the header, into `products_stream`
# MAGIC - Schema in `PRODUCTS_CHECKPOINT_PATH`, column types inferred
# MAGIC - Schema evolution mode `addNewColumns`, set explicitly
# MAGIC
# MAGIC **Hint**: same as Exercise 1, plus the reader's CSV header option and an explicit schema evolution mode.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create Auto Loader stream for CSV products

products_stream = spark.readStream \





products_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Auto Loader for CSV Products

# products_stream = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "csv") \
#     .option("cloudFiles.schemaLocation", PRODUCTS_CHECKPOINT_PATH) \
#     .option("cloudFiles.inferColumnTypes", "true") \
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
#     .option("header", "true") \
#     .load(PRODUCTS_LANDING_PATH)

# products_stream.printSchema()

# COMMAND ----------

# Write products to bronze table
products_query = products_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", PRODUCTS_CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .table(PRODUCTS_BRONZE_TABLE)

products_query.awaitTermination()

print(f"✅ Data loaded into: {PRODUCTS_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see **1,010** products: 1,000 products + 10 planted duplicates (older catalog snapshots). Bronze keeps them all; notebook 03 removes the duplicates.

# COMMAND ----------

# 🔍 Verify products data
product_count = spark.table(PRODUCTS_BRONZE_TABLE).count()
print(f"Total products loaded: {product_count:,} (1,010 on a first run)")

display(spark.table(PRODUCTS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Streaming Triggers
# MAGIC Auto Loader supports different trigger types for controlling when data is processed.
# MAGIC ### Trigger Types
# MAGIC | Trigger | Behavior | Use Case |
# MAGIC |---------|----------|----------|
# MAGIC | `availableNow=True` | Process everything new in micro-batches, then stop | Scheduled incremental batch (recommended). Serverless, including Free Edition, runs only this trigger and the deprecated `once` |
# MAGIC | `processingTime='10 seconds'` | Check for new data every 10 s, run a micro-batch when there is some | Always-on near-real-time on classic compute |
# MAGIC | no trigger | Default: `processingTime` 0, micro-batches back to back | Lowest-latency micro-batch on classic compute |
# MAGIC | `once=True` | Deprecated | Use availableNow |
# MAGIC
# MAGIC **For production**: `availableNow` on a schedule covers most ingestion; always-on triggers (`processingTime`, no trigger) need classic compute. Serverless, including Free Edition, rejects them.
# MAGIC
# MAGIC **For this lab**: every stream uses `availableNow=True`, the trigger Free Edition serverless runs.

# COMMAND ----------

# Check sales landing data
print(f"Sales landing path: {SALES_LANDING_PATH}")
display(dbutils.fs.ls(SALES_LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Ingest the Sales Backlog with Auto Loader
# MAGIC **Your task**: Create an Auto Loader stream for sales data (high-volume JSON) into `sales_stream`. The cell after the solution writes it to `SALES_BRONZE_TABLE` with `availableNow`.
# MAGIC
# MAGIC **Why**: the sales backlog is 10 batches spread over about 100 files; `availableNow` drains the whole backlog in one run and stops. That is how a scheduled job uses Auto Loader without an always-on stream.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read JSON from `SALES_LANDING_PATH`
# MAGIC - Schema location: `SALES_CHECKPOINT_PATH`
# MAGIC - Enable type inference
# MAGIC
# MAGIC **Hint**: Same pattern as customers, but with sales paths.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create Auto Loader stream for sales JSON data

sales_stream = spark.readStream \





sales_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Ingest the Sales Backlog with Auto Loader

# sales_stream = spark.readStream \
#     .format("cloudFiles") \
#     .option("cloudFiles.format", "json") \
#     .option("cloudFiles.schemaLocation", SALES_CHECKPOINT_PATH) \
#     .option("cloudFiles.inferColumnTypes", "true") \
#     .load(SALES_LANDING_PATH)

# sales_stream.printSchema()

# COMMAND ----------

# Write sales stream to bronze table
sales_query = sales_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", SALES_CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .table(SALES_BRONZE_TABLE)

sales_query.awaitTermination()

print(f"✅ Data loaded into: {SALES_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see **50,500** rows: 50,000 orders written in 10 batches of 5,000 (each batch lands as several part files, which is why the listing above shows about 100 files), plus 50 client-retry duplicates per batch. `line_items` is still a nested array: one Bronze row is one order, not one product sold.
# MAGIC
# MAGIC Run this notebook again and the count is 500 higher for every earlier run of the 🔄 cell below, which lands one more 500-order file each time. The 🔄 cell itself always reports `new rows: 500`.

# COMMAND ----------

# 🔍 Verify sales data
sales_count = spark.table(SALES_BRONZE_TABLE).count()
print(f"Total sales loaded: {sales_count:,} (50,500 on a first run)")

display(spark.table(SALES_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 New data arrives
# MAGIC The cell below lands one new file of 500 orders for 1 and 2 July 2026, the two days after the backlog (built from existing orders, under new `ORD-202607-X` ids), in the sales volume, then runs the same Auto Loader write as above.
# MAGIC
# MAGIC **You should see** the Bronze count grow by exactly **500**: the checkpoint remembers every file it already ingested, so Auto Loader reads only the new one. Run notebook 02 again and Bronze grows by another 500 rows with the same order ids: notebook 03's dropDuplicates on `order_id` keeps each order once in Silver.

# COMMAND ----------

# 🔄 Land one new 500-order file, then re-run the sales write
from pyspark.sql import functions as F
from pyspark.sql.window import Window

before = spark.table(SALES_BRONZE_TABLE).count()

extra = (spark.read.json(SALES_LANDING_PATH)
         .dropDuplicates(["order_id"])
         .orderBy("order_id")
         .limit(500)
         .withColumn("rn", F.row_number().over(Window.orderBy("order_id")))
         .withColumn("order_id", F.format_string("ORD-202607-X%05d", F.col("rn")))
         # The next two days' orders: same time of day, dated 2026-07-01 and 2026-07-02
         .withColumn("order_timestamp", F.concat(
             F.when(F.col("rn") % 2 == 0, F.lit("2026-07-01")).otherwise(F.lit("2026-07-02")),
             F.substring("order_timestamp", 11, 9)))
         .drop("rn"))
extra_rows = extra.collect()  # collect first: never write into the folder a lazy read is still listing
spark.createDataFrame(extra_rows, extra.schema).coalesce(1).write.mode("append").format("json").save(SALES_LANDING_PATH)

sales_query = sales_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", SALES_CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .table(SALES_BRONZE_TABLE)
sales_query.awaitTermination()

after = spark.table(SALES_BRONZE_TABLE).count()
print(f"Bronze sales before: {before:,}  after: {after:,}  new rows: {after - before:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: COPY INTO for Batch Incremental Loads
# MAGIC COPY INTO is a SQL command for idempotent, incremental batch loading.
# MAGIC ### COPY INTO vs Auto Loader
# MAGIC | Feature | COPY INTO | Auto Loader |
# MAGIC |---------|-----------|-------------|
# MAGIC | **Type** | Batch (SQL) | Streaming (Python/SQL) |
# MAGIC | **Idempotent** | Yes | Yes |
# MAGIC | **Checkpointing** | Automatic (tracked in the table) | One `checkpointLocation` option |
# MAGIC | **Scale** | Thousands of files over time | Millions of files or more over time |
# MAGIC | **Use Case** | Scheduled batch jobs | Continuous ingestion |
# MAGIC | **Complexity** | Simpler | More features |
# MAGIC ### COPY INTO Syntax
# MAGIC ```sql
# MAGIC COPY INTO table_name
# MAGIC FROM 'path'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC ```
# MAGIC `COPY_OPTIONS ('mergeSchema')` evolves the target table; `FORMAT_OPTIONS ('mergeSchema')` only merges schemas across the source files while reading.
# MAGIC
# MAGIC **Key benefit**: Safe to re-run - automatically skips already loaded files.
# MAGIC
# MAGIC You load events with COPY INTO and leave them in Bronze; no later notebook builds Silver or Gold from them.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 5: Use COPY INTO for Events
# MAGIC **Why**: COPY INTO is the SQL-only option for teams without streaming jobs; re-running it after a failure loads nothing twice.
# MAGIC
# MAGIC **Part 1**: Create the target table
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Table name: `{EVENTS_BRONZE_TABLE}`, format DELTA, created only if it does not exist yet
# MAGIC - Columns: event_id, customer_id, event_type, product_id, event_timestamp, device_type, browser, session_id, page_url
# MAGIC - All columns STRING (Bronze keeps the raw values; casting happens in Silver)
# MAGIC
# MAGIC **SQL Syntax:**
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS table_name (
# MAGIC column1 TYPE,
# MAGIC column2 TYPE
# MAGIC )
# MAGIC USING DELTA
# MAGIC ```

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Create the events bronze table

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {EVENTS_BRONZE_TABLE}
    (
        
        
        
        
        
        
        
        
        
    )
    USING DELTA
""")

print(f"Created table: {EVENTS_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Create Events Table

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {EVENTS_BRONZE_TABLE}
#     (
#         event_id STRING,
#         customer_id STRING,
#         event_type STRING,
#         product_id STRING,
#         event_timestamp STRING,
#         device_type STRING,
#         browser STRING,
#         session_id STRING,
#         page_url STRING
#     )
#     USING DELTA
# """)

# print(f"✅ Created table: {EVENTS_BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Part 2**: Load data with COPY INTO
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Copy from: `{EVENTS_LANDING_PATH}`
# MAGIC - File format: JSON
# MAGIC - Let the target table gain the source columns it does not declare yet (`referrer`, `ip_address`). Section 5 says which options clause evolves the target table and which only merges the source files.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Use COPY INTO to load events data

spark.sql(f"""




""")

print("COPY INTO completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: COPY INTO for Events

# spark.sql(f"""
#     COPY INTO {EVENTS_BRONZE_TABLE}
#     FROM '{EVENTS_LANDING_PATH}'
#     FILEFORMAT = JSON
#     COPY_OPTIONS ('mergeSchema' = 'true')
# """)

# print("✅ COPY INTO completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see:
# MAGIC - **102,843** events: `events_raw` holds exactly as many rows as the events landing files (the cell prints both, and their difference: 0).
# MAGIC - 11 columns: the 9 you declared plus `referrer` and `ip_address`, which `mergeSchema` added.

# COMMAND ----------

# 🔍 Verify events data
events_count = spark.table(EVENTS_BRONZE_TABLE).count()
landed_count = spark.read.json(EVENTS_LANDING_PATH).count()
print(f"Total events loaded: {events_count:,} (102,843 on a first run)")
print(f"Events in the landing files: {landed_count:,} (102,843 on a first run)")
print(f"Rows in events_raw minus events in the landing files: {events_count - landed_count:,} (expect 0)")
print(f"Columns: {len(spark.table(EVENTS_BRONZE_TABLE).columns)} (expect 11)")

display(spark.table(EVENTS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test COPY INTO Idempotency
# MAGIC Run COPY INTO again: it skips every file it already loaded.

# COMMAND ----------

# Re-run COPY INTO - should skip already loaded files
result = spark.sql(f"""
    COPY INTO {EVENTS_BRONZE_TABLE}
    FROM '{EVENTS_LANDING_PATH}'
    FILEFORMAT = JSON
""")

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC **Observation**: Notice `num_affected_rows` is 0 on the second run. COPY INTO tracks loaded files and skips them, making it safe for scheduled jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Schema Evolution and Rescued Data
# MAGIC When the source schema changes, Auto Loader reacts according to the evolution mode:
# MAGIC ### Schema Evolution Modes
# MAGIC 1. **addNewColumns** (default when Auto Loader infers the schema)
# MAGIC    - On a new column the stream FAILS with `UnknownFieldException` after recording the column in the schema location
# MAGIC    - The restart runs on the new schema, so run the stream in a job with retries (and `mergeSchema` on the Delta sink)
# MAGIC 2. **failOnNewColumns**
# MAGIC    - Fails the stream if schema changes
# MAGIC    - Use when strict schema control is needed
# MAGIC 3. **rescue**
# MAGIC    - Schema never evolves; new columns go to `_rescued_data` and the stream keeps running
# MAGIC    - Use when the table schema must not change
# MAGIC 4. **none** (default when you pass a schema)
# MAGIC    - Schema never evolves, new columns are ignored
# MAGIC ### The _rescued_data Column
# MAGIC When Auto Loader infers the schema it adds `_rescued_data` in every mode. It holds values that did not fit the schema (a column missing from the schema, a type mismatch, a case mismatch) as JSON with the source file path. The row still lands, and the typed column reads NULL. Rename the column, or add it to a schema you provide, with `.option("rescuedDataColumn", "_rescued")`.

# COMMAND ----------

# Example: Rescue mode pattern (demo only, not executed)

rescue_pattern = """
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("rescuedDataColumn", "_rescued") \
    .load(source_path)
"""

print("Rescue mode pattern:")
print(rescue_pattern)

# COMMAND ----------

# Check for rescued data in bronze tables

for table in [CUSTOMERS_BRONZE_TABLE, PRODUCTS_BRONZE_TABLE, SALES_BRONZE_TABLE]:
    try:
        rescued_count = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM {table}
            WHERE _rescued_data IS NOT NULL
        """).collect()[0]['count']
        
        if rescued_count > 0:
            print(f"⚠️  {table}: {rescued_count} records with rescued data")
        else:
            print(f"✅ {table}: No rescued data")
    except:
        print(f"ℹ️  {table}: No _rescued_data column (clean data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Best Practices for Schema Management
# MAGIC 1. ✅ **Start with schema inference** - Let Auto Loader discover the schema
# MAGIC 2. ✅ **Use addNewColumns mode in a job with retries** - A new column costs one automatic restart
# MAGIC 3. ✅ **Monitor rescued data** - Set up alerts for `_rescued_data IS NOT NULL`
# MAGIC ### Common Issues and Solutions
# MAGIC | Issue | Solution |
# MAGIC |-------|----------|
# MAGIC | Schema mismatch (a new column in the source) | With `addNewColumns` (the default for an inferred schema) the stream stops once and the restart picks up the new column, so run the stream in a job with retries. `rescue` keeps the stream running and sends new columns to `_rescued_data` |
# MAGIC | Values with the wrong type | They land in `_rescued_data`: alert on `_rescued_data IS NOT NULL` |
# MAGIC | Malformed records | `badRecordsPath` or the `_corrupt_record` column (not `_rescued_data`) |
# MAGIC | Type inference errors | Provide explicit schema or disable type inference |
# MAGIC | Slow file discovery at millions of files | File notification mode with file events (`cloudFiles.useManagedFileEvents = true` on an external location with file events enabled) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Monitoring Streaming Queries
# MAGIC Three checks to run after an ingestion: the active streaming queries, the table history, and the row count of every Bronze table.

# COMMAND ----------

# List all active streaming queries
active_streams = spark.streams.active

print(f"Active streaming queries: {len(active_streams)}")
for stream in active_streams:
    print(f"  - ID: {stream.id}")
    print(f"    Name: {stream.name}")
    print(f"    Status: {stream.status}")

# COMMAND ----------

# View table history to see streaming writes
display(spark.sql(f"DESCRIBE HISTORY {SALES_BRONZE_TABLE}").limit(10))

# COMMAND ----------

# Check record counts across all bronze tables
display(spark.sql(f"""
    SELECT 
        '{CUSTOMERS_BRONZE_TABLE}' as table_name,
        COUNT(*) as record_count
    FROM {CUSTOMERS_BRONZE_TABLE}
    
    UNION ALL
    
    SELECT 
        '{PRODUCTS_BRONZE_TABLE}' as table_name,
        COUNT(*) as record_count
    FROM {PRODUCTS_BRONZE_TABLE}
    
    UNION ALL
    
    SELECT 
        '{SALES_BRONZE_TABLE}' as table_name,
        COUNT(*) as record_count
    FROM {SALES_BRONZE_TABLE}
    
    UNION ALL
    
    SELECT 
        '{EVENTS_BRONZE_TABLE}' as table_name,
        COUNT(*) as record_count
    FROM {EVENTS_BRONZE_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC **1. Auto Loader Basics**
# MAGIC - `cloudFiles` format for incremental ingestion
# MAGIC - Schema inference and evolution
# MAGIC - Checkpointing for exactly-once processing
# MAGIC
# MAGIC **2. File Format Support**
# MAGIC - JSON: Semi-structured with automatic schema
# MAGIC - CSV: needs the header option (and `sep` when the delimiter is not a comma)
# MAGIC - Pattern applies to Parquet too
# MAGIC
# MAGIC **3. Streaming vs Batch**
# MAGIC - Auto Loader: Streaming with trigger options
# MAGIC - COPY INTO: Batch incremental with idempotency
# MAGIC - Triggers: `availableNow` (incremental batch), `processingTime` / default (always-on micro-batch, classic compute only)
# MAGIC
# MAGIC **4. Error Handling**
# MAGIC - Schema evolution: addNewColumns, failOnNewColumns, rescue, none
# MAGIC - Rescued data column for values that do not fit the schema
# MAGIC - Monitoring and alerting strategies
# MAGIC
# MAGIC **5. Production Patterns**
# MAGIC - Checkpoint management
# MAGIC - Schema location next to the checkpoint
# MAGIC - Query monitoring
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Write Auto Loader code for JSON and CSV?
# MAGIC - [ ] Configure schema inference and evolution?
# MAGIC - [ ] Explain checkpointing and its importance?
# MAGIC - [ ] Compare Auto Loader vs COPY INTO?
# MAGIC - [ ] Handle schema errors with rescue mode?
# MAGIC - [ ] Choose appropriate trigger types?
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 03** covers:
# MAGIC - Bronze to Silver transformations
# MAGIC - Data quality checks
# MAGIC - Deduplication strategies
# MAGIC - SCD Type 2 patterns
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
# MAGIC All four Bronze tables are populated. Next: Notebook 03 builds Silver from customers, products and sales.
