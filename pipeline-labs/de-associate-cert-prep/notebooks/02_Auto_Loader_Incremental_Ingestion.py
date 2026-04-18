# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Auto Loader & Incremental Ingestion
# MAGIC **Exam Coverage**: Section 2 (ELT with Spark SQL and Python)
# MAGIC **Duration**: 45-60 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Use Auto Loader to incrementally ingest JSON files
# MAGIC - Configure schema inference and evolution in Auto Loader
# MAGIC - Implement streaming ingestion for CSV files
# MAGIC - Use COPY INTO for batch incremental loads
# MAGIC - Handle schema inference errors and rescued data columns
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
# MAGIC ### Basic Auto Loader Pattern
# MAGIC ```python
# MAGIC spark.readStream \
# MAGIC .format("cloudFiles") \
# MAGIC .option("cloudFiles.format", "json") \
# MAGIC .option("cloudFiles.schemaLocation", checkpoint_path) \
# MAGIC .load(source_path) \
# MAGIC .writeStream \
# MAGIC .option("checkpointLocation", checkpoint_path) \
# MAGIC .trigger(availableNow=True) \
# MAGIC .table(table_name)
# MAGIC ```
# MAGIC Let's start by importing our configuration.

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
# MAGIC **Requirements:**
# MAGIC - Use `.readStream` with format "cloudFiles"
# MAGIC - Set `cloudFiles.format` to "json"
# MAGIC - Store schema in `CUSTOMERS_CHECKPOINT_PATH`
# MAGIC - Enable column type inference
# MAGIC - Load from `CUSTOMERS_LANDING_PATH`
# MAGIC - Store result in variable `customers_stream`
# MAGIC
# MAGIC **Key options:**
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "json")
# MAGIC .option("cloudFiles.schemaLocation", path)
# MAGIC .option("cloudFiles.inferColumnTypes", "true")
# MAGIC ```
# MAGIC **Hint**: Use the pattern from Section 1 introduction.

# COMMAND ----------

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
# MAGIC **Requirements:**
# MAGIC - Format: "delta"
# MAGIC - Output mode: "append"
# MAGIC - Checkpoint: `CUSTOMERS_CHECKPOINT_PATH`
# MAGIC - Trigger: `availableNow=True` (process all available data then stop)
# MAGIC - Target table: `CUSTOMERS_BRONZE_TABLE`
# MAGIC - Store query in `customers_query`
# MAGIC - Wait for completion with `.awaitTermination()`
# MAGIC
# MAGIC **Syntax:**
# MAGIC ```python
# MAGIC stream.writeStream \
# MAGIC .format("delta") \
# MAGIC .outputMode("append") \
# MAGIC .option("checkpointLocation", ...)
# MAGIC .trigger(availableNow=True) \
# MAGIC .table(...)
# MAGIC ```
# MAGIC **Hint**: Use `trigger(availableNow=True)` for micro-batch processing.

# COMMAND ----------

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

# Verify data was loaded
customer_count = spark.table(CUSTOMERS_BRONZE_TABLE).count()
print(f"Total customers loaded: {customer_count:,}")

# Display sample records
display(spark.table(CUSTOMERS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding Checkpoints
# MAGIC Let's examine what's in the checkpoint directory:

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
# MAGIC This ensures exactly-once processing even if the stream fails and restarts.

# COMMAND ----------

# MAGIC %md
# MAGIC **Extra Practice**
# MAGIC
# MAGIC If you want, you can:
# MAGIC
# MAGIC - Run the `data_generator` notebook
# MAGIC - Read and write again to see how it processes the new data!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Auto Loader for CSV with Schema Evolution
# MAGIC CSV files require additional configuration:
# MAGIC - Header row handling
# MAGIC - Delimiter specification
# MAGIC - Schema evolution strategy
# MAGIC ### Schema Evolution Modes
# MAGIC | Mode | Behavior |
# MAGIC |------|----------|
# MAGIC | `addNewColumns` | Automatically add new columns (default) |
# MAGIC | `failOnNewColumns` | Fail the stream if schema changes |
# MAGIC | `rescue` | Store unparseable data in `_rescued_data` |
# MAGIC **Best practice**: Use `addNewColumns` for flexible data ingestion.

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
# MAGIC **Requirements:**
# MAGIC - Format: "cloudFiles"
# MAGIC - File format: CSV with headers
# MAGIC - Schema location: `PRODUCTS_CHECKPOINT_PATH`
# MAGIC - Enable type inference
# MAGIC - Schema evolution mode: "addNewColumns"
# MAGIC - CSV header option: "true"
# MAGIC - Load from: `PRODUCTS_LANDING_PATH`
# MAGIC
# MAGIC **CSV-specific options:**
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "csv")
# MAGIC .option("header", "true")
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC ```
# MAGIC **Hint**: Similar to JSON Auto Loader, but with CSV-specific options.

# COMMAND ----------

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

# Verify products data
product_count = spark.table(PRODUCTS_BRONZE_TABLE).count()
print(f"Total products loaded: {product_count:,}")

display(spark.table(PRODUCTS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Streaming Triggers
# MAGIC Auto Loader supports different trigger types for controlling when data is processed.
# MAGIC ### Trigger Types
# MAGIC | Trigger | Behavior | Use Case |
# MAGIC |---------|----------|----------|
# MAGIC | `availableNow=True` | Process all available data, then stop | Micro-batch, testing |
# MAGIC | `processingTime='10 seconds'` | Trigger every 10 seconds | Near real-time |
# MAGIC | `once=True` | Process once and stop (deprecated) | Use availableNow instead |
# MAGIC | No trigger | Continuous processing | True streaming |
# MAGIC
# MAGIC **For production**: Use `processingTime` for near real-time or no trigger for continuous.
# MAGIC
# MAGIC **For this lab**: We use `availableNow=True` to avoid long-running streams.

# COMMAND ----------

# Check sales landing data
print(f"Sales landing path: {SALES_LANDING_PATH}")
display(dbutils.fs.ls(SALES_LANDING_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Streaming with Different Triggers
# MAGIC **Your task**: Create an Auto Loader stream for sales data (high-volume JSON).
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Read JSON from `SALES_LANDING_PATH`
# MAGIC - Schema location: `SALES_CHECKPOINT_PATH`
# MAGIC - Enable type inference
# MAGIC - Write to `SALES_BRONZE_TABLE`
# MAGIC - Use `trigger(availableNow=True)` for this lab
# MAGIC
# MAGIC **Note**: In production, you might use:
# MAGIC ```python
# MAGIC .trigger(processingTime='30 seconds')  # Near real-time
# MAGIC # or no trigger for continuous
# MAGIC ```
# MAGIC **Hint**: Same pattern as customers, but with sales paths.

# COMMAND ----------

# TODO: Create Auto Loader stream for sales JSON data

sales_stream = spark.readStream \





sales_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Auto Loader for Sales

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

# Verify sales data
sales_count = spark.table(SALES_BRONZE_TABLE).count()
print(f"Total sales loaded: {sales_count:,}")

display(spark.table(SALES_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: COPY INTO for Batch Incremental Loads
# MAGIC COPY INTO is a SQL command for idempotent, incremental batch loading.
# MAGIC ### COPY INTO vs Auto Loader
# MAGIC | Feature | COPY INTO | Auto Loader |
# MAGIC |---------|-----------|-------------|
# MAGIC | **Type** | Batch (SQL) | Streaming (Python/SQL) |
# MAGIC | **Idempotent** | Yes | Yes |
# MAGIC | **Checkpointing** | Automatic | Manual setup |
# MAGIC | **Use Case** | Scheduled batch jobs | Continuous ingestion |
# MAGIC | **Complexity** | Simpler | More features |
# MAGIC ### COPY INTO Syntax
# MAGIC ```sql
# MAGIC COPY INTO target_table
# MAGIC FROM source_path
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC ```
# MAGIC **Key benefit**: Safe to re-run - automatically skips already loaded files.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 5: Use COPY INTO for Events
# MAGIC **Part 1**: Create the target table
# MAGIC **Requirements:**
# MAGIC - Table name: `{EVENTS_BRONZE_TABLE}`
# MAGIC - Format: DELTA
# MAGIC - Columns: event_id, customer_id, event_type, product_id, event_timestamp, device_type, browser, session_id, page_url
# MAGIC - All columns: STRING type (except event_timestamp: TIMESTAMP)
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
#     CREATE OR REPLACE TABLE {EVENTS_BRONZE_TABLE}
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
# MAGIC **Requirements:**
# MAGIC - Copy from: `{EVENTS_LANDING_PATH}`
# MAGIC - File format: JSON
# MAGIC - Enable merge schema (format option)
# MAGIC **SQL Syntax:**
# MAGIC ```sql
# MAGIC COPY INTO table_name
# MAGIC FROM 'path'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC ```

# COMMAND ----------

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

spark.sql(f"""
    COPY INTO {EVENTS_BRONZE_TABLE}
    FROM '{EVENTS_LANDING_PATH}'
    FILEFORMAT = JSON
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

print("✅ COPY INTO completed")

# COMMAND ----------

# Verify events data
events_count = spark.table(EVENTS_BRONZE_TABLE).count()
print(f"Total events loaded: {events_count:,}")

display(spark.table(EVENTS_BRONZE_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test COPY INTO Idempotency
# MAGIC Let's verify that COPY INTO skips already-loaded files:

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
# MAGIC Real-world data is messy. Auto Loader provides mechanisms to handle issues:
# MAGIC ### Schema Evolution Modes
# MAGIC 1. **addNewColumns** (recommended)
# MAGIC - Automatically adds new columns when detected
# MAGIC - Existing queries continue to work
# MAGIC 2. **failOnNewColumns**
# MAGIC - Fails the stream if schema changes
# MAGIC - Use when strict schema control is needed
# MAGIC 3. **rescue**
# MAGIC - Stores unparseable data in `_rescued_data` column
# MAGIC - Prevents data loss from parsing errors
# MAGIC ### The _rescued_data Column
# MAGIC When using rescue mode:
# MAGIC ```python
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC .option("cloudFiles.rescuedDataColumn", "_rescued_data")
# MAGIC ```
# MAGIC Unparseable records are saved in `_rescued_data` instead of being dropped.

# COMMAND ----------

# Example: Rescue mode pattern (demo only, not executed)

rescue_pattern = """
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
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
# MAGIC 2. ✅ **Use addNewColumns mode** - Flexible and safe for most cases
# MAGIC 3. ✅ **Monitor rescued data** - Set up alerts for `_rescued_data IS NOT NULL`
# MAGIC 4. ✅ **Version your schemas** - Store inferred schemas in source control
# MAGIC 5. ✅ **Test with sample data** - Validate schema before production
# MAGIC ### Common Issues and Solutions
# MAGIC | Issue | Solution |
# MAGIC |-------|----------|
# MAGIC | Schema mismatch | Enable `schemaEvolutionMode = "addNewColumns"` |
# MAGIC | Corrupt records | Use `schemaEvolutionMode = "rescue"` |
# MAGIC | Type inference errors | Provide explicit schema or disable type inference |
# MAGIC | Performance issues | Use `cloudFiles.useIncrementalListing = true` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Monitoring Streaming Queries
# MAGIC Production streaming pipelines need monitoring. Databricks provides several tools.

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
# MAGIC **2. File Format Support**
# MAGIC - JSON: Semi-structured with automatic schema
# MAGIC - CSV: Requires header and delimiter options
# MAGIC - Pattern applies to Parquet too
# MAGIC **3. Streaming vs Batch**
# MAGIC - Auto Loader: Streaming with trigger options
# MAGIC - COPY INTO: Batch incremental with idempotency
# MAGIC - Triggers: `availableNow`, `processingTime`, continuous
# MAGIC **4. Error Handling**
# MAGIC - Schema evolution: addNewColumns, failOnNewColumns, rescue
# MAGIC - Rescued data column for unparseable records
# MAGIC - Monitoring and alerting strategies
# MAGIC **5. Production Patterns**
# MAGIC - Checkpoint management
# MAGIC - Schema versioning
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
# MAGIC You've mastered Auto Loader and incremental ingestion. All bronze tables are populated. Proceed to Notebook 03 to transform this data into the Silver layer.
