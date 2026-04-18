# Databricks notebook source
# MAGIC %md
# MAGIC # Zenith Online - Data Transformation Pipeline (Unity Catalog)
# MAGIC
# MAGIC This notebook implements the full Bronze-Silver-Gold ETL pipeline for Zenith Online, fully integrated with Databricks Unity Catalog. It is designed to be run after the data generator has populated the landing zone volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Initialization
# MAGIC
# MAGIC Import required libraries and set up Spark session. Define all paths and table names for Unity Catalog integration.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
import pandas as pd

# In Databricks, the SparkSession `spark` is already created for you.

# COMMAND ----------

from variables import CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, SCHEMA_BASE_PATH, RAW_STREAMING_PATH, RAW_BATCH_PRODUCTS_PATH, RAW_BATCH_CUSTOMERS_PATH, CHECKPOINT_BASE_PATH

# Full table names
BRONZE_EVENTS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_user_events"
BRONZE_CUSTOMERS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_customer_profiles"
BRONZE_PRODUCTS_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_product_details"
SILVER_TABLE = f"{CATALOG_NAME}.{SILVER_SCHEMA}.silver_sessionized_activity"
GOLD_DAILY_PRODUCT_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.gold_daily_product_performance"
GOLD_CUSTOMER_SUMMARY_TABLE = f"{CATALOG_NAME}.{GOLD_SCHEMA}.customer_purchase_summary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Environment Setup
# MAGIC
# MAGIC Create the catalog and schemas if they do not exist. This ensures all downstream tables are created in the correct namespace.

# COMMAND ----------

# MAGIC %run "./environment_setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate initial data

# COMMAND ----------

from data_generator import generate_batch_data, generate_streaming_events
generate_batch_data()

# COMMAND ----------

generate_streaming_events(infinite=False, stream_events_per_batch=50000)
# Infinite streaming is disabled by infinite=False, which means it will just generate one batch of data and stop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 - Bronze Layer: Raw Data Ingestion
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
# MAGIC **Why:** Establishes a governed, append-only raw fact table capturing every user interaction with minimal transformation so downstream layers can always trace back to the original event. Using Auto Loader plus an explicit schema guarantees schema drift control and reliable incremental ingestion even as new files arrive.
# MAGIC
# MAGIC **End Goal:** A durable Delta table (`bronze_user_events`) in the Bronze schema that faithfully mirrors the raw JSON feed with added timestamps for lineage, enabling reproducible enrichment and time-aware processing in Silver.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Auto Loader (`cloudFiles`)**: Incrementally processes new files as they arrive
# MAGIC - **Explicit Schema**: Prevents schema drift and ensures data quality
# MAGIC - **Checkpointing**: Enables fault-tolerant stream processing
# MAGIC - **Metadata Columns**: Add timestamps for data lineage
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Print: `"Starting Bronze Layer: Streaming Events Ingestion"`
# MAGIC 2. Define explicit schema with these fields (all nullable=True):
# MAGIC    - `event_id` → StringType
# MAGIC    - `event_timestamp` → StringType
# MAGIC    - `user_id` → IntegerType
# MAGIC    - `event_type` → StringType
# MAGIC    - `product_id` → IntegerType
# MAGIC    - `session_id` → StringType
# MAGIC 3. Use Auto Loader to read streaming JSON:
# MAGIC    - Format: `"cloudFiles"`
# MAGIC    - Option: `cloudFiles.format` = `"json"`
# MAGIC    - Option: `cloudFiles.schemaLocation` = `f"{SCHEMA_BASE_PATH}/bronze_events"`
# MAGIC    - Apply schema and load from `RAW_STREAMING_PATH`
# MAGIC 4. Add derived columns:
# MAGIC    - `ingestion_timestamp` using `F.current_timestamp()`
# MAGIC    - `event_dt` by converting `event_timestamp` to timestamp using `F.to_timestamp()`
# MAGIC 5. Write stream to Delta table:
# MAGIC    - Format: `"delta"`, Output mode: `"append"`
# MAGIC    - Checkpoint: `f"{CHECKPOINT_BASE_PATH}/bronze_events"`
# MAGIC    - Trigger: `availableNow=True`
# MAGIC    - Target: `toTable(BRONZE_EVENTS_TABLE)`
# MAGIC 6. Print completion message with table name

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

print("Starting Bronze Layer: Streaming Events Ingestion")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("session_id", StringType(), True)
])

bronze_events_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE_PATH}/bronze_events")
    .schema(event_schema)
    .load(RAW_STREAMING_PATH)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("event_dt", F.to_timestamp("event_timestamp"))
)

(
    bronze_events_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/bronze_events")
    .trigger(availableNow=True)
    .toTable(BRONZE_EVENTS_TABLE)
)

print(f"Bronze Layer: Streaming events processing complete. Table `{BRONZE_EVENTS_TABLE}` is updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Customer & Product Data
# MAGIC Ingest customer profiles (CSV) and product details (Parquet) into Bronze tables for further enrichment.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 2: Create `bronze_customer_profiles` Batch Table
# MAGIC
# MAGIC **Why:** Converts a periodically refreshed dimension snapshot (customers) into a managed Delta table so it can be efficiently joined with streaming facts. Enforcing a schema up front prevents accidental type widening (e.g., inferring customer_id as string) that would later break joins.
# MAGIC
# MAGIC **End Goal:** A clean, type-consistent customer dimension (`bronze_customer_profiles`) ready for enrichment joins that preserves referential integrity with events.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Batch Loading**: Read static files with explicit schemas
# MAGIC - **Schema Enforcement**: Define data types to prevent inference errors
# MAGIC - **Delta Overwrite**: Replace existing data with new snapshots
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Print: `"Starting Bronze Layer: Batch Ingestion"`
# MAGIC 2. Define `customer_schema` with:
# MAGIC    - `customer_id` → IntegerType, nullable=False
# MAGIC    - `signup_date` → StringType, nullable=True
# MAGIC    - `location` → StringType, nullable=True
# MAGIC 3. Read CSV: `spark.read.format("csv").schema(customer_schema).option("header", "true").load(RAW_BATCH_CUSTOMERS_PATH)`
# MAGIC 4. Write to Delta: `.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CUSTOMERS_TABLE)`
# MAGIC 5. Print completion message with table name

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

print("Starting Bronze Layer: Batch Ingestion")

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("signup_date", StringType(), True),
    StructField("location", StringType(), True)
])
customers_df = spark.read.format("csv").schema(customer_schema).option("header", "true").load(RAW_BATCH_CUSTOMERS_PATH)
customers_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CUSTOMERS_TABLE)
print(f"Bronze Layer: Wrote customer profiles to `{BRONZE_CUSTOMERS_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 TASK 3: Create `bronze_product_details` Batch Table
# MAGIC
# MAGIC **Why:** Provides a governed product dimension (names, categories, prices) required to translate low-level product_id references in events into analytics-friendly attributes and monetary values.
# MAGIC
# MAGIC **End Goal:** A Delta product dimension (`bronze_product_details`) enabling category/revenue calculations and later skew-mitigated aggregations.
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Read Parquet: `spark.read.format("parquet").load(RAW_BATCH_PRODUCTS_PATH)`
# MAGIC 2. Write to Delta: `.write.format("delta").mode("overwrite").saveAsTable(BRONZE_PRODUCTS_TABLE)`
# MAGIC 3. Print completion message with table name

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

products_df = spark.read.format("parquet").load(RAW_BATCH_PRODUCTS_PATH)
products_df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_PRODUCTS_TABLE)
print(f"Bronze Layer: Wrote product details to `{BRONZE_PRODUCTS_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 - Silver Layer - Clean, Enrich, and De-duplicate
# MAGIC
# MAGIC **Goal:** Combine raw data sources, clean the data, remove duplicates, and create an enriched, queryable table for deeper analysis.
# MAGIC
# MAGIC **Input:** Bronze layer tables.
# MAGIC
# MAGIC **Output:** A central, enriched table: `silver_sessionized_activity`.
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
# MAGIC **Why:** Normalizes granular location strings into a smaller, business-recognized region taxonomy to simplify segmentation, reduce cardinality in aggregations, and accelerate downstream queries.
# MAGIC
# MAGIC **End Goal:** A reusable function (`categorize_region`) producing a standardized region column used for customer and revenue rollups in Silver and Gold tables.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Pandas UDF**: Enables vectorized operations (much faster than row-by-row Python UDFs)
# MAGIC - **Series Processing**: Operates on pandas Series for efficient batch transformations
# MAGIC - **Type Hints**: Required for Pandas UDFs (`pd.Series` input/output)
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Use decorator: `@F.pandas_udf(StringType())`
# MAGIC 2. Define function signature: `def categorize_region(locations: pd.Series) -> pd.Series:`
# MAGIC 3. Inside function, create two lists:
# MAGIC    - `east_coast = ['New York', 'Philadelphia']`
# MAGIC    - `west_coast = ['Los Angeles', 'San Diego']`
# MAGIC 4. Define helper function `get_region(location)`:
# MAGIC    - Return `'East Coast'` if location in `east_coast`
# MAGIC    - Return `'West Coast'` if location in `west_coast`
# MAGIC    - Return `'Central'` otherwise
# MAGIC 5. Return: `locations.apply(get_region)`
# MAGIC 6. Print: `"Silver Layer: Pandas UDF \`categorize_region\` created."`

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

@F.pandas_udf(StringType())
def categorize_region(locations: pd.Series) -> pd.Series:
    east_coast = ['New York', 'Philadelphia']
    west_coast = ['Los Angeles', 'San Diego']
    def get_region(location):
        if location in east_coast: return 'East Coast'
        elif location in west_coast: return 'West Coast'
        else: return 'Central'
    return locations.apply(get_region)

print("Silver Layer: Pandas UDF `categorize_region` created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Transformation
# MAGIC Deduplicate streaming events, join with customer and product data, enrich with region, and write to the Silver Delta table.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 5: Create `silver_sessionized_activity` Streaming Table
# MAGIC
# MAGIC **Why:** Transforms noisy raw events into a de-duplicated, enriched activity fact set combining product, customer, temporal, and derived region context—shaping data into a form suited for consistent business aggregation without re-implementing joins each time.
# MAGIC
# MAGIC **End Goal:** A single conformed Silver table (`silver_sessionized_activity`) that is the authoritative behavioral dataset powering all Gold-layer KPIs.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Watermarking**: Handles late-arriving data (defines how long to wait)
# MAGIC - **Stream-Static Joins**: Join streaming fact table with static dimension tables
# MAGIC - **Broadcast Join**: Optimize small dimension table joins
# MAGIC - **Deduplication**: Remove duplicate events based on unique key
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Print: `"Starting Silver Layer: Enriching and Cleaning Events"`
# MAGIC 2. Read data:
# MAGIC    - `bronze_events_stream_df = spark.readStream.table(BRONZE_EVENTS_TABLE)`
# MAGIC    - `customers_df = spark.read.table(BRONZE_CUSTOMERS_TABLE)`
# MAGIC    - `products_df = spark.read.table(BRONZE_PRODUCTS_TABLE)`
# MAGIC 3. Apply watermarking and deduplication:
# MAGIC    - Watermark: `.withWatermark("event_dt", "3 minutes")`
# MAGIC    - Deduplication: `.dropDuplicates(["event_id"])`
# MAGIC 4. Enrich the data via joins:
# MAGIC    - **Inner join** with broadcast products on `product_id`
# MAGIC    - **Left join** with customers: `deduplicated_stream_df.user_id == customers_df.customer_id`
# MAGIC 5. Apply column transformations:
# MAGIC    - Add region: `.withColumn("region", categorize_region(F.col("location")))`
# MAGIC    - Rename: `.withColumnRenamed("event_type", "action")`
# MAGIC    - Add date: `.withColumn("event_date", F.to_date(F.col("event_dt")))`
# MAGIC 6. Select final schema: `event_id`, `event_dt`, `event_date`, `action`, `user_id`, `region`, `product_id`, `product_name`, `category`, `price`
# MAGIC    (Use `deduplicated_stream_df.user_id` and `deduplicated_stream_df.product_id` to avoid ambiguity)
# MAGIC 7. Write stream:
# MAGIC    - Format: `"delta"`, Output mode: `"append"`
# MAGIC    - Checkpoint: `f"{CHECKPOINT_BASE_PATH}/silver_activity"`
# MAGIC    - Trigger: `availableNow=True`
# MAGIC    - Target: `toTable(SILVER_TABLE)`
# MAGIC 8. Print completion message

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

print("Starting Silver Layer: Enriching and Cleaning Events")

bronze_events_stream_df = spark.readStream.table(BRONZE_EVENTS_TABLE)
customers_df = spark.read.table(BRONZE_CUSTOMERS_TABLE)
products_df = spark.read.table(BRONZE_PRODUCTS_TABLE)

deduplicated_stream_df = (
    bronze_events_stream_df
        .withWatermark("event_dt", "3 minutes")
        .dropDuplicates(["event_id"])
)

enriched_df = (
    deduplicated_stream_df
    .join(
        F.broadcast(products_df),
        deduplicated_stream_df.product_id == products_df.product_id,
        "inner"
    )
    .join(
        customers_df,
        deduplicated_stream_df.user_id == customers_df.customer_id,
        "left"
    )
    .withColumn("region", categorize_region(F.col("location")))
    .withColumnRenamed("event_type", "action")
    .withColumn("event_date", F.to_date(F.col("event_dt")))
    .select(
        "event_id", "event_dt", "event_date", "action",
        deduplicated_stream_df.user_id, "region",
        deduplicated_stream_df.product_id, "product_name", "category", "price"
    )
)

(
    enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/silver_activity")
    .trigger(availableNow=True)
    .toTable(SILVER_TABLE)
)

print(f"Silver Layer: Enriched activity stream processing complete. Table `{SILVER_TABLE}` is updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 - Gold Layer: Business-Focused Aggregations
# MAGIC
# MAGIC **Goal:** Build aggregated, denormalized tables that directly answer key business questions and are optimized for BI tools.
# MAGIC
# MAGIC **Target Schema:** `03_gold`
# MAGIC
# MAGIC **Input:** Silver layer table (`silver_sessionized_activity`).
# MAGIC
# MAGIC **Output:**
# MAGIC - `gold_daily_product_performance`
# MAGIC - `customer_purchase_summary`
# MAGIC
# MAGIC ---
# MAGIC ### Daily Product Performance (Handling Data Skew)
# MAGIC Aggregate Silver data to compute daily product metrics, using salting to mitigate data skew for popular products.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 6: Create `gold_daily_product_performance` Aggregate Table
# MAGIC
# MAGIC **Why:** Produces daily product performance metrics (views, funnel progression, revenue) optimized for BI dashboards and trend analyses. Salting mitigates impact of skewed hot products, ensuring stable performance and accurate ranking calculations.
# MAGIC
# MAGIC **End Goal:** A partitioned, query-ready Delta table (`gold_daily_product_performance`) enabling rapid insights into product engagement and monetization by day and category with embedded revenue ranking.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Data Skew**: When one key has significantly more data (e.g., popular products)
# MAGIC - **Salting**: Add random suffix to hot keys to distribute processing across partitions
# MAGIC - **Two-Stage Aggregation**: Aggregate with salt, then remove salt and re-aggregate
# MAGIC - **Window Functions**: Rank products by revenue within each category
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Print: `"Starting Gold Layer: Daily Product Performance Aggregation"`
# MAGIC 2. Read: `silver_df = spark.read.table(SILVER_TABLE)`
# MAGIC 3. **Handle Data Skew (First Aggregation with salt)**:
# MAGIC    - Add salt column: `.withColumn("salt", (F.rand() * 5).cast("int"))`
# MAGIC    - Group by: `event_date`, `product_id`, `product_name`, `category`, `salt`
# MAGIC    - Calculate using conditional aggregations:
# MAGIC      - `views` = count when `action == "view_product"`
# MAGIC      - `adds_to_cart` = count when `action == "add_to_cart"`
# MAGIC      - `purchases` = count when `action == "purchase"`
# MAGIC      - `daily_revenue` = sum of `price` when `action == "purchase"`, else 0
# MAGIC 4. **Final Aggregation (remove salt)**:
# MAGIC    - Group by: `event_date`, `product_id`, `product_name`, `category`
# MAGIC    - Sum metrics: `total_views`, `total_adds_to_cart`, `total_purchases`, `total_revenue`
# MAGIC 5. **Add Product Ranking**:
# MAGIC    - Create window: `Window.partitionBy("event_date", "category").orderBy(F.col("total_revenue").desc())`
# MAGIC    - Add column: `revenue_rank` using `F.rank().over(window_spec)`
# MAGIC 6. **Write the Gold Table**:
# MAGIC    - Format: `"delta"`, Mode: `"overwrite"`
# MAGIC    - Option: `overwriteSchema` = `"true"`
# MAGIC    - Partition by: `event_date`
# MAGIC    - Save to: `GOLD_DAILY_PRODUCT_TABLE`
# MAGIC 7. Print completion message

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

print("Starting Gold Layer: Daily Product Performance Aggregation")
silver_df = spark.read.table(SILVER_TABLE)

SALTING_FACTOR = 5
salted_df = silver_df.withColumn("salt", (F.rand() * SALTING_FACTOR).cast("int"))

salted_agg_df = (
    salted_df.groupBy("event_date", "product_id", "product_name", "category", "salt")
    .agg(
        F.count(F.when(F.col("action") == "view_product", 1)).alias("views"),
        F.count(F.when(F.col("action") == "add_to_cart", 1)).alias("adds_to_cart"),
        F.count(F.when(F.col("action") == "purchase", 1)).alias("purchases"),
        F.sum(F.when(F.col("action") == "purchase", F.col("price")).otherwise(0)).alias("daily_revenue")
    )
)

daily_product_performance_df = (
    salted_agg_df.groupBy("event_date", "product_id", "product_name", "category")
    .agg(
        F.sum("views").alias("total_views"),
        F.sum("adds_to_cart").alias("total_adds_to_cart"),
        F.sum("purchases").alias("total_purchases"),
        F.sum("daily_revenue").alias("total_revenue")
    )
)

window_spec = Window.partitionBy("event_date", "category").orderBy(F.col("total_revenue").desc())
final_product_gold_df = daily_product_performance_df.withColumn("revenue_rank", F.rank().over(window_spec))

(
    final_product_gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable(GOLD_DAILY_PRODUCT_TABLE)
)

print(f"Gold Layer: Wrote to `{GOLD_DAILY_PRODUCT_TABLE}` after handling skew.")

# COMMAND ----------

display(final_product_gold_df.count())
display(spark.table(GOLD_DAILY_PRODUCT_TABLE).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Purchase Summary
# MAGIC Aggregate customer-level purchase metrics and perform a data quality check for missing regions.
# MAGIC
# MAGIC ---
# MAGIC ### 🎯 TASK 7: Create `customer_purchase_summary` Aggregate Table
# MAGIC
# MAGIC **Why:** Consolidates purchase behavior per customer to support LTV-style analysis, top customer identification, and targeted marketing segmentation. The null region count surfaces data completeness issues early.
# MAGIC
# MAGIC **End Goal:** A Gold table (`customer_purchase_summary`) listing monetization KPIs per customer, sorted for immediate consumption by business stakeholders.
# MAGIC
# MAGIC **Key Concepts:**
# MAGIC - **Data Quality Checks**: Monitor NULL values before aggregation
# MAGIC - **Customer Segmentation**: Group by customer and region for analysis
# MAGIC - **Approximate Aggregations**: Use `approx_count_distinct` for large-scale distinct counts
# MAGIC - **Serverless Compatibility**: Direct filter/count instead of accumulators
# MAGIC
# MAGIC **Your Task:**
# MAGIC 1. Print: `"Starting Gold Layer: Customer Purchase Summary"`
# MAGIC 2. Read: `silver_df = spark.read.table(SILVER_TABLE)`
# MAGIC 3. **Perform Data Quality Check**:
# MAGIC    - Count NULL regions: `unknown_location_count = silver_df.filter(F.col("region").isNull()).count()`
# MAGIC    - Print: `f"Data Quality Check: Found {unknown_location_count} events with unknown customer locations."`
# MAGIC 4. **Filter and Aggregate**:
# MAGIC    - Filter for purchase actions only: `.filter(F.col("action") == 'purchase')`
# MAGIC    - Group by: `user_id`, `region`
# MAGIC    - Calculate:
# MAGIC      - `total_purchase_value` = sum of `price`
# MAGIC      - `total_purchases` = count of `event_id`
# MAGIC      - `distinct_products_purchased` = approx distinct count of `product_id`
# MAGIC      - `last_purchase_timestamp` = max of `event_dt`
# MAGIC 5. **Sort and Write**:
# MAGIC    - Order by: `total_purchase_value` descending
# MAGIC    - Write: Format `"delta"`, Mode `"overwrite"` to `GOLD_CUSTOMER_SUMMARY_TABLE`
# MAGIC 6. Print completion message
# MAGIC
# MAGIC **Note**: Accumulators are not available on serverless compute. Use direct count approach instead.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **✅ Solution below** ⬇️

# COMMAND ----------

print("Starting Gold Layer: Customer Purchase Summary")
silver_df = spark.read.table(SILVER_TABLE)

# --- Data Quality Check (Serverless-Compatible Method) ---
# # Accumulators are not available on serverless compute as they require direct sparkContext access.
# unknown_location_count = spark.sparkContext.accumulator(0)
# def count_unknown_locations(region):
#     if region is None:
#         global unknown_location_count
#         unknown_location_count += 1
#     return region
# count_unknown_udf = F.udf(count_unknown_locations, StringType())
# The best practice is to perform a direct count() action on a filtered DataFrame.
# This achieves the same goal of gathering a metric from the data.
unknown_location_count = silver_df.filter(F.col("region").isNull()).count()

print(f"Data Quality Check: Found {unknown_location_count} events with unknown customer locations.")

customer_summary_df = (
    silver_df.filter(F.col("action") == 'purchase')
    .groupBy("user_id", "region")
    .agg(
        F.sum("price").alias("total_purchase_value"),
        F.count("event_id").alias("total_purchases"),
        F.approx_count_distinct("product_id").alias("distinct_products_purchased"),
        F.max("event_dt").alias("last_purchase_timestamp")
    )
    .orderBy(F.col("total_purchase_value").desc())
)

(
    customer_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_CUSTOMER_SUMMARY_TABLE)
)
print(f"Gold Layer: Wrote to `{GOLD_CUSTOMER_SUMMARY_TABLE}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answering Business Questions
# MAGIC
# MAGIC Now that our Gold tables exist in Unity Catalog, we can query them directly with SQL.
# MAGIC
# MAGIC ### Business Question 1:
# MAGIC What are the top 5 most purchased products for the most recent day?
# MAGIC
# MAGIC Query the Gold product performance table to find the most popular products.

# COMMAND ----------

print("--- Top 5 Purchased Products (Most Recent Day) ---")
spark.sql(f"""
    SELECT
        event_date,
        product_name,
        category,
        total_purchases,
        total_revenue
    FROM {GOLD_DAILY_PRODUCT_TABLE}
    --WHERE event_date = (SELECT MAX(event_date) FROM {GOLD_DAILY_PRODUCT_TABLE})
    ORDER BY total_purchases DESC
    LIMIT 5
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Question 2:
# MAGIC What is the daily sales revenue per category?
# MAGIC
# MAGIC Summarize daily revenue by product category for trend analysis.

# COMMAND ----------

print("--- Daily Sales Revenue per Category ---")
spark.sql(f"""
    SELECT
        event_date,
        category,
        SUM(total_revenue) as category_revenue
    FROM {GOLD_DAILY_PRODUCT_TABLE}
    GROUP BY event_date, category
    ORDER BY event_date DESC, category_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Question 3:
# MAGIC What are the top 10 Customers by purchase value?
# MAGIC
# MAGIC Identify the highest-value customers based on total purchase value.

# COMMAND ----------

print("--- Top 10 Customers by Purchase Value ---")
spark.sql(f"""
    SELECT
        user_id,
        region,
        total_purchase_value,
        total_purchases,
        last_purchase_timestamp
    FROM {GOLD_CUSTOMER_SUMMARY_TABLE}
    ORDER BY total_purchase_value DESC
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Pipeline
# MAGIC
# MAGIC The script has successfully processed data through the Bronze, Silver, and Gold layers within Unity Catalog, creating valuable business assets and demonstrating key capabilities tested in the Spark Developer certification exam.
