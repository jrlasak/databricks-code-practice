# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Bronze to Silver Transformations
# MAGIC **Exam Coverage**: Section 3 (Incremental Data Processing)
# MAGIC **Duration**: 60-75 minutes
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Implement data quality transformations (deduplication, standardization)
# MAGIC - Validate and cleanse customer and product data
# MAGIC - Process streaming sales with joins and enrichment
# MAGIC - Implement Slowly Changing Dimension (SCD) Type 2
# MAGIC - Create data quality monitoring dashboards
# MAGIC - Apply business rules and constraints
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Introduction to Medallion Architecture
# MAGIC The Medallion Architecture organizes data into three layers:
# MAGIC ### 🥉 Bronze Layer (Raw)
# MAGIC - **Purpose**: Exact copy of source data
# MAGIC - **Quality**: May contain duplicates, nulls, invalid data
# MAGIC - **Schema**: Raw, unmodified
# MAGIC - **Use case**: Audit trail, reprocessing
# MAGIC ### 🥈 Silver Layer (Cleaned)
# MAGIC - **Purpose**: Validated, cleansed, conformed data
# MAGIC - **Quality**: Deduplicated, standardized, validated
# MAGIC - **Schema**: Consistent, business-friendly
# MAGIC - **Use case**: Analytics, ML feature engineering
# MAGIC ### 🥇 Gold Layer (Business)
# MAGIC - **Purpose**: Aggregated, business-level metrics
# MAGIC - **Quality**: Highly curated, denormalized
# MAGIC - **Schema**: Optimized for query performance
# MAGIC - **Use case**: BI reports, dashboards, KPIs
# MAGIC
# MAGIC **This notebook**: Bronze → Silver transformations

# COMMAND ----------

# MAGIC %md
# MAGIC Import shared variables and configuration

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Set current catalog and schema to Silver
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

print(f"Current Catalog: {spark.catalog.currentCatalog()}")
print(f"Current Schema: {spark.catalog.currentDatabase()}")

# COMMAND ----------

# Import required functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Clean Customer Data
# MAGIC Customer data typically requires:
# MAGIC - **Deduplication**: Remove duplicate customer records
# MAGIC - **Standardization**: Consistent formats for email, phone, names
# MAGIC - **Validation**: Valid email patterns, phone numbers
# MAGIC - **Null handling**: Business rules for required fields
# MAGIC
# MAGIC Let's start by examining the bronze data.

# COMMAND ----------

# Read bronze customer data
customers_bronze = spark.table(CUSTOMERS_BRONZE_TABLE)

print(f"Bronze customer count: {customers_bronze.count():,}")
customers_bronze.printSchema()
display(customers_bronze.limit(10))

# COMMAND ----------

# Data quality assessment
customers_bronze.select(
    F.count("*").alias("total_records"),
    F.count("customer_id").alias("non_null_ids"),
    F.countDistinct("customer_id").alias("unique_ids"),
    F.count("email").alias("non_null_emails"),
    F.count("phone").alias("non_null_phones"),
    F.sum(F.when(F.col("email").isNull() | F.col("phone").isNull(), 1).otherwise(0)).alias("missing_contact")
).show()

# COMMAND ----------

# Check for duplicate customer_ids
duplicate_check = customers_bronze.groupBy("customer_id").count().filter("count > 1")
duplicate_count = duplicate_check.count()

print(f"Duplicate customer IDs: {duplicate_count}")
if duplicate_count > 0:
    display(duplicate_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplication Strategy
# MAGIC When multiple records exist for the same `customer_id`, we need a tiebreaker:
# MAGIC | Strategy | Approach |
# MAGIC |----------|----------|
# MAGIC | **Most recent** | Keep row with latest timestamp |
# MAGIC | **Most complete** | Keep row with fewest nulls |
# MAGIC | **Explicit priority** | Use a priority column |
# MAGIC
# MAGIC **Common pattern**: Use window functions with `row_number()`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 1: Deduplicate Customer Records
# MAGIC **Your task**: Remove duplicate `customer_id` records using a window function.
# MAGIC **Requirements:**
# MAGIC - Create a window partitioned by `customer_id`
# MAGIC - Order by `customer_id` descending (arbitrary tiebreaker)
# MAGIC - Use `row_number()` to assign numbers
# MAGIC - Keep only rows where `row_num == 1`
# MAGIC - Drop the helper column
# MAGIC - Store result in `customers_deduped`
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC window_spec = Window.partitionBy("col").orderBy(F.col("col").desc())
# MAGIC df.withColumn("row_num", F.row_number().over(window_spec))
# MAGIC ```
# MAGIC **Hint**: Import `Window` from `pyspark.sql.window` (already done above).

# COMMAND ----------

# TODO: Deduplicate customers using window function

# Create window spec
window_spec = Window.partitionBy(     ).orderBy(

# Apply row_number and filter
customers_deduped = customers_bronze \




print(f"After deduplication: {customers_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Deduplicate Customers

# window_spec = Window.partitionBy("customer_id").orderBy(F.col("customer_id").desc())

# customers_deduped = customers_bronze \
#     .withColumn("row_num", F.row_number().over(window_spec)) \
#     .filter(F.col("row_num") == 1) \
#     .drop("row_num")

# print(f"✅ After deduplication: {customers_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Standardize and Validate Customer Data
# MAGIC **Your task**: Clean and add validation flags to customer data.
# MAGIC **Part 1 - Standardization:**
# MAGIC - Email: lowercase and trim
# MAGIC - Phone: remove non-numeric characters
# MAGIC - Names: proper case (initcap) and trim
# MAGIC - Add `processed_at` timestamp
# MAGIC **Part 2 - Validation Flags:**
# MAGIC - `is_valid_email`: Check regex pattern
# MAGIC - `is_valid_phone`: Check length == 10
# MAGIC - `data_quality_score`: Count of valid fields (0-4)
# MAGIC
# MAGIC **Functions you'll need:**
# MAGIC ```python
# MAGIC F.lower(), F.trim(), F.initcap()
# MAGIC F.regexp_replace(col, "[^0-9]", "")  # Keep only digits
# MAGIC F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# MAGIC F.current_timestamp()
# MAGIC ```
# MAGIC **Hint**: Build in steps - standardize first, then add flags.

# COMMAND ----------

# TODO: Clean and standardize customer data

customers_clean = customers_deduped.select(
    F.col("customer_id"),
    # TODO: Apply initcap and trim to first_name
    
    # TODO: Apply initcap and trim to last_name
    
    # TODO: Lowercase and trim email
    
    # TODO: Remove non-digits from phone
    
    F.col("location"),
    F.col("loyalty_tier"),
    F.col("registration_date").alias("account_created_date"),
    # TODO: Add processed_at timestamp
    
)

# TODO: Add validation flags
customers_clean = customers_clean.withColumn(
    "is_valid_email",
    # TODO: Add email regex validation
    
).withColumn(
    "is_valid_phone",
    # TODO: Check phone length == 10
    
).withColumn(
    "data_quality_score",
    # TODO: Sum up quality indicators (email not null + phone not null + valid email + valid phone)
    
    
    
    
)

display(customers_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Standardize and Validate

# customers_clean = customers_deduped.select(
#     F.col("customer_id"),
#     F.trim(F.initcap(F.col("first_name"))).alias("first_name"),
#     F.trim(F.initcap(F.col("last_name"))).alias("last_name"),
#     F.lower(F.trim(F.col("email"))).alias("email"),
#     F.regexp_replace(F.col("phone"), "[^0-9]", "").alias("phone"),
#     F.col("location"),
#     F.col("loyalty_tier"),
#     F.col("registration_date").alias("account_created_date"),
#     F.current_timestamp().alias("processed_at")
# )

# customers_clean = customers_clean.withColumn(
#     "is_valid_email",
#     F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# ).withColumn(
#     "is_valid_phone",
#     F.length(F.col("phone")) == 10
# ).withColumn(
#     "data_quality_score",
#     (
#         F.when(F.col("email").isNotNull(), 1).otherwise(0) +
#         F.when(F.col("phone").isNotNull(), 1).otherwise(0) +
#         F.when(F.col("is_valid_email"), 1).otherwise(0) +
#         F.when(F.col("is_valid_phone"), 1).otherwise(0)
#     )
# )

# display(customers_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Business Rules and Filtering
# MAGIC Now apply business rules to filter out low-quality records.

# COMMAND ----------

# Filter out records that don't meet minimum quality standards
# Business rule: Must have valid customer_id and at least one valid contact method
customers_validated = customers_clean.filter(
    (F.col("customer_id").isNotNull()) &
    ((F.col("is_valid_email")) | (F.col("is_valid_phone")))
)

print(f"Records passing validation: {customers_validated.count():,}")
print(f"Records filtered out: {customers_clean.count() - customers_validated.count():,}")

# COMMAND ----------

# Write cleaned customer data to Silver table
customers_validated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(CUSTOMERS_SILVER_TABLE)

print(f"✅ Created Silver table: {CUSTOMERS_SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Clean Product Data
# MAGIC Product data requires similar treatment:
# MAGIC - **Deduplication**: Remove duplicate products
# MAGIC - **Format fixes**: Standardize names, categories, SKUs
# MAGIC - **Price validation**: Ensure positive, reasonable prices
# MAGIC - **Calculated fields**: Profit margins
# MAGIC
# MAGIC Let's examine the data first.

# COMMAND ----------

# Read bronze product data
products_bronze = spark.table(PRODUCTS_BRONZE_TABLE)

print(f"Bronze product count: {products_bronze.count():,}")
products_bronze.printSchema()
display(products_bronze.limit(10))

# COMMAND ----------

# Assess product data quality
products_bronze.select(
    F.count("*").alias("total_records"),
    F.countDistinct("product_id").alias("unique_products"),
    F.count("price").alias("non_null_prices"),
    F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("invalid_prices"),
    F.count("category").alias("non_null_categories")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 3: Clean and Validate Product Data
# MAGIC **Your task**: Apply the same cleaning pattern to products.
# MAGIC **Part 1 - Deduplication:**
# MAGIC - Window partitioned by `product_id`
# MAGIC - Keep first row per product
# MAGIC **Part 2 - Standardization:**
# MAGIC - Trim product_name, category, subcategory
# MAGIC - Cast price and cost to `decimal(10,2)`
# MAGIC - Add `processed_at`
# MAGIC **Part 3 - Validation:**
# MAGIC - `is_valid_price`: price > 0 AND price < 100000
# MAGIC - `is_valid_cost`: cost > 0 AND cost <= price
# MAGIC - `profit_margin`: ((price - cost) / price) * 100
# MAGIC **Part 4 - Filter:**
# MAGIC - Must have product_id, product_name, and valid price
# MAGIC
# MAGIC **Hint**: Follow the customer cleaning pattern.

# COMMAND ----------

# TODO: Deduplicate products

window_spec = Window.partitionBy(

products_deduped = products_bronze \




print(f"After deduplication: {products_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Deduplicate Products

# window_spec = Window.partitionBy("product_id").orderBy(F.col("product_id").desc())

# products_deduped = products_bronze \
#     .withColumn("row_num", F.row_number().over(window_spec)) \
#     .filter(F.col("row_num") == 1) \
#     .drop("row_num")

# print(f"✅ After deduplication: {products_deduped.count():,}")

# COMMAND ----------

# TODO: Clean, standardize, and validate products

products_clean = products_deduped.select(
    F.col("product_id"),
    # TODO: Trim product_name
    
    # TODO: Trim category
    
    # TODO: Trim subcategory
    
    # TODO: Cast price to decimal(10,2)
    
    # TODO: Cast cost to decimal(10,2)
        
    # TODO: Add processed_at
    
)

# TODO: Add validation flags
products_clean = products_clean.withColumn(
    "is_valid_price",
    # TODO: price > 0 AND price < 100000
    
).withColumn(
    "is_valid_cost",
    # TODO: cost > 0 AND cost <= price
    
).withColumn(
    "profit_margin",
    # TODO: Calculate (price - cost) / price * 100, handle nulls
    
    
)

display(products_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Clean and Validate Products

# products_clean = products_deduped.select(
#     F.col("product_id"),
#     F.trim(F.col("product_name")).alias("product_name"),
#     F.trim(F.col("category")).alias("category"),
#     F.trim(F.col("subcategory")).alias("subcategory"),
#     F.col("price").cast("decimal(10,2)").alias("price"),
#     F.col("cost").cast("decimal(10,2)").alias("cost"),
#     F.current_timestamp().alias("processed_at")
# )

# products_clean = products_clean.withColumn(
#     "is_valid_price",
#     (F.col("price") > 0) & (F.col("price") < 100000)
# ).withColumn(
#     "is_valid_cost",
#     (F.col("cost") > 0) & (F.col("cost") <= F.col("price"))
# ).withColumn(
#     "profit_margin",
#     F.when(
#         (F.col("price") > 0) & (F.col("cost").isNotNull()),
#         ((F.col("price") - F.col("cost")) / F.col("price") * 100)
#     ).otherwise(None)
# )

# display(products_clean.limit(10))

# COMMAND ----------

# Filter products meeting quality standards
products_validated = products_clean.filter(
    (F.col("product_id").isNotNull()) &
    (F.col("product_name").isNotNull()) &
    (F.col("is_valid_price"))
)

print(f"Products passing validation: {products_validated.count():,}")

# COMMAND ----------

# Write to Silver table
products_validated.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(PRODUCTS_SILVER_TABLE)

print(f"✅ Created Silver table: {PRODUCTS_SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Process Sales with Streaming Joins
# MAGIC Sales transactions are high-volume and require:
# MAGIC - **Streaming processing**: Handle continuous data flow
# MAGIC - **Join enrichment**: Add customer and product details
# MAGIC - **Calculated fields**: Subtotals, validation flags
# MAGIC - **Stream-static joins**: Join streaming sales with static dimensions
# MAGIC ### Stream-Static Joins
# MAGIC **Pattern**: Stream (sales) joins static (customers, products)
# MAGIC ```python
# MAGIC streaming_df.join(static_df, "key", "left")
# MAGIC ```
# MAGIC **Important**: Dimension tables must be small enough to fit in memory.

# COMMAND ----------

# Read bronze sales as a stream
sales_bronze_stream = spark.readStream \
    .format("delta") \
    .table(SALES_BRONZE_TABLE)

sales_bronze_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
from pyspark.sql.types import IntegerType, DecimalType

orders_flat = sales_bronze_stream.withColumn("line_item", explode("line_items")) \
    .select(
        col("order_id"),
        col("customer_id"),
        col("order_status"),
        col("order_timestamp"),
        col("payment_method"),
        col("shipping_address"),
        col("line_item.product_id").alias("product_id"),
        col("line_item.quantity").cast(IntegerType()).alias("quantity"),
        col("line_item.unit_price").cast(DecimalType(10,2)).alias("unit_price"),
        col("_rescued_data")
    )

# COMMAND ----------

# Load dimension tables (static)
customers_dim = spark.table(CUSTOMERS_SILVER_TABLE)
products_dim = spark.table(PRODUCTS_SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 4: Enrich Sales with Dimensions
# MAGIC **Your task**: Join streaming sales with customer and product dimensions.
# MAGIC **Requirements:**
# MAGIC 1. Join sales with customers (left join on `customer_id`)
# MAGIC - Select: customer_id, first_name, last_name, loyalty_tier
# MAGIC 2. Join result with products (left join on `product_id`)
# MAGIC - Select: product_id, product_name, category, subcategory, price
# MAGIC 3. Create final select with:
# MAGIC - All key fields
# MAGIC   - `customer_name`: Concatenate first + last name
# MAGIC   - `subtotal`: quantity * unit_price
# MAGIC - Other relevant fields
# MAGIC   - `is_valid_sale`: Check all required fields exist
# MAGIC
# MAGIC **Hint**: Chain the joins, then select columns.

# COMMAND ----------

# TODO: Join sales with dimensions and transform

# Join with customers
sales_enriched = sales_bronze_stream \
    .join(
        # TODO: Select customer fields and join
        
        
    ) \
    .join(
        # TODO: Select product fields and join
        
        
    )

# TODO: Select and transform fields
sales_clean = sales_enriched.select(
    F.col("sale_id"),
    F.col("customer_id"),
    # TODO: Concatenate customer name
    
    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("subcategory"),
    F.col("quantity"),
    F.col("unit_price"),
    # TODO: Calculate subtotal
    
    F.col("payment_method"),
    F.col("order_timestamp"),
    F.col("loyalty_tier"),
    F.current_timestamp().alias("processed_at")
)

# TODO: Add validation flag
sales_clean = sales_clean.withColumn(
    "is_valid_sale",
    # TODO: Check required fields
    
    
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: Enrich Sales with Joins

# sales_enriched = orders_flat \
#     .join(
#         customers_dim.select("customer_id", "first_name", "last_name", "loyalty_tier"),
#         "customer_id",
#         "left"
#     ) \
#     .join(
#         products_dim.select("product_id", "product_name", "category", "subcategory", "price"),
#         "product_id",
#         "left"
#     )

# sales_clean = sales_enriched.select(
#     F.col("order_id"),
#     F.col("customer_id"),
#     F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("customer_name"),
#     F.col("product_id"),
#     F.col("product_name"),
#     F.col("category"),
#     F.col("subcategory"),
#     F.col("quantity"),
#     F.col("unit_price"),
#     (F.col("quantity") * F.col("unit_price")).alias("total_amount"),
#     F.col("payment_method"),
#     F.col("order_timestamp"),
#     F.col("loyalty_tier"),
#     F.current_timestamp().alias("processed_at")
# )

# sales_clean = sales_clean.withColumn(
#     "is_valid_sale",
#     (F.col("customer_id").isNotNull()) &
#     (F.col("product_id").isNotNull()) &
#     (F.col("quantity") > 0) &
#     (F.col("total_amount") > 0)
# )

# Production best practice: Add watermark for late-arriving data
# sales_clean = sales_clean.withWatermark("sale_timestamp", "1 hour")
# This allows data up to 1 hour late to be processed correctly

# COMMAND ----------

# Write streaming sales to Silver table
sales_query = sales_clean \
    .filter(F.col("is_valid_sale")) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", SALES_SILVER_CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .table(SALES_SILVER_TABLE)

sales_query.awaitTermination()

print(f"✅ Sales data processed to: {SALES_SILVER_TABLE}")

# COMMAND ----------

# Verify sales Silver data
sales_count = spark.table(SALES_SILVER_TABLE).count()
print(f"Total sales in Silver: {sales_count:,}")

display(spark.table(SALES_SILVER_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Implement SCD Type 2
# MAGIC **Slowly Changing Dimension (SCD) Type 2** tracks historical changes.
# MAGIC ### How SCD Type 2 Works
# MAGIC When a dimension record changes:
# MAGIC 1. **Close** the existing record (set `effective_end_date`, `is_current = false`)
# MAGIC 2. **Insert** a new record with updated values
# MAGIC 3. New record has `effective_start_date = today`, `is_current = true`
# MAGIC ### SCD Type 2 Columns
# MAGIC | Column | Purpose |
# MAGIC |--------|--------|
# MAGIC | `effective_start_date` | When this version became active |
# MAGIC | `effective_end_date` | When superseded (NULL = current) |
# MAGIC | `is_current` | Boolean flag for current record |
# MAGIC | `version` | Version number (optional) |
# MAGIC ### Example
# MAGIC Customer changes loyalty tier from Bronze → Silver:
# MAGIC **Before:**
# MAGIC ```
# MAGIC customer_id | tier   | start_date | end_date | is_current
# MAGIC C001       | Bronze | 2024-01-01 | NULL     | true
# MAGIC ```
# MAGIC **After:**
# MAGIC ```
# MAGIC customer_id | tier   | start_date | end_date   | is_current
# MAGIC C001       | Bronze | 2024-01-01 | 2024-06-01 | false
# MAGIC C001       | Silver | 2024-06-01 | NULL       | true
# MAGIC ```

# COMMAND ----------

# Check if SCD2 table exists, if not create it
try:
    scd2_table = spark.table(CUSTOMERS_SCD2_TABLE)
    print(f"SCD2 table exists with {scd2_table.count():,} records")
except:
    print("Creating initial SCD2 table...")
    
    # Initialize with current customer data
    initial_scd2 = spark.table(CUSTOMERS_SILVER_TABLE).select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("phone"),
        F.col("location"),
        F.col("loyalty_tier"),
        F.col("account_created_date").cast("date"),
        F.col("account_created_date").cast("date").alias("effective_start_date"),
        F.lit(None).cast("date").alias("effective_end_date"),
        F.lit(True).alias("is_current"),
        F.lit(1).alias("version")
    )
    
    initial_scd2.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(CUSTOMERS_SCD2_TABLE)
    
    print(f"✅ Created {CUSTOMERS_SCD2_TABLE}")

# COMMAND ----------

# Display current SCD2 table
display(spark.table(CUSTOMERS_SCD2_TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Changes
# MAGIC Let's simulate some loyalty tier changes to demonstrate SCD Type 2.

# COMMAND ----------

# Simulate loyalty tier upgrades
updates = spark.table(CUSTOMERS_SILVER_TABLE).limit(5).withColumn(
    "loyalty_tier",
    F.when(F.col("loyalty_tier") == "Bronze", "Silver")
     .when(F.col("loyalty_tier") == "Silver", "Gold")
     .otherwise(F.col("loyalty_tier"))
)

print("Simulated updates:")
display(updates)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 5: Implement SCD Type 2 MERGE
# MAGIC **Your task**: Update the SCD2 table using Delta's MERGE operation.
# MAGIC **Two-step process:**
# MAGIC
# MAGIC **Step 1**: Close existing current records that changed
# MAGIC - Match on `customer_id` and `is_current = true`
# MAGIC - Check if `loyalty_tier` changed
# MAGIC - Update: `effective_end_date = current_date()`, `is_current = false`
# MAGIC
# MAGIC **Step 2**: Insert new versions
# MAGIC - Find which customers changed (join with closed records)
# MAGIC - Append new rows with updated data
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC target_table.alias("target").merge(
# MAGIC updates.alias("updates"),
# MAGIC "target.customer_id = updates.customer_id AND target.is_current = true"
# MAGIC ).whenMatchedUpdate(
# MAGIC condition="target.loyalty_tier != updates.loyalty_tier",
# MAGIC set={...}
# MAGIC ).execute()
# MAGIC ```
# MAGIC **Hint**: Use `DeltaTable.forName()` to get the target table.

# COMMAND ----------

# TODO: Implement SCD Type 2 MERGE

# Get target table
target_table = DeltaTable.forName(spark, CUSTOMERS_SCD2_TABLE)

# Prepare updates with SCD2 columns
updates_prepared = updates.select(
    F.col("customer_id"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("email"),
    F.col("phone"),
    F.col("location"),
    F.col("loyalty_tier"),
    F.col("account_created_date").cast("date"),
    F.current_date().alias("effective_start_date"),
    F.lit(None).cast("date").alias("effective_end_date"),
    F.lit(True).alias("is_current"),
    F.lit(2).alias("version")
)

# TODO: Step 1 - Close existing records that changed
target_table.alias("target").merge(
    # TODO: Fill in merge logic
    
    
).whenMatchedUpdate(
    # TODO: Fill in update condition and set
    
    
).execute()

print("Closed changed records")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Solution below** ⬇️

# COMMAND ----------

# ✅ SOLUTION: SCD Type 2 MERGE (Step 1)

# target_table = DeltaTable.forName(spark, CUSTOMERS_SCD2_TABLE)

# updates_prepared = updates.select(
#     F.col("customer_id"),
#     F.col("first_name"),
#     F.col("last_name"),
#     F.col("email"),
#     F.col("phone"),
#     F.col("location"),
#     F.col("loyalty_tier"),
#     F.col("account_created_date").cast("date"),
#     F.current_date().alias("effective_start_date"),
#     F.lit(None).cast("date").alias("effective_end_date"),
#     F.lit(True).alias("is_current"),
#     F.lit(2).alias("version")
# )

# target_table.alias("target").merge(
#     updates_prepared.alias("updates"),
#     "target.customer_id = updates.customer_id AND target.is_current = true"
# ).whenMatchedUpdate(
#     condition="target.loyalty_tier != updates.loyalty_tier",
#     set={
#         "effective_end_date": F.current_date(),
#         "is_current": "false"
#     }
# ).execute()

# print("✅ Closed changed records")

# COMMAND ----------

# Step 2: Insert new versions for changed records
changed_customers = updates_prepared.alias("updates").join(
    spark.table(CUSTOMERS_SCD2_TABLE)
        .filter(F.col("is_current") == False)
        .filter(F.col("effective_end_date") == F.current_date())
        .select("customer_id")
        .alias("changed"),
    "customer_id",
    "inner"
)

if changed_customers.count() > 0:
    changed_customers.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(CUSTOMERS_SCD2_TABLE)
    
    print(f"✅ Inserted {changed_customers.count()} new versions")
else:
    print("No changes detected")

# COMMAND ----------

# Verify SCD2 changes - show customers with multiple versions
display(
    spark.table(CUSTOMERS_SCD2_TABLE)
        .groupBy("customer_id")
        .agg(F.count("*").alias("version_count"))
        .filter("version_count > 1")
        .join(
            spark.table(CUSTOMERS_SCD2_TABLE),
            "customer_id"
        )
        .orderBy("customer_id", "effective_start_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Data Quality Monitoring
# MAGIC Production pipelines need continuous quality monitoring.

# COMMAND ----------

# Customer quality metrics
customer_quality = spark.table(CUSTOMERS_SILVER_TABLE).select(
    F.lit("Customers").alias("entity"),
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("is_valid_email"), 1).otherwise(0)).alias("valid_emails"),
    F.sum(F.when(F.col("is_valid_phone"), 1).otherwise(0)).alias("valid_phones"),
    F.avg("data_quality_score").alias("avg_quality_score")
)

display(customer_quality)

# COMMAND ----------

# Product quality metrics
product_quality = spark.table(PRODUCTS_SILVER_TABLE).select(
    F.lit("Products").alias("entity"),
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("is_valid_price"), 1).otherwise(0)).alias("valid_prices"),
    F.sum(F.when(F.col("is_valid_cost"), 1).otherwise(0)).alias("valid_costs"),
    F.avg("profit_margin").alias("avg_profit_margin")
)

display(product_quality)

# COMMAND ----------

# Combined quality dashboard
display(spark.sql(f"""
    SELECT 
        'Bronze → Silver Pipeline' as pipeline,
        current_timestamp() as check_time,
        'customers' as table_name,
        COUNT(*) as record_count,
        SUM(CASE WHEN is_valid_email THEN 1 ELSE 0 END) as quality_checks_passed
    FROM {CUSTOMERS_SILVER_TABLE}
    
    UNION ALL
    
    SELECT 
        'Bronze → Silver Pipeline',
        current_timestamp(),
        'products',
        COUNT(*),
        SUM(CASE WHEN is_valid_price THEN 1 ELSE 0 END)
    FROM {PRODUCTS_SILVER_TABLE}
    
    UNION ALL
    
    SELECT 
        'Bronze → Silver Pipeline',
        current_timestamp(),
        'sales',
        COUNT(*),
        SUM(CASE WHEN is_valid_sale THEN 1 ELSE 0 END)
    FROM {SALES_SILVER_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Summary and Checkpoint
# MAGIC ### 🎯 Key Concepts Covered
# MAGIC **1. Medallion Architecture**
# MAGIC - Bronze: Raw ingestion with no transformations
# MAGIC - Silver: Cleaned, validated, conformed data
# MAGIC - Gold: Business-level aggregations (next notebook)
# MAGIC
# MAGIC **2. Data Cleaning Techniques**
# MAGIC - Deduplication with window functions
# MAGIC - Standardization (case, trim, format)
# MAGIC - Validation (regex, ranges, business rules)
# MAGIC - Quality scoring
# MAGIC
# MAGIC **3. Stream Processing**
# MAGIC - Stream-static joins
# MAGIC - Enrichment with dimension tables
# MAGIC - Calculated fields
# MAGIC - Checkpointing for streaming writes
# MAGIC
# MAGIC **4. SCD Type 2**
# MAGIC - Effective date ranges
# MAGIC - Current record flags
# MAGIC - MERGE pattern for updates
# MAGIC - Historical tracking
# MAGIC
# MAGIC **5. Data Quality**
# MAGIC - Validation flags
# MAGIC - Quality metrics
# MAGIC - Monitoring dashboards
# MAGIC
# MAGIC ### ✅ Exam Checklist
# MAGIC Can you:
# MAGIC - [ ] Implement deduplication with window functions?
# MAGIC - [ ] Apply standardization transformations?
# MAGIC - [ ] Join streaming and static dataframes?
# MAGIC - [ ] Write MERGE for SCD Type 2?
# MAGIC - [ ] Create validation logic?
# MAGIC - [ ] Explain Bronze/Silver/Gold layers?
# MAGIC ### 📚 Next Steps
# MAGIC **Notebook 04** covers:
# MAGIC - Silver to Gold transformations
# MAGIC - Window functions for analytics
# MAGIC - Customer lifetime value
# MAGIC - Business-level aggregations
# MAGIC ---
# MAGIC **🎉 Notebook Complete!**
# MAGIC The Silver layer is populated with clean, validated data ready for business analytics. Proceed to Notebook 04 for Gold layer transformations.
