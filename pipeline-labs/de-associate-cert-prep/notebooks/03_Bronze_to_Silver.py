# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Bronze to Silver Transformations
# MAGIC
# MAGIC **Exam Coverage**: Section 3 (Data Processing & Transformations)
# MAGIC
# MAGIC **Duration**: about 1 hour
# MAGIC
# MAGIC ---
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Implement data quality transformations (deduplication, standardization)
# MAGIC - Validate and cleanse customer and product data
# MAGIC - Process streaming sales with joins and enrichment
# MAGIC - Implement Slowly Changing Dimension (SCD) Type 2
# MAGIC - Build data quality metrics queries
# MAGIC - Apply business-rule filters
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
# MAGIC At CartHarborHQ the loyalty team segments customers and merchandising reports revenue by category: both read the Silver tables you build here, so a duplicate or a misspelled category in Silver shows up in their numbers.
# MAGIC
# MAGIC Customer data typically requires:
# MAGIC - **Deduplication**: Remove duplicate customer records
# MAGIC - **Standardization**: Consistent formats for email, phone, names
# MAGIC - **Validation**: Valid email patterns, phone numbers
# MAGIC - **Null handling**: Business rules for required fields

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
# MAGIC The check above counts customer ids, not rows: the 200 extra rows fall on 197 customers, because a few customers were re-sent twice.
# MAGIC
# MAGIC When multiple records exist for the same `customer_id`, we need a tiebreaker:
# MAGIC
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
# MAGIC
# MAGIC **Why**: One duplicated customer doubles that customer's revenue in every Gold table that joins to it.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - A window partitioned by `customer_id`, ordered by `registration_date` ascending: the account was created at the first registration; the planted duplicates are later re-sends of the same record, identical apart from the date
# MAGIC - Number the rows with `row_number()`, keep row 1, and drop the helper column
# MAGIC - Store the result in `customers_deduped`
# MAGIC
# MAGIC **Pattern:**
# MAGIC ```python
# MAGIC window_spec = Window.partitionBy("col").orderBy(F.col("col").asc())  # .desc() puts the newest first
# MAGIC df.withColumn("row_num", F.row_number().over(window_spec))
# MAGIC ```
# MAGIC **Hint**: Import `Window` from `pyspark.sql.window` (already done above).

# COMMAND ----------

# ✏️ STUDENT EXERCISE
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

# # The first registration wins: the account was created then; later copies are re-sends of the same record
# window_spec = Window.partitionBy("customer_id").orderBy(F.col("registration_date").asc())

# customers_deduped = customers_bronze \
#     .withColumn("row_num", F.row_number().over(window_spec)) \
#     .filter(F.col("row_num") == 1) \
#     .drop("row_num")

# print(f"✅ After deduplication: {customers_deduped.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see **10,000** rows and **10,000** distinct `customer_id`s (Bronze had 10,200 rows). If you still see 10,200, your filter on the row number did not run.

# COMMAND ----------

# 🔍 Verify the deduplication
print(f"Rows: {customers_deduped.count():,} (expect 10,000)")
print(f"Distinct customer_id: {customers_deduped.select('customer_id').distinct().count():,} (expect 10,000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 2: Standardize and Validate Customer Data
# MAGIC **Your task**: Clean and add validation flags to customer data.
# MAGIC
# MAGIC **Why**: the source sends names as `  John  `, `jOHN` or `JOHN` and every phone as `+1-XXX-XXX-XXXX`; standardize them and every report shows one spelling, and the flags let you filter on purpose instead of losing rows silently.
# MAGIC
# MAGIC **Part 1 - Standardization:**
# MAGIC - Email: lowercase and trim
# MAGIC - Phone: remove non-numeric characters, then the leading US country code 1 (the source sends `+1-XXX-XXX-XXXX`; US area codes never start with 1)
# MAGIC - Names: proper case (initcap) and trim
# MAGIC - Add `processed_at` timestamp
# MAGIC
# MAGIC **Part 2 - Validation Flags:**
# MAGIC - `is_valid_email`: matches an email pattern (name, `@`, domain, dot, at least 2 letters)
# MAGIC - `is_valid_phone`: exactly 10 digits remain
# MAGIC - `data_quality_score`: 1 point each for email present, phone present, valid email, valid phone (0-4)
# MAGIC
# MAGIC **Hint**: one regex-replace for the phone, one regex match for the email; `pyspark.sql.functions` has both. Build in steps: standardize first, then add flags.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Clean and standardize customer data

customers_clean = customers_deduped.select(
    F.col("customer_id"),
    # TODO: Apply initcap and trim to first_name
    
    # TODO: Apply initcap and trim to last_name
    
    # TODO: Lowercase and trim email
    
    # TODO: Remove non-digits and the leading country code 1 from phone

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
    # TODO: Check exactly 10 digits remain

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
#     F.regexp_replace(F.regexp_replace(F.col("phone"), "[^0-9]", ""), "^1", "").alias("phone"),
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
# MAGIC The cell below applies the business rule: keep a customer with a non-null `customer_id` and a valid email OR a valid phone.
# MAGIC
# MAGIC ### 🔍 Verify Your Results
# MAGIC Every generated phone is valid once the country code is removed, so a customer fails only when the email lacks its `@` AND the phone is missing (20% of phones are null). That leaves **9,986** customers passing validation and **14** filtered out. If about 100 are filtered out, your phone cleaning still leaves 11 digits and no phone counts as valid.

# COMMAND ----------

# 🔍 Verify: filter out records that don't meet minimum quality standards
# Business rule: Must have valid customer_id and at least one valid contact method
customers_validated = customers_clean.filter(
    (F.col("customer_id").isNotNull()) &
    ((F.col("is_valid_email")) | (F.col("is_valid_phone")))
)

n_valid = customers_validated.count()
n_out = customers_clean.count() - n_valid
print(f"Records passing validation: {n_valid:,} (expect 9,986)")
print(f"Records filtered out: {n_out:,} (expect 14)")

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
# MAGIC - **Format fixes**: Trim names, trim and proper-case categories
# MAGIC - **Price validation**: Ensure positive, reasonable prices
# MAGIC - **Calculated fields**: Profit margins

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
# MAGIC
# MAGIC **Why**: a negative price makes `profit_margin` meaningless, so the Part 4 filter on `is_valid_price` keeps the product out of the `products_clean` Silver table. Exercise 4 then drops every sales line whose product is missing from that table (read there as `products_dim`), so each product this filter removes also drops out of category revenue in Gold: know how many it removes before you trust the dashboard.
# MAGIC
# MAGIC **Part 1 - Deduplication:**
# MAGIC - Window partitioned by `product_id`, ordered by `last_updated` descending; keep row 1 (the latest catalog snapshot)
# MAGIC
# MAGIC **Part 2 - Standardization:**
# MAGIC - Trim product_name and subcategory
# MAGIC - Trim AND proper-case category (`initcap`): 15% of rows arrive as `eLECTRONICS` or `ELECTRONICS`, and every Gold KPI groups by category
# MAGIC - Cast price and cost to `decimal(10,2)`
# MAGIC - Add `processed_at`
# MAGIC
# MAGIC **Part 3 - Validation:**
# MAGIC - `is_valid_price`: price > 0 AND price < 100000
# MAGIC - `is_valid_cost`: cost > 0 AND cost <= price
# MAGIC - `profit_margin`: ((price - cost) / price) * 100
# MAGIC
# MAGIC Part 4 (filter: product_id, product_name and a valid price) is provided in the cell after the solution: run it and read it.
# MAGIC
# MAGIC **Hint**: Follow the customer cleaning pattern.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
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

# # The latest catalog snapshot wins: the duplicates are older snapshots of the same product
# window_spec = Window.partitionBy("product_id").orderBy(F.col("last_updated").desc())

# products_deduped = products_bronze \
#     .withColumn("row_num", F.row_number().over(window_spec)) \
#     .filter(F.col("row_num") == 1) \
#     .drop("row_num")

# print(f"✅ After deduplication: {products_deduped.count():,}")

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Clean, standardize, and validate products

products_clean = products_deduped.select(
    F.col("product_id"),
    # TODO: Trim product_name

    # TODO: Trim and proper-case category

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
#     F.initcap(F.trim(F.col("category"))).alias("category"),
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

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below (Part 4, provided). You should see:
# MAGIC - **1,000** products after deduplication (Bronze had 1,010).
# MAGIC - Exactly **5** distinct categories: Books, Clothing, Electronics, Home & Garden, Sports & Outdoors. More than 5 means the category case is not fixed yet.
# MAGIC - **988** products passing validation: the ~1% with a negative price fail `is_valid_price`.

# COMMAND ----------

# 🔍 Verify: filter products meeting quality standards (Part 4)
products_validated = products_clean.filter(
    (F.col("product_id").isNotNull()) &
    (F.col("product_name").isNotNull()) &
    (F.col("is_valid_price"))
)

categories = sorted(r.category for r in products_clean.select('category').distinct().collect())
print(f"Products after deduplication: {products_clean.count():,} (expect 1,000)")
print(f"Distinct categories: {len(categories)} (expect 5)")
print(f"Category names: {categories}")
print(f"Products passing validation: {products_validated.count():,} (expect 988)")

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
# MAGIC - **Calculated fields**: line amounts (`total_amount`), validation flags
# MAGIC - **Stream-static joins**: Join streaming sales with static dimensions
# MAGIC ### Stream-Static Joins
# MAGIC **Pattern**: Stream (sales) joins static (customers, products)
# MAGIC ```python
# MAGIC streaming_df.join(static_df, "key", "left")
# MAGIC ```
# MAGIC **Important**: each micro-batch joins against the latest version of the static Delta table, so a dimension update reaches the next micro-batch without a restart. The join keeps no streaming state, so it needs no watermark. Updates to the static table do not reprocess rows the stream already joined.

# COMMAND ----------

# Read bronze sales as a stream
sales_bronze_stream = spark.readStream \
    .format("delta") \
    .table(SALES_BRONZE_TABLE)

sales_bronze_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col
from pyspark.sql.types import IntegerType, DecimalType

# 1% of orders arrive twice (client retries, same order_id): drop them before exploding, or Gold counts their revenue twice.
# A stream that never stops would bound this dedup state with a watermark on a TIMESTAMP column (order_timestamp is a string here): .withColumn("order_ts", F.to_timestamp("order_timestamp")).withWatermark("order_ts", "1 hour").dropDuplicatesWithinWatermark(["order_id"]) (a watermark next to a plain dropDuplicates(["order_id"]) does not shrink the state)
orders_flat = sales_bronze_stream.dropDuplicates(["order_id"]).withColumn("line_item", explode("line_items")) \
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
# MAGIC **Your task**: Enrich the order lines in `orders_flat` (built above: one row per line item with `order_id`, `customer_id`, `order_status`, `product_id`, `quantity`, `unit_price`, `payment_method`, `order_timestamp`) with customer and product attributes.
# MAGIC
# MAGIC **Why**: notebook 04 reads `order_id`, `order_status` and `total_amount` by exactly these names; denormalizing here means every Gold query reads one table instead of re-joining three.
# MAGIC
# MAGIC **Requirements:**
# MAGIC 1. Start from `orders_flat`. Left-join it to `customers_dim` on `customer_id` (keep `first_name`, `last_name`, `loyalty_tier` and a match marker, see requirement 2), then to `products_dim` on `product_id` (keep `product_name`, `category`, `subcategory`).
# MAGIC 2. Select `order_id`, `customer_id`, `customer_name`, `product_id`, `product_name`, `category`, `subcategory`, `quantity`, `unit_price`, `total_amount`, `order_status`, `payment_method`, `order_timestamp`, `loyalty_tier`, `processed_at`.
# MAGIC
# MAGIC    `customer_name`: first + ' ' + last name, or `Unknown customer` when the customer is not in `customers_dim` (the sale still counts as revenue). Detect the miss with a marker that only the customer side of the join supplies: a constant true column named `in_dim`, selected alongside the customer columns, which the left join leaves null exactly when the customer is missing. Do not use the name columns: they can be null for a known customer.
# MAGIC
# MAGIC    `total_amount`: quantity x unit_price, the LINE amount, not the order total (notebook 04 sums this column).
# MAGIC
# MAGIC 3. Add `is_valid_sale`: true when `customer_id` and `product_id` are not null, `quantity` > 0, `total_amount` > 0, and the product exists in `products_dim` (`product_name` not null: a line with no category cannot be reported by category).
# MAGIC
# MAGIC **Hint**: Chain the joins, then select columns. A name built from two columns must survive one of them being null.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
# TODO: Join sales with dimensions and transform

# Join with customers
sales_enriched = orders_flat \
    .join(
        # TODO: Select customer fields and join
        
        
    ) \
    .join(
        # TODO: Select product fields and join
        
        
    )

# TODO: Select and transform fields
sales_clean = sales_enriched.select(
    F.col("order_id"),
    F.col("customer_id"),
    # TODO: customer_name (first + ' ' + last, or 'Unknown customer')

    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("subcategory"),
    F.col("quantity"),
    F.col("unit_price"),
    # TODO: Calculate total_amount (quantity * unit_price)

    F.col("order_status"),
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
#         customers_dim.select("customer_id", "first_name", "last_name", "loyalty_tier",
#                              F.lit(True).alias("in_dim")),
#         "customer_id",
#         "left"
#     ) \
#     .join(
#         products_dim.select("product_id", "product_name", "category", "subcategory"),
#         "product_id",
#         "left"
#     )

# sales_clean = sales_enriched.select(
#     F.col("order_id"),
#     F.col("customer_id"),
#     F.when(F.col("in_dim"), F.concat_ws(" ", "first_name", "last_name"))
#      .otherwise(F.lit("Unknown customer")).alias("customer_name"),
#     F.col("product_id"),
#     F.col("product_name"),
#     F.col("category"),
#     F.col("subcategory"),
#     F.col("quantity"),
#     F.col("unit_price"),
#     (F.col("quantity") * F.col("unit_price")).alias("total_amount"),
#     F.col("order_status"),
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
#     (F.col("total_amount") > 0) &
#     (F.col("product_name").isNotNull())
# )

# COMMAND ----------

# MAGIC %md
# MAGIC The cell below writes `sales_clean` through the checkpoint in `SALES_SILVER_CHECKPOINT_PATH`. To redo Exercise 4 after it has run, first drop the table and delete its checkpoint folder with `spark.sql(f"DROP TABLE IF EXISTS {SALES_SILVER_TABLE}")` and `dbutils.fs.rm(SALES_SILVER_CHECKPOINT_PATH, True)`: the checkpoint has already read every Bronze row, so running the write again without them adds nothing.

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

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see:
# MAGIC - **Grain**: one row per order line item, so there are more rows than distinct `order_id`s (an order has 1 to 5 lines).
# MAGIC - **Distinct orders**: Bronze **50,500** (the 50,000 generated orders plus notebook 02's 500 practice orders; Bronze holds 51,000 rows because the 500 client-retry copies share an order_id with their original), Silver a little lower. Each order reaches Silver once (as its line items), and an order whose every line fails `is_valid_sale` (a `quantity` <= 0, or a product that is not among the 988 in `products_dim`) has no line left to write.
# MAGIC - `Orders with more Silver lines than Bronze lines: 0 (expect 0)`: no order reached Silver twice. An order can list the same product on two lines, so a repeated `order_id` + `product_id` pair is not a duplicate.
# MAGIC - **No invalid lines**: 0 rows with a null `product_name` and 0 rows with `quantity` <= 0 (the `is_valid_sale` filter removed them).
# MAGIC - `Lines from 'Unknown customer' orders: 1,107`: lines whose customer is not in `customers_dim` (the Silver customers that passed validation). They still count as revenue.

# COMMAND ----------

# 🔍 Verify sales Silver data
sales_silver_check = spark.table(SALES_SILVER_TABLE)
sales_bronze_orders = spark.table(SALES_BRONZE_TABLE).dropDuplicates(["order_id"])
print(f"Total line items in Silver: {sales_silver_check.count():,}")
print(f"Distinct orders: Bronze {sales_bronze_orders.count():,}  Silver {sales_silver_check.select('order_id').distinct().count():,}")

# An order ingested twice would have more Silver lines than its one Bronze copy has line items
bronze_lines = sales_bronze_orders.select("order_id", F.size("line_items").alias("bronze_lines"))
silver_lines = sales_silver_check.groupBy("order_id").agg(F.count("*").alias("silver_lines"))
orders_over = silver_lines.join(bronze_lines, "order_id").filter(F.col("silver_lines") > F.col("bronze_lines")).count()
print(f"Orders with more Silver lines than Bronze lines: {orders_over} (expect 0)")
print(f"Rows with null product_name: {sales_silver_check.filter(F.col('product_name').isNull()).count()}")
print(f"Rows with quantity <= 0: {sales_silver_check.filter(F.col('quantity') <= 0).count()}")
print(f"Lines from 'Unknown customer' orders: {sales_silver_check.filter(F.col('customer_name') == 'Unknown customer').count():,}")

display(sales_silver_check.limit(10))

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
# MAGIC Five customers move up one loyalty tier (Bronze to Silver, Silver to Gold, Gold to Platinum); the cell below builds these updates.

# COMMAND ----------

# Simulate loyalty tier upgrades: the first 5 customers (by customer_id) who can move up a tier
updates = spark.table(CUSTOMERS_SILVER_TABLE) \
    .filter(F.col("loyalty_tier").isin("Bronze", "Silver", "Gold")) \
    .orderBy("customer_id") \
    .limit(5) \
    .withColumn(
        "loyalty_tier",
        F.when(F.col("loyalty_tier") == "Bronze", "Silver")
         .when(F.col("loyalty_tier") == "Silver", "Gold")
         .otherwise("Platinum")
    )

print("Simulated updates:")
display(updates)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🎯 EXERCISE 5: Implement SCD Type 2 MERGE
# MAGIC **Your task**: Update the SCD2 table using Delta's MERGE operation.
# MAGIC
# MAGIC **Why**: Overwriting the tier rewrites history: last quarter's revenue by tier re-buckets the day a customer upgrades. SCD2 keeps the tier that was true at order time.
# MAGIC
# MAGIC **Two-step process:**
# MAGIC
# MAGIC **Step 1** (your code): Close existing current records that changed
# MAGIC - Match each update to the target's current row for the same customer
# MAGIC - Act only when `loyalty_tier` changed
# MAGIC - Set `effective_end_date` to today and `is_current` to false
# MAGIC
# MAGIC **Step 2** (provided after the solution): Insert new versions
# MAGIC - A changed customer has no current row once Step 1 closed it; inserting only those makes this safe to re-run
# MAGIC - Append new rows with updated data
# MAGIC
# MAGIC **Hint**: match on the business key AND the current-row flag; close only the rows whose tracked attribute differs. The target table handle and the prepared updates are provided in the cell below.

# COMMAND ----------

# ✏️ STUDENT EXERCISE
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

# Step 2: a changed customer has no current row once Step 1 closed it; inserting only those makes this safe to re-run
changed_customers = updates_prepared.join(
    spark.table(CUSTOMERS_SCD2_TABLE).filter("is_current = true").select("customer_id"),
    "customer_id",
    "left_anti"
)

# Count BEFORE the write: the DataFrame is lazy, so counting after the append re-runs the
# anti-join against the table that now has those current rows, and reports 0
changed_count = changed_customers.count()

if changed_count > 0:
    changed_customers.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(CUSTOMERS_SCD2_TABLE)

    print(f"✅ Inserted {changed_count} new versions")
else:
    print("No changes detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Verify Your Results
# MAGIC Run the cell below. You should see **5** customers with 2 rows each (10 rows):
# MAGIC - The old row: `version = 1`, `is_current = false`, `effective_end_date` = today.
# MAGIC - The new row: `version = 2`, `is_current = true`, the upgraded `loyalty_tier`, `effective_start_date` = today.
# MAGIC
# MAGIC The Step 2 cell above prints `✅ Inserted 5 new versions` the first time. Re-run the notebook and the table stays at 10 rows for these 5 customers: the MERGE finds no tier difference and Step 2 prints `No changes detected`.

# COMMAND ----------

# 🔍 Verify SCD2 changes - show customers with multiple versions
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
# MAGIC Run these metrics after every Silver load to catch a quality drop before it reaches Gold.

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

# MAGIC %md
# MAGIC The combined view below puts one check next to each table's row count: `customers` counts rows with a valid email, `products` counts rows whose cost is present and not above the price (the ~10% of products with no cost and the ~2% with cost above price fail it, so expect roughly 1 product in 8 to fail), and `sales` counts lines matched to a known customer (the 1,107 `Unknown customer` lines are the ones that fail it).

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
        SUM(CASE WHEN is_valid_cost THEN 1 ELSE 0 END)
    FROM {PRODUCTS_SILVER_TABLE}
    
    UNION ALL
    
    SELECT 
        'Bronze → Silver Pipeline',
        current_timestamp(),
        'sales',
        COUNT(*),
        SUM(CASE WHEN customer_name <> 'Unknown customer' THEN 1 ELSE 0 END)
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
# MAGIC - Quality metrics queries across the Silver tables
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
# MAGIC Your four Silver tables are written: `customers_clean`, `products_clean`, `sales_clean` and `dim_customers_scd2`. Next: Notebook 04 builds the three Gold tables from `sales_clean`.
