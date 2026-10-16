from pyspark import pipelines as dp
from pyspark.sql import functions as F
from variables import *


# Complete reference solution for Tasks 1-20. Read it, or copy one task at a time into
# its task file. Never add this file to the pipeline next to the task files: it defines
# the same tables, and the update fails on the duplicates.

# -----------------------------------------
# 01 BRONZE: Raw Layer
# -----------------------------------------

# Task 1
@dp.table(name=BRONZE_SALES, comment = "Raw sales data")
@dp.expect_or_fail("valid_transaction_id", "transaction_id is not null")
def bronze_sales():
    return (
        spark.readStream.format("delta").load(RAW_SALES_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("transaction_id", F.col("transaction_id").cast("int"))
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("total_amount", F.col("total_amount").cast("double"))
        .withColumn("payment_method", F.col("payment_method").cast("string"))
        .withColumn("discount_applied", F.col("discount_applied").cast("double"))
        .withColumn("tax_amount", F.col("tax_amount").cast("double"))
    )

# Task 2
@dp.table(name=BRONZE_CUSTOMERS, comment="Raw customers data from landing zone")
@dp.expect_or_fail("valid_customer_id", "customer_id is not null")
def bronze_customers():
    return (
        spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("address", F.col("address").cast("string"))
        .withColumn("join_date", F.col("join_date").cast("timestamp"))
        .withColumn("loyalty_points", F.col("loyalty_points").cast("int"))
        .withColumn("phone_number", F.col("phone_number").cast("string"))
        .withColumn("age", F.col("age").cast("int"))
        .withColumn("gender", F.col("gender").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )

# Task 3
@dp.table(name=BRONZE_PRODUCTS, comment="Raw products data")
@dp.expect_or_fail("valid_product_id", "product_id is not null")
def bronze_products():
    return (
        spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("product_id", F.col("product_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("category", F.col("category").cast("string"))
        .withColumn("brand", F.col("brand").cast("string"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
        .withColumn("size", F.col("size").cast("string"))
        .withColumn("color", F.col("color").cast("string"))
        .withColumn("description", F.col("description").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )

# Task 4
@dp.table(name=BRONZE_STORES, comment="Raw stores data")
@dp.expect_or_fail("valid_store_id", "store_id is not null")
def bronze_stores():
    return (
        spark.readStream.format("delta").load(RAW_STORES_PATH)
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("store_id", F.col("store_id").cast("int"))
        .withColumn("name", F.col("name").cast("string"))
        .withColumn("address", F.col("address").cast("string"))
        .withColumn("manager", F.col("manager").cast("string"))
        .withColumn("open_date", F.col("open_date").cast("timestamp"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("phone_number", F.col("phone_number").cast("string"))
        .withColumn("last_update_time", F.col("last_update_time").cast("timestamp"))
    )

# -----------------------------------------
# 02A SILVER: Cleaned Streams (Intermediate Views)
# -----------------------------------------

# Task 5
@dp.temporary_view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
@dp.expect("realistic_age", "age >= 18 AND age <= 100")
@dp.expect("realistic_loyalty_points", "loyalty_points >= 0")
@dp.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
def customers_cleaned_stream():
    return spark.readStream.table(BRONZE_CUSTOMERS)

# Task 6
@dp.temporary_view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_product_price", "price > 0 AND price < 1000")
@dp.expect("non_negative_stock", "stock_quantity >= 0")
@dp.expect("valid_category", "category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')")
@dp.expect("valid_brand", "brand IS NOT NULL")
def products_cleaned_stream():
    return spark.readStream.table(BRONZE_PRODUCTS)

# Task 7
@dp.temporary_view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect("manager_assigned", "manager <> 'Unknown'")
@dp.expect("valid_status", "status IN ('Open', 'Under Renovation')")
def stores_cleaned_stream():
    return spark.readStream.table(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))

# Task 8
@dp.temporary_view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
@dp.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')")
@dp.expect("realistic_discount", "discount_applied >= 0")
def sales_cleaned_stream():
    return (
        spark.readStream.table(BRONZE_SALES)
        .withColumn("date", F.col("event_time").cast("date"))
        .withWatermark("event_time", "10 minutes")
        # Data delay is controlled by LATENCY_MAX_S in data generator
    )

# -----------------------------------------
# 02B SILVER: SCD2 Dimension Streaming Tables
# (using dp.create_auto_cdc_flow)
# -----------------------------------------
# Task 9
dp.create_streaming_table(SILVER_CUSTOMERS)
dp.create_auto_cdc_flow(
    target=SILVER_CUSTOMERS,
    source=CUSTOMERS_CLEANED_STREAM,
    keys=["customer_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender'],
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2
)

# Task 10
dp.create_streaming_table(SILVER_PRODUCTS)
dp.create_auto_cdc_flow(
    target=SILVER_PRODUCTS,
    source=PRODUCTS_CLEANED_STREAM,
    keys=["product_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size'],
    stored_as_scd_type=2
)

# Task 11
dp.create_streaming_table(SILVER_STORES)
dp.create_auto_cdc_flow(
    target=SILVER_STORES,
    source=STORES_CLEANED_STREAM,
    keys=["store_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'address', 'manager', 'status'],
    stored_as_scd_type=2
)

# -----------------------------------------
# 02C SILVER: Sales and Returns Fact Tables
# -----------------------------------------

# Task 12
@dp.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
@dp.expect_or_drop("valid_discount", "discount_applied >= 0")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_sales_transactions():
    return (
        spark.readStream.table(SALES_CLEANED_STREAM)
        .filter(F.col("quantity") > 0)
    )

# Task 13
@dp.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
@dp.expect_or_drop("valid_discount", "discount_applied >= 0")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_returns_transactions():
    return (
        spark.readStream.table(SALES_CLEANED_STREAM)
        .filter(F.col("quantity") < 0)
        .withColumn("returned_quantity", F.abs(F.col("quantity")))
        .withColumn("returned_amount", F.abs(F.col("total_amount")))
        .drop("quantity", "total_amount")
    )

# -----------------------------------------
# 02D SILVER: Create "Current" Dimension Views
# (current version of a key = the row where __END_AT IS NULL)
# -----------------------------------------

# Task 14
@dp.temporary_view(
    name=SILVER_CUSTOMERS_CURRENT,
    comment="Current customers (SCD2)"
)
def silver_customers_current():
    return spark.read.table(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())

# Task 15
@dp.temporary_view(
    name=SILVER_PRODUCTS_CURRENT,
    comment="Current products (SCD2)"
)
def silver_products_current():
    return spark.read.table(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())

# Task 16
@dp.temporary_view(
    name=SILVER_STORES_CURRENT,
    comment="Current stores (SCD2)"
)
def silver_stores_current():
    return spark.read.table(SILVER_STORES).filter(F.col("__END_AT").isNull())

# -----------------------------------------
# 03 GOLD: Denormalized Analytics and Aggregates
# -----------------------------------------

# Task 17
@dp.table(
    name=GOLD_DENORMALIZED_SALES_FACTS,
    comment="Sales joined to the current customer, product and store"
)
def denormalized_sales_facts():
    sales = spark.readStream.table(SILVER_SALES_TRANSACTIONS)
    customers = spark.read.table(SILVER_CUSTOMERS_CURRENT)
    products = spark.read.table(SILVER_PRODUCTS_CURRENT)
    stores = spark.read.table(SILVER_STORES_CURRENT)
    
    df = (
        sales
        .join(stores, "store_id", "left")
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .select(
            sales.transaction_id,
            sales.event_time,
            sales["date"].alias("sale_date"),
            sales.customer_id,
            customers.name.alias("customer_name"),
            sales.product_id,
            products.name.alias("product_name"),
            products.category.alias("product_category"),
            sales.store_id,
            stores.name.alias("store_name"),
            sales.quantity,
            sales.unit_price,
            sales.total_amount,
            sales.payment_method,
            sales.discount_applied,
            sales.tax_amount,
        )
    )
    return df

# Task 18
@dp.materialized_view(
    name=GOLD_DAILY_SALES_BY_STORE,
    comment="Daily store sales summary"
)
def gold_daily_sales_by_store():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    stores = spark.read.table(SILVER_STORES_CURRENT).select("store_id", F.col("name").alias("store_name"))
    agg = (
        df.groupBy("sale_date", "store_id")
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("transaction_id").alias("total_transactions"),
                F.sum("quantity").alias("total_items_sold"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
            .join(stores, "store_id", "left")
            .select(
                "sale_date",
                "store_id",
                "store_name",
                "total_revenue",
                "total_transactions",
                "total_items_sold",
                "unique_customers"
            )
    )
    return agg

# Task 19
@dp.materialized_view(
    name=GOLD_PRODUCT_PERFORMANCE,
    comment="Product sales performance metrics"
)
def gold_product_performance():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    products = spark.read.table(SILVER_PRODUCTS_CURRENT).select(
        "product_id",
        F.col("name").alias("product_name"),
        F.col("category").alias("product_category")
    )
    agg = (
        df.groupBy("product_id")
          .agg(
              F.round(F.sum("total_amount"),2).alias("total_revenue"),
              F.sum("quantity").alias("total_quantity_sold"),
              F.count("transaction_id").alias("total_orders")
          )
          .join(products, "product_id", "left")
          .select(
              "product_id",
              "product_name",
              "product_category",
              "total_revenue",
              "total_quantity_sold",
              "total_orders"
          )
    )
    return agg

# Task 20
@dp.materialized_view(
    name=GOLD_CUSTOMER_LIFETIME_VALUE,
    comment="Customer lifetime value metrics"
)
def gold_customer_lifetime_value():
    df = spark.read.table(GOLD_DENORMALIZED_SALES_FACTS)
    customers = spark.read.table(SILVER_CUSTOMERS_CURRENT).select("customer_id", F.col("name").alias("customer_name"))
    agg = (
        df.groupBy("customer_id")
          .agg(
              F.round(F.sum("total_amount"),2).alias("total_spend"),
              F.countDistinct("transaction_id").alias("total_orders"),
              F.min("event_time").cast("date").alias("first_purchase_date"),
              F.max("event_time").cast("date").alias("last_purchase_date"),
              F.round(F.avg("total_amount"),2).alias("avg_order_value")
          )
          .join(customers, "customer_id", "left")
          .select(
              "customer_id",
              "customer_name",
              "total_spend",
              "total_orders",
              "first_purchase_date",
              "last_purchase_date",
              "avg_order_value"
          )
    )
    return agg
