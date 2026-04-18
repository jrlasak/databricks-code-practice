import dlt
from datetime import datetime, timezone
from pyspark.sql import functions as F
from variables import *


# -----------------------------------------
# All table names
# -----------------------------------------

BRONZE_SALES = f"{BRONZE_SCHEMA}.bronze_sales"
BRONZE_CUSTOMERS = f"{BRONZE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS = f"{BRONZE_SCHEMA}.bronze_products"
BRONZE_STORES = f"{BRONZE_SCHEMA}.bronze_stores"

CUSTOMERS_CLEANED_STREAM = "customers_cleaned_stream"
PRODUCTS_CLEANED_STREAM = "products_cleaned_stream"
STORES_CLEANED_STREAM = "stores_cleaned_stream"
SALES_CLEANED_STREAM = "sales_cleaned_stream"

SILVER_CUSTOMERS = f"{SILVER_SCHEMA}.silver_customers"
SILVER_PRODUCTS = f"{SILVER_SCHEMA}.silver_products"
SILVER_STORES = f"{SILVER_SCHEMA}.silver_stores"
SILVER_SALES_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_sales_transactions"
SILVER_RETURNS_TRANSACTIONS = f"{SILVER_SCHEMA}.silver_returns_transactions"

SILVER_CUSTOMERS_CURRENT = "silver_customers_current"
SILVER_PRODUCTS_CURRENT = "silver_products_current"
SILVER_STORES_CURRENT = "silver_stores_current"

GOLD_DENORMALIZED_SALES_FACTS = f"{GOLD_SCHEMA}.denormalized_sales_facts"
GOLD_DAILY_SALES_BY_STORE = f"{GOLD_SCHEMA}.gold_daily_sales_by_store"
GOLD_PRODUCT_PERFORMANCE = f"{GOLD_SCHEMA}.gold_product_performance"
GOLD_CUSTOMER_LIFETIME_VALUE = f"{GOLD_SCHEMA}.gold_customer_lifetime_value"

# -----------------------------------------
# 01 BRONZE: Raw Layer
# -----------------------------------------

# Task 1
@dlt.table(name=BRONZE_SALES, comment = "Raw sales data")
@dlt.expect_or_fail("valid_transaction_id", "transaction_id is not null")
def bronze_sales():
    return (
        spark.readStream.format("delta").load(RAW_SALES_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
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
@dlt.table(name=BRONZE_CUSTOMERS, comment="Raw customers data from landing zone")
@dlt.expect_or_fail("valid_customer_id", "customer_id is not null")
def bronze_customers():
    return (
        spark.readStream.format("delta").load(RAW_CUSTOMERS_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
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
@dlt.table(name=BRONZE_PRODUCTS, comment="Raw products data")
@dlt.expect_or_fail("valid_product_id", "product_id is not null")
def bronze_products():
    return (
        spark.readStream.format("delta").load(RAW_PRODUCTS_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
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
@dlt.table(name=BRONZE_STORES, comment="Raw stores data")
@dlt.expect_or_fail("valid_store_id", "store_id is not null")
def bronze_stores():
    return (
        spark.readStream.format("delta").load(RAW_STORES_PATH)
        .withColumn("ingest_timestamp", F.lit(datetime.now(timezone.utc)))
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
@dlt.view(name=CUSTOMERS_CLEANED_STREAM, comment="QC for customers stream")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email", "email IS NULL OR email LIKE '%@%.%'")
@dlt.expect("realistic_age", "age >= 18 AND age <= 100")
@dlt.expect("realistic_loyalty_points", "loyalty_points >= 0")
@dlt.expect("valid_gender", "gender IN ('Male', 'Female', 'Other')")
def customers_cleaned_stream():
    return dlt.read_stream(BRONZE_CUSTOMERS)

# Task 6
@dlt.view(name=PRODUCTS_CLEANED_STREAM, comment="QC for products stream")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_price", "price > 0 AND price < 10000")
@dlt.expect("non_negative_stock", "stock_quantity >= 0")
@dlt.expect("valid_category", "category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')")
@dlt.expect("valid_brand", "brand IS NOT NULL")
def products_cleaned_stream():
    return dlt.read_stream(BRONZE_PRODUCTS)

# Task 7
@dlt.view(name=STORES_CLEANED_STREAM, comment="QC for stores stream")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect("valid_manager_name", "manager IS NOT NULL")
@dlt.expect("valid_status", "status IN ('Open', 'Under Renovation')")
def stores_cleaned_stream():
    return dlt.read_stream(BRONZE_STORES).withColumn("manager", F.coalesce(F.col("manager"), F.lit("Unknown")))

# Task 8
@dlt.view(name=SALES_CLEANED_STREAM, comment="QC for sales stream (fact)")
@dlt.expect("valid_payment_method", "payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')")
@dlt.expect("realistic_discount", "discount_applied >= 0")
def sales_cleaned_stream():
    return (
        dlt.read_stream(BRONZE_SALES)
        .withColumn("date", F.col("event_time").cast("date"))
        .withWatermark("event_time", "10 minutes")
        # Data delay is controlled by LATENCY_MAX_S in data generator
    )

# -----------------------------------------
# 02B SILVER: Materialized SCD2 Business Dimension Tables
# (using create_auto_cdc_flow)
# -----------------------------------------
# Task 9
dlt.create_streaming_table(SILVER_CUSTOMERS)
dlt.create_auto_cdc_flow(
    target=SILVER_CUSTOMERS,
    source=f"live.{CUSTOMERS_CLEANED_STREAM}",
    keys=["customer_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    track_history_column_list=['name', 'email', 'address', 'phone_number', 'gender'],
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    stored_as_scd_type=2
)

# Task 10
dlt.create_streaming_table(SILVER_PRODUCTS)
dlt.create_auto_cdc_flow(
    target=SILVER_PRODUCTS,
    source=f"live.{PRODUCTS_CLEANED_STREAM}",
    keys=["product_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'category', 'brand', 'price', 'description', 'color', 'size'],
    stored_as_scd_type=2
)

# Task 11
dlt.create_streaming_table(SILVER_STORES)
dlt.create_auto_cdc_flow(
    target=SILVER_STORES,
    source=f"live.{STORES_CLEANED_STREAM}",
    keys=["store_id"],
    sequence_by=F.col("last_update_time"),
    ignore_null_updates=True,
    except_column_list=["last_update_time", "ingest_timestamp", "source_file_path"],
    track_history_column_list=['name', 'address', 'manager', 'status'],
    stored_as_scd_type=2
)

# Task 12
@dlt.table(name=SILVER_SALES_TRANSACTIONS, comment="Stream sales table (positive quantity)")
@dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_sales_transactions():
    return (
        dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
        .filter(F.col("quantity") > 0)
    )

# Task 13
@dlt.table(name=SILVER_RETURNS_TRANSACTIONS, comment="Stream returns table (negative quantity)")
@dlt.expect_or_drop("valid_discount", "discount_applied >= 0")
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_returns_transactions():
    return (
        dlt.read_stream(f"live.{SALES_CLEANED_STREAM}")
        .filter(F.col("quantity") < 0)
        .withColumn("returned_quantity", F.abs(F.col("quantity")))
        .withColumn("returned_amount", F.abs(F.col("total_amount")))
        .drop("quantity", "total_amount")
    )

# -----------------------------------------
# 02C SILVER: Current State Views for Dimension Lookup
# (using __END_AT IS NULL for current, since create_auto_cdc_flow does not use _dlt_current)
# -----------------------------------------

# Task 14
@dlt.view(
    name=SILVER_CUSTOMERS_CURRENT,
    comment="Current customers (SCD2)"
)
def silver_customers_current():
    return dlt.read(SILVER_CUSTOMERS).filter(F.col("__END_AT").isNull())

# Task 15
@dlt.view(
    name=SILVER_PRODUCTS_CURRENT,
    comment="Current products (SCD2)"
)
def silver_products_current():
    return dlt.read(SILVER_PRODUCTS).filter(F.col("__END_AT").isNull())

# Task 16
@dlt.view(
    name=SILVER_STORES_CURRENT,
    comment="Current stores (SCD2)"
)
def silver_stores_current():
    return dlt.read(SILVER_STORES).filter(F.col("__END_AT").isNull())

# -----------------------------------------
# 03 GOLD: Denormalized Analytics and Aggregates
# -----------------------------------------

# Task 17
@dlt.table(
    name=GOLD_DENORMALIZED_SALES_FACTS,
    comment="Fully denormalized sales fact table for analytic use. A streaming table."
)
def denormalized_sales_facts():
    sales = dlt.read_stream(f"LIVE.{SILVER_SALES_TRANSACTIONS}")
    customers = dlt.read(f"LIVE.{SILVER_CUSTOMERS_CURRENT}")
    products = dlt.read(f"LIVE.{SILVER_PRODUCTS_CURRENT}")
    stores = dlt.read(f"LIVE.{SILVER_STORES_CURRENT}")
    
    df = (
        sales
        .join(stores, "store_id", "left")
        .join(customers, "customer_id", "left")
        .join(products, "product_id", "left")
        .select(
            sales.transaction_id,
            sales.event_time,
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
@dlt.table(
    name=GOLD_DAILY_SALES_BY_STORE,
    comment="Daily store sales summary. A batch table."
)
def gold_daily_sales_by_store():
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy(
                F.window("event_time", "1 day").alias("sale_window"),
                "store_id",
                "store_name"
            )
            .agg(
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.count("transaction_id").alias("total_transactions"),
                F.sum("quantity").alias("total_items_sold"),
                F.countDistinct("customer_id").alias("unique_customers")
            )
            .select(
                F.col("sale_window.start").cast("date").alias("sale_date"),
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
@dlt.table(
    name=GOLD_PRODUCT_PERFORMANCE,
    comment="Product sales performance metrics. A batch table."
)
def gold_product_performance():
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy("product_id", "product_name", "product_category")
          .agg(
              F.round(F.sum("total_amount"),2).alias("total_revenue"),
              F.round(F.sum("quantity"), 2).alias("total_quantity_sold"),
              F.count("transaction_id").alias("total_orders")
          )
    )
    return agg

# Task 20
@dlt.table(
    name=GOLD_CUSTOMER_LIFETIME_VALUE,
    comment="Customer lifetime value metrics. A batch table."
)
def gold_customer_lifetime_value():
    df = dlt.read(f"LIVE.{GOLD_DENORMALIZED_SALES_FACTS}")
    agg = (
        df.groupBy("customer_id", "customer_name")
          .agg(
              F.round(F.sum("total_amount"),2).alias("total_spend"),
              F.countDistinct("transaction_id").alias("total_orders"),
              F.min("event_time").cast("date").alias("first_purchase_date"),
              F.max("event_time").cast("date").alias("last_purchase_date"),
              F.round(F.avg("total_amount"),2).alias("avg_order_value")
          )
    )
    return agg
