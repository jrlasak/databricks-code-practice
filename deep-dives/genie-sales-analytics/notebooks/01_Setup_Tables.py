# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup Tables
# MAGIC Creates the `aurora_retail` catalog, the `sales` schema, and the 7 tables of a clean
# MAGIC **retail sales star schema** - with rich Unity Catalog column comments. Those comments are
# MAGIC the single biggest lever on Genie accuracy: Genie reads them before it writes any SQL.
# MAGIC
# MAGIC | Table | Grain | Role |
# MAGIC |-------|-------|------|
# MAGIC | `dim_date` | one row per calendar day | date dimension |
# MAGIC | `dim_product` | one row per product (SKU) | product dimension |
# MAGIC | `dim_customer` | one row per customer | customer dimension |
# MAGIC | `dim_store` | one row per store / channel | store dimension |
# MAGIC | `fact_sales` | one row per order line | the sales fact |
# MAGIC | `sales_targets` | one row per region × month | monthly revenue targets |
# MAGIC | `performance_insights` | one row per ranked driver | precomputed gap drivers (filled by notebook 04) |
# MAGIC
# MAGIC Column names are clean `snake_case` on purpose - no spaces, so no Delta column mapping and no
# MAGIC backticks anywhere in Genie. Re-running is safe (`CREATE OR REPLACE` / `IF NOT EXISTS`).

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Catalog + schema (idempotent). Free Edition allows creating catalogs.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Catalog + schema ready: {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_DATE} (
    date_key      INT     COMMENT 'Surrogate key in yyyymmdd form (e.g. 20250515). Join target for fact_sales.date_key.',
    date          DATE    COMMENT 'Calendar date.',
    day_name      STRING  COMMENT 'Day of week name: Monday..Sunday.',
    week_of_year  INT     COMMENT 'ISO week number, 1..53.',
    month         INT     COMMENT 'Month number, 1..12.',
    month_name    STRING  COMMENT 'Month name: January..December.',
    year_month    STRING  COMMENT 'Year and month as YYYY-MM (e.g. 2025-05). Use this for monthly grouping. The "this month" anchor is the most recent value present: SELECT MAX(year_month) FROM this table.',
    quarter       INT     COMMENT 'Calendar quarter, 1..4.',
    year          INT     COMMENT 'Calendar year.',
    is_weekend    BOOLEAN COMMENT 'TRUE for Saturday/Sunday.'
)
COMMENT 'Date dimension, one row per calendar day spanning the trailing 12 months. The "current month" in every demo question is the most recent month present: SELECT MAX(year_month) FROM this table.'
""")
print("dim_date ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_product

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_PRODUCT} (
    product_key   INT     COMMENT 'Surrogate key. Join target for fact_sales.product_key.',
    product_id    STRING  COMMENT 'Business product code, e.g. SKU-1042.',
    product_name  STRING  COMMENT 'Human-readable product name, e.g. "Summit 600 Down Jacket".',
    category      STRING  COMMENT 'Top-level merchandising category: Outdoor & Camping, Apparel, Footwear, Electronics, Home & Kitchen, Sports & Fitness, Travel & Bags, Accessories.',
    subcategory   STRING  COMMENT 'Finer grouping within a category.',
    brand         STRING  COMMENT 'Brand name.',
    unit_cost     DOUBLE  COMMENT 'Cost of goods per unit in USD. Used for margin: gross_margin = net_sales - quantity*unit_cost.',
    list_price    DOUBLE  COMMENT 'Catalog list price per unit in USD before any discount.'
)
COMMENT 'Product dimension, one row per SKU. ~60 products across 8 categories.'
""")
print("dim_product ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_CUSTOMER} (
    customer_key   INT     COMMENT 'Surrogate key. Join target for fact_sales.customer_key.',
    customer_id    STRING  COMMENT 'Business customer code, e.g. CUST-004821.',
    customer_name  STRING  COMMENT 'Customer full name.',
    loyalty_tier   STRING  COMMENT 'Loyalty program tier: Standard, Plus, or VIP. VIP are the highest-value members.',
    region         STRING  COMMENT 'Customer home region: West, Midwest, Northeast, South.',
    country        STRING  COMMENT 'Customer country (this dataset is US-only: "USA").',
    signup_date    DATE    COMMENT 'Date the customer joined the loyalty program.'
)
COMMENT 'Customer dimension, one row per customer. ~500 customers.'
""")
print("dim_customer ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_store

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DIM_STORE} (
    store_key    INT     COMMENT 'Surrogate key. Join target for fact_sales.store_key.',
    store_id     STRING  COMMENT 'Business store code, e.g. STR-12.',
    store_name   STRING  COMMENT 'Store name, e.g. "Denver Flagship" or "Online - West".',
    channel      STRING  COMMENT 'Sales channel: "Retail" for physical stores, "Online" for the e-commerce channel. Use this to compare online vs in-store performance.',
    city         STRING  COMMENT 'City the store serves (Online stores carry the regional hub city).',
    region       STRING  COMMENT 'Store region: West, Midwest, Northeast, South. This is the region used for regional sales analysis and target comparison.',
    country      STRING  COMMENT 'Store country (US-only: "USA").'
)
COMMENT 'Store dimension, one row per physical store plus one Online store per region. ~20 rows. The region column here drives all regional sales reporting.'
""")
print("dim_store ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FACT_SALES} (
    sale_id         STRING  COMMENT 'Unique order-line identifier.',
    order_id        STRING  COMMENT 'Order identifier. One order can have multiple sale lines.',
    date_key        INT     COMMENT 'Date of sale in yyyymmdd form. Join to dim_date.date_key. To filter by month, join dim_date and filter year_month.',
    product_key     INT     COMMENT 'Join to dim_product.product_key for product name, category, cost, and price.',
    customer_key    INT     COMMENT 'Join to dim_customer.customer_key for loyalty tier and customer region.',
    store_key       INT     COMMENT 'Join to dim_store.store_key for channel (Retail/Online) and store region. Use the STORE region for regional sales analysis.',
    quantity        INT     COMMENT 'Units sold on this line.',
    unit_price      DOUBLE  COMMENT 'Price charged per unit in USD (equals list_price; promotional reductions are captured in discount_amount).',
    discount_amount DOUBLE  COMMENT 'Total dollar discount applied to this line in USD.',
    net_sales       DOUBLE  COMMENT 'Net revenue for this line in USD = quantity*unit_price - discount_amount. This is the primary revenue measure - sum it for total sales / revenue.',
    cost_amount     DOUBLE  COMMENT 'Total cost of goods for this line in USD = quantity*unit_cost.',
    gross_margin    DOUBLE  COMMENT 'Gross profit for this line in USD = net_sales - cost_amount. Sum it for total margin; divide by net_sales for margin percent.'
)
COMMENT 'Sales fact, one row per order line. ~120,000 rows over 12 months. net_sales is the revenue measure; gross_margin is the profit measure.'
""")
print("fact_sales ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sales_targets

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SALES_TARGETS} (
    region          STRING  COMMENT 'Region the target applies to: West, Midwest, Northeast, South.',
    year_month      STRING  COMMENT 'Target month as YYYY-MM (e.g. 2025-05).',
    revenue_target  DOUBLE  COMMENT 'Monthly net-revenue target for this region in USD. Compare SUM(fact_sales.net_sales) for the matching region and month against this to judge "on track vs target".'
)
COMMENT 'Monthly net-revenue targets per region. Join region + year_month against actual net_sales to answer "are we on track this month".'
""")
print("sales_targets ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## performance_insights (empty until notebook 04 runs)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {PERFORMANCE_INSIGHTS} (
    region                   STRING  COMMENT 'Region the insight is about.',
    year_month               STRING  COMMENT 'Month the insight is about, YYYY-MM.',
    rank                     INT     COMMENT 'Rank 1..N - 1 is the single biggest driver of the gap to target.',
    driver_type              STRING  COMMENT 'What kind of driver: "CATEGORY" or "PRODUCT".',
    dimension_value          STRING  COMMENT 'The category name or product name this driver row refers to.',
    revenue_current          DOUBLE  COMMENT 'Net revenue for this driver in the analyzed month, USD.',
    revenue_prior            DOUBLE  COMMENT 'Net revenue for this driver in the prior month, USD.',
    revenue_delta            DOUBLE  COMMENT 'revenue_current - revenue_prior, USD. Negative means it shrank (a detractor).',
    contribution_to_gap_pct  DOUBLE  COMMENT 'Share of the regions month-over-month revenue decline attributable to this driver, in percent.',
    insight                  STRING  COMMENT 'One-sentence natural-language summary of this driver.'
)
COMMENT 'Precomputed ranked drivers of each regions revenue gap. Populated by 04_Compute_Insights. Read by the top_drivers() and explain_driver() UC functions. Empty until notebook 04 runs (that is expected, not an error).'
""")
print("performance_insights ready (empty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, comment
FROM {CATALOG}.information_schema.tables
WHERE table_schema = '{SCHEMA}'
ORDER BY table_name
"""))
print("Expected: 7 tables (dim_date, dim_product, dim_customer, dim_store, fact_sales, sales_targets, performance_insights).")
