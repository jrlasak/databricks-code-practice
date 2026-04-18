# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: PySpark DataFrame Transformations
# MAGIC Creates per-exercise source tables from base tables in `db_code.elt`.
# MAGIC Each exercise gets independent tables so exercises are fully atomic.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "pyspark_transformations"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic select/filter/withColumn
# MAGIC Source: orders with all statuses. User filters to completed, adds tax column.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_ex1 AS
SELECT order_id, customer_id, product_id, amount, status, order_date, updated_at
FROM {CATALOG}.{BASE_SCHEMA}.orders
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Column renaming and type casting
# MAGIC Source: customers with camelCase-style columns and string dates for casting practice.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.customers_ex2 AS
SELECT
  customer_id AS customerId,
  name AS customerName,
  email AS emailAddress,
  region AS customerRegion,
  tier AS customerTier,
  CAST(signup_date AS STRING) AS signupDate
FROM {CATALOG}.{BASE_SCHEMA}.customers
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Complex when/otherwise chains
# MAGIC Source: orders with varying amounts and statuses for tier categorization.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_ex3 AS
SELECT order_id, customer_id, amount, status, order_date
FROM {CATALOG}.{BASE_SCHEMA}.orders
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Pivot - order counts by status per customer
# MAGIC Source: orders with multiple statuses per customer for pivoting. NULL customer_ids excluded.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_ex4 AS
SELECT order_id, customer_id, amount, status
FROM {CATALOG}.{BASE_SCHEMA}.orders
WHERE customer_id IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Unpivot with stack() - wide metrics to long format
# MAGIC Source: a wide metrics table with one row per customer and metric columns.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.customer_metrics_ex5 AS
SELECT
  c.customer_id,
  c.name,
  COALESCE(completed.cnt, 0) AS completed_orders,
  COALESCE(pending.cnt, 0) AS pending_orders,
  COALESCE(cancelled.cnt, 0) AS cancelled_orders,
  COALESCE(total_rev.rev, 0.0) AS total_revenue
FROM {CATALOG}.{BASE_SCHEMA}.customers c
LEFT JOIN (
  SELECT customer_id, COUNT(*) AS cnt
  FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE status = 'completed' GROUP BY customer_id
) completed ON c.customer_id = completed.customer_id
LEFT JOIN (
  SELECT customer_id, COUNT(*) AS cnt
  FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE status = 'pending' GROUP BY customer_id
) pending ON c.customer_id = pending.customer_id
LEFT JOIN (
  SELECT customer_id, COUNT(*) AS cnt
  FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE status = 'cancelled' GROUP BY customer_id
) cancelled ON c.customer_id = cancelled.customer_id
LEFT JOIN (
  SELECT customer_id, CAST(SUM(amount) AS DOUBLE) AS rev
  FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE status = 'completed' GROUP BY customer_id
) total_rev ON c.customer_id = total_rev.customer_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Python UDF for custom logic with null handling
# MAGIC Source: events table with JSON payloads to parse and classify.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.events_ex6 AS
SELECT event_id, user_id, event_type, event_ts, payload
FROM {CATALOG}.{BASE_SCHEMA}.events
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Replace UDF with native Spark functions
# MAGIC Source: events table. The UDF from Exercise 6 is provided as a baseline.
# MAGIC User rewrites it using only native functions.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.events_ex7 AS
SELECT event_id, user_id, event_type, event_ts, payload
FROM {CATALOG}.{BASE_SCHEMA}.events
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Chained transform pipeline with replaceWhere
# MAGIC Source: orders joined with customers + order_items for multi-step pipeline.
# MAGIC Pre-populate target table with cancelled orders for replaceWhere exercise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_ex8 AS
SELECT
  o.order_id,
  o.customer_id,
  o.amount,
  o.status,
  o.order_date,
  o.updated_at,
  c.name AS customer_name,
  c.region
FROM {CATALOG}.{BASE_SCHEMA}.orders o
LEFT JOIN {CATALOG}.{BASE_SCHEMA}.customers c
  ON o.customer_id = c.customer_id
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.order_items_ex8 AS
SELECT order_id, product_id, quantity, unit_price
FROM {CATALOG}.{BASE_SCHEMA}.order_items
""")

# Pre-populate target table with cancelled orders for replaceWhere exercise
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.enriched_orders_ex8 AS
SELECT
  order_id,
  customer_id,
  CAST(NULL AS STRING) AS customer_name,
  CAST(NULL AS STRING) AS region,
  amount,
  status,
  CAST(NULL AS STRING) AS amount_bucket,
  CAST(NULL AS STRUCT<total_items: BIGINT, total_value: DOUBLE>) AS items_summary,
  order_date,
  updated_at
FROM {CATALOG}.{BASE_SCHEMA}.orders
WHERE status = 'cancelled'
""")

# COMMAND ----------

print(f"Setup complete for PySpark DataFrame Transformations")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Exercise 1: orders_ex1 ({spark.table(f'{CATALOG}.{SCHEMA}.orders_ex1').count()} rows)")
print(f"  Exercise 2: customers_ex2 ({spark.table(f'{CATALOG}.{SCHEMA}.customers_ex2').count()} rows)")
print(f"  Exercise 3: orders_ex3 ({spark.table(f'{CATALOG}.{SCHEMA}.orders_ex3').count()} rows)")
print(f"  Exercise 4: orders_ex4 ({spark.table(f'{CATALOG}.{SCHEMA}.orders_ex4').count()} rows)")
print(f"  Exercise 5: customer_metrics_ex5 ({spark.table(f'{CATALOG}.{SCHEMA}.customer_metrics_ex5').count()} rows)")
print(f"  Exercise 6: events_ex6 ({spark.table(f'{CATALOG}.{SCHEMA}.events_ex6').count()} rows)")
print(f"  Exercise 7: events_ex7 ({spark.table(f'{CATALOG}.{SCHEMA}.events_ex7').count()} rows)")
print(f"  Exercise 8: orders_ex8 ({spark.table(f'{CATALOG}.{SCHEMA}.orders_ex8').count()} rows)")
print(f"  Exercise 8: order_items_ex8 ({spark.table(f'{CATALOG}.{SCHEMA}.order_items_ex8').count()} rows)")
print(f"  Exercise 8: enriched_orders_ex8 ({spark.table(f'{CATALOG}.{SCHEMA}.enriched_orders_ex8').count()} rows)")
