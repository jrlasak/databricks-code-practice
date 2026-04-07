# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Spark SQL Joins & Aggregations
# MAGIC Creates per-topic schema, pre-computes expected counts for assertions,
# MAGIC and cleans up previous exercise output tables. Run automatically via `%run`.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "spark_sql_joins"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Pre-compute expected counts for deterministic assertions.
# These depend on the base data created by 00_Setup.py.

# Exercise 1: inner join orders x customers (drops orders with NULL customer_id)
_ex1_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
    ON o.customer_id = c.customer_id
""").collect()[0].cnt
JOINS_EX1_EXPECTED_COUNT = _ex1_count

# Exercise 3: anti-join orphan order_items (order_ids not in orders)
_ex3_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.{BASE_SCHEMA}.order_items oi
    LEFT ANTI JOIN {CATALOG}.{BASE_SCHEMA}.orders o
    ON oi.order_id = o.order_id
""").collect()[0].cnt
JOINS_EX3_EXPECTED_COUNT = _ex3_count

# Exercise 4: semi-join customers who have at least one order
_ex4_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.{BASE_SCHEMA}.customers c
    LEFT SEMI JOIN {CATALOG}.{BASE_SCHEMA}.orders o
    ON c.customer_id = o.customer_id
""").collect()[0].cnt
JOINS_EX4_EXPECTED_COUNT = _ex4_count

# Exercise 5: self-join same-day orders by same customer
_ex5_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM {CATALOG}.{BASE_SCHEMA}.orders o1
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.orders o2
    ON o1.customer_id = o2.customer_id
       AND o1.order_date = o2.order_date
       AND o1.order_id < o2.order_id
    WHERE o1.customer_id IS NOT NULL
""").collect()[0].cnt
JOINS_EX5_EXPECTED_COUNT = _ex5_count

# Exercise 6: multi-table join chain with QUALIFY (one row per order_id)
_ex6_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM (
        SELECT o.order_id,
               ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY oi.quantity DESC) AS rn
        FROM {CATALOG}.{BASE_SCHEMA}.orders o
        INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c ON o.customer_id = c.customer_id
        INNER JOIN {CATALOG}.{BASE_SCHEMA}.order_items oi ON o.order_id = oi.order_id
        INNER JOIN {CATALOG}.{BASE_SCHEMA}.products p ON oi.product_id = p.product_id
    ) WHERE rn = 1
""").collect()[0].cnt
JOINS_EX6_EXPECTED_COUNT = _ex6_count

# Exercise 7: GROUP BY with HAVING - high-value customers
_ex7_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM (
        SELECT o.customer_id, SUM(o.amount) AS total_spent
        FROM {CATALOG}.{BASE_SCHEMA}.orders o
        WHERE o.customer_id IS NOT NULL
        GROUP BY o.customer_id
        HAVING SUM(o.amount) > 500
    )
""").collect()[0].cnt
JOINS_EX7_EXPECTED_COUNT = _ex7_count

# COMMAND ----------

# Clean up any previous exercise output tables (idempotent)
for tbl in [
    "joins_ex1_output",
    "joins_ex2_output",
    "joins_ex3_output",
    "joins_ex4_output",
    "joins_ex5_output",
    "joins_ex6_output",
    "joins_ex7_output",
    "joins_ex8_rollup",
    "joins_ex9_cube",
]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

print(f"Setup complete. Exercise output tables go in {CATALOG}.{SCHEMA}.")
print(f"Expected counts: Ex1={JOINS_EX1_EXPECTED_COUNT}, Ex3={JOINS_EX3_EXPECTED_COUNT}, Ex4={JOINS_EX4_EXPECTED_COUNT}, Ex5={JOINS_EX5_EXPECTED_COUNT}, Ex6={JOINS_EX6_EXPECTED_COUNT}, Ex7={JOINS_EX7_EXPECTED_COUNT}")
