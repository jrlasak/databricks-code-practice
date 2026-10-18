# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Change Data Feed (CDF)
# MAGIC Creates one table per exercise in `db_code.change_data_feed` (CDF on for every table except `cdf_ex1_orders`) and applies the INSERT, UPDATE, DELETE and MERGE each exercise reads back.
# MAGIC
# MAGIC The exercise notebook runs it for you with `%run`.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "change_data_feed"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

# COMMAND ----------

# Drop every table first so each one starts again at version 0 on every run (CREATE OR REPLACE would keep the old history)
for t in ["cdf_ex1_orders", "cdf_ex2_orders", "cdf_ex3_orders", "cdf_ex4_orders", "cdf_ex5_orders",
          "cdf_ex6_source", "cdf_ex6_target", "cdf_ex7_orders", "cdf_ex7_source"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")

# Drop the output tables your answers create (and the leftover cdf_ex7_changes), so an unsolved exercise fails its check on every rerun
for t in ["cdf_ex2_changes", "cdf_ex3_changes", "cdf_ex4_changes", "cdf_ex5_inserts", "cdf_ex7_counts",
          "cdf_ex7_changes"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")

# Exercise 1: table without CDF (you enable it)
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex1_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# Exercise 2: CDF enabled, then INSERT 2 rows at v1
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex2_orders
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.cdf_ex2_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
    ('ORD-102', 'CUST-011', 'PROD-003', 89.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")

# Exercise 3: CDF enabled, then UPDATE at v1
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex3_orders
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.cdf_ex3_orders
    SET status = 'shipped', amount = 120.00
    WHERE order_id = 'ORD-001'
""")

# Exercise 4: CDF enabled, then DELETE at v1
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex4_orders
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.cdf_ex4_orders
    WHERE order_id = 'ORD-005'
""")

# COMMAND ----------

# Exercise 5: CDF enabled, then INSERT + UPDATE + DELETE (v1, v2, v3)
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex5_orders
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.cdf_ex5_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.cdf_ex5_orders
    SET status = 'shipped' WHERE order_id = 'ORD-001'
""")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.cdf_ex5_orders
    WHERE order_id = 'ORD-005'
""")

# Exercise 6: source with CDF + target (copy of v0 state, needs sync)
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex6_source
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.cdf_ex6_source
    SET status = 'shipped', amount = 120.00
    WHERE order_id = 'ORD-001'
""")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.cdf_ex6_source
    WHERE order_id = 'ORD-005'
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.cdf_ex6_source VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")
# Target: copy of source's initial state (v0) - out of sync
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex6_target
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{SCHEMA}.cdf_ex6_source VERSION AS OF 0
""")

# Exercise 7: CDF enabled, then MERGE at v1
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex7_orders
    TBLPROPERTIES (delta.enableChangeDataFeed = true, 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.cdf_ex7_source AS
    SELECT * FROM VALUES
        ('ORD-001', 'CUST-001', 'PROD-001', 130.00, 'shipped', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.cdf_ex7_orders t
    USING {CATALOG}.{SCHEMA}.cdf_ex7_source s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
