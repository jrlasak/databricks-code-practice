# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: MERGE Operations
# MAGIC Creates per-exercise target and source tables. Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "merge_operations"
BASE_SCHEMA = "delta_lake"

# Each exercise gets its own target (copy of 5 base orders) and source table.
# This keeps exercises fully independent - exercise 2 doesn't depend on exercise 1.
BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

# Target tables: copy 5 orders from base table for each exercise (except Ex 8 which uses customers)
for i in range(1, 10):
    if i == 8:
        continue
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex{i}_target
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
        WHERE order_id IN {BASE_IDS}
    """)

# COMMAND ----------

# Exercise 1-2 source: 2 existing orders (modified) + 2 new
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex1_source AS
    SELECT order_id, customer_id, product_id,
           CASE WHEN order_id = 'ORD-001' THEN 109.50
                WHEN order_id = 'ORD-002' THEN 175.00 END AS amount,
           'shipped' AS status, order_date,
           TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002')
    UNION ALL
    SELECT * FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-102', 'CUST-011', 'PROD-003', 89.00, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")
spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex2_source AS SELECT * FROM {CATALOG}.{SCHEMA}.merge_ex1_source")

# Exercise 3 source: 3 existing orders with updated values (no new orders)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex3_source AS
    SELECT order_id, customer_id, product_id,
           amount + 10.00 AS amount,
           'shipped' AS status, order_date,
           TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002', 'ORD-003')
""")

# Exercise 4 source: has DUPLICATE order_ids (must dedup before merge)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex4_source AS
    SELECT * FROM VALUES
        ('ORD-001', 'CUST-001', 'PROD-001', 99.50, 'shipped', DATE '2026-03-01', TIMESTAMP '2026-03-01 08:00:00'),
        ('ORD-001', 'CUST-001', 'PROD-001', 119.50, 'shipped', DATE '2026-03-01', TIMESTAMP '2026-03-01 12:00:00'),
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# Exercise 5 source: mixed timestamps (newer + older than target)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex5_source AS
    SELECT order_id, customer_id, product_id, 109.50 AS amount, 'shipped' AS status,
           order_date, TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-001'
    UNION ALL
    SELECT order_id, customer_id, product_id, 50.00 AS amount, 'returned' AS status,
           order_date, TIMESTAMP '2025-01-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-002'
    UNION ALL
    SELECT * FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# Exercise 6 source: includes cancelled order (for DELETE)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex6_source AS
    SELECT order_id, customer_id, product_id, amount,
           CASE WHEN order_id = 'ORD-001' THEN 'cancelled' ELSE 'shipped' END AS status,
           order_date, TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002')
    UNION ALL
    SELECT * FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# Exercise 7 source: multi-condition (delete + update + skip + insert)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex7_source AS
    SELECT order_id, customer_id, product_id, amount, 'cancelled' AS status,
           order_date, TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-001'
    UNION ALL
    SELECT order_id, customer_id, product_id, 175.00 AS amount, 'shipped' AS status,
           order_date, TIMESTAMP '2026-03-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-002'
    UNION ALL
    SELECT order_id, customer_id, product_id, 50.00 AS amount, 'pending' AS status,
           order_date, TIMESTAMP '2025-01-01 10:00:00' AS updated_at
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-003'
    UNION ALL
    SELECT * FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-102', 'CUST-011', 'PROD-003', 89.00, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# COMMAND ----------

# Exercise 8: SCD Type 2 customer dimension (different table structure)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex8_target (
        customer_id STRING, name STRING, email STRING, region STRING, tier STRING,
        is_current BOOLEAN, effective_start_date DATE, effective_end_date DATE
    ) USING DELTA
""")
spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.{SCHEMA}.merge_ex8_target
    SELECT customer_id, name, email, region, tier,
           true, signup_date, DATE '9999-12-31'
    FROM {CATALOG}.{BASE_SCHEMA}.customers
    WHERE customer_id IN ('CUST-001', 'CUST-002', 'CUST-003')
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex8_source AS
    SELECT * FROM VALUES
        ('CUST-001', 'Alice Smith', 'alice.new@example.com', 'US-East', 'platinum'),
        ('CUST-003', 'Carol Lee', 'carol@example.com', 'US-West', 'silver'),
        ('CUST-010', 'New Customer', 'new@example.com', 'EU-West', 'bronze')
    AS t(customer_id, name, email, region, tier)
""")

# Exercise 9 source: has extra column for schema evolution
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.merge_ex9_source AS
    SELECT order_id, customer_id, product_id, 109.50 AS amount, 'shipped' AS status,
           order_date, TIMESTAMP '2026-03-01 10:00:00' AS updated_at,
           10.0 AS discount_pct
    FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id = 'ORD-001'
    UNION ALL
    SELECT * FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00', 5.0)
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at, discount_pct)
""")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
