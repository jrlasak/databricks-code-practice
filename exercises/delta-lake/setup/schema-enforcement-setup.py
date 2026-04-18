# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Schema Enforcement & Evolution
# MAGIC Creates per-exercise target and source tables with various schema configurations.
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "schema_enforcement"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

# Target tables: copy 5 orders from base for exercises 1-5 and 10
for i in [1, 2, 3, 4, 5, 10]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex{i}_target
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
        WHERE order_id IN {BASE_IDS}
    """)

# Exercises 6-7: single tables (constraint exercises)
for i in [6, 7]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex{i}_orders
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
        WHERE order_id IN {BASE_IDS}
    """)

# COMMAND ----------

# Exercise 1 source: matching schema (3 new orders)
# CAST amount to DOUBLE to match target schema (VALUES infers DECIMAL)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex1_source AS
    SELECT order_id, customer_id, product_id, CAST(amount AS DOUBLE) AS amount, status, order_date, updated_at
    FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-102', 'CUST-011', 'PROD-003', 89.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-103', 'CUST-012', 'PROD-001', 150.00, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# Exercise 2-3 source: extra column (discount_pct)
# CAST amount to DOUBLE to match target schema (VALUES infers DECIMAL)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex2_source AS
    SELECT order_id, customer_id, product_id, CAST(amount AS DOUBLE) AS amount, status, order_date, updated_at, CAST(discount_pct AS DOUBLE) AS discount_pct
    FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00', 5.0),
        ('ORD-102', 'CUST-011', 'PROD-003', 89.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00', 10.0),
        ('ORD-103', 'CUST-012', 'PROD-001', 150.00, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00', 0.0)
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at, discount_pct)
""")
spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex3_source AS SELECT * FROM {CATALOG}.{SCHEMA}.schema_ex2_source")

# Exercise 4 source: completely different schema (products, not orders)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex4_source AS
    SELECT product_id, name AS product_name, category, price
    FROM {CATALOG}.{BASE_SCHEMA}.products
    WHERE product_id IN ('PROD-001', 'PROD-002', 'PROD-003')
""")

# COMMAND ----------

# Exercise 5 source: amount as STRING (type mismatch)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex5_source (
        order_id STRING, customer_id STRING, product_id STRING,
        amount STRING, status STRING, order_date DATE, updated_at TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.schema_ex5_source VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', '49.99', 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
    ('ORD-102', 'CUST-011', 'PROD-003', '89.00', 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
    ('ORD-103', 'CUST-012', 'PROD-001', '150.00', 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")

# Exercise 8: table with both constraints (for dropping exercise)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex8_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex8_orders ALTER COLUMN status SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex8_orders ADD CONSTRAINT positive_amount CHECK (amount > 0)")

# Exercise 9: table with multiple CHECK constraints (for information_schema query)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex9_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex9_orders ADD CONSTRAINT positive_amount CHECK (amount > 0)")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex9_orders ADD CONSTRAINT valid_status CHECK (status IN ('pending', 'completed', 'shipped', 'cancelled'))")

# Exercise 10 source: two extra columns (for MERGE schema evolution)
# CAST numeric columns to DOUBLE to match target schema
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex10_source AS
    SELECT order_id, customer_id, product_id, amount, status, order_date, updated_at,
           CAST(10.0 AS DOUBLE) AS discount_pct, CAST(5.99 AS DOUBLE) AS shipping_cost
    FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002')
    UNION ALL
    SELECT order_id, customer_id, product_id, CAST(amount AS DOUBLE) AS amount, status, order_date, updated_at, CAST(discount_pct AS DOUBLE) AS discount_pct, CAST(shipping_cost AS DOUBLE) AS shipping_cost
    FROM VALUES
        ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00', 5.0, 3.99)
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at, discount_pct, shipping_cost)
""")

# Exercise 11: table with constraints pre-applied (for violation exercise)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex11_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex11_orders ALTER COLUMN status SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex11_orders ADD CONSTRAINT positive_amount CHECK (amount > 0)")

# Exercise 12: target with constraints + source with mixed valid/invalid data
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex12_target
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex12_target ALTER COLUMN status SET NOT NULL")
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.schema_ex12_target ADD CONSTRAINT positive_amount CHECK (amount > 0)")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.schema_ex12_source AS
    SELECT order_id, customer_id, product_id, CAST(amount AS DOUBLE) AS amount, status, order_date, updated_at
    FROM VALUES
        ('ORD-001', 'CUST-001', 'PROD-001', 120.00, 'shipped', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-201', 'CUST-020', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
        ('ORD-202', 'CUST-021', 'PROD-003', -10.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
    UNION ALL
    SELECT 'ORD-203', 'CUST-022', 'PROD-001', CAST(75.00 AS DOUBLE), CAST(NULL AS STRING), DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'
""")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
