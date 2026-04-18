# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Window Functions
# MAGIC Creates per-exercise source tables for window function exercises. Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "window_functions"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Exercise 1: Orders with deliberate duplicates for ROW_NUMBER dedup
# 12 rows: 7 customers, CUST-001 has 3 orders, CUST-002 has 2 orders, CUST-003 has 2 orders
# One row has NULL customer_id (edge case: should be excluded)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex1_orders AS
    SELECT * FROM VALUES
        ('ORD-001', 'CUST-001', 'PROD-001', CAST(120.00 AS DOUBLE), 'completed', DATE '2026-01-15', TIMESTAMP '2026-01-15 10:00:00'),
        ('ORD-006', 'CUST-001', 'PROD-003', CAST(85.50 AS DOUBLE), 'completed', DATE '2026-02-10', TIMESTAMP '2026-02-10 14:00:00'),
        ('ORD-011', 'CUST-001', 'PROD-002', CAST(200.00 AS DOUBLE), 'shipped', DATE '2026-03-05', TIMESTAMP '2026-03-05 09:00:00'),
        ('ORD-002', 'CUST-002', 'PROD-002', CAST(75.00 AS DOUBLE), 'completed', DATE '2026-01-20', TIMESTAMP '2026-01-20 11:00:00'),
        ('ORD-007', 'CUST-002', 'PROD-004', CAST(150.00 AS DOUBLE), 'shipped', DATE '2026-02-15', TIMESTAMP '2026-02-15 16:00:00'),
        ('ORD-003', 'CUST-003', 'PROD-005', CAST(45.00 AS DOUBLE), 'pending', DATE '2026-01-25', TIMESTAMP '2026-01-25 08:00:00'),
        ('ORD-010', 'CUST-003', 'PROD-001', CAST(90.00 AS DOUBLE), 'completed', DATE '2026-02-28', TIMESTAMP '2026-02-28 11:00:00'),
        ('ORD-004', 'CUST-004', 'PROD-001', CAST(300.00 AS DOUBLE), 'completed', DATE '2026-01-30', TIMESTAMP '2026-01-30 12:00:00'),
        ('ORD-005', 'CUST-005', 'PROD-003', CAST(60.00 AS DOUBLE), 'cancelled', DATE '2026-02-01', TIMESTAMP '2026-02-01 10:00:00'),
        ('ORD-008', 'CUST-006', 'PROD-002', CAST(0.00 AS DOUBLE), 'completed', DATE '2026-02-20', TIMESTAMP '2026-02-20 13:00:00'),
        ('ORD-012', 'CUST-007', 'PROD-004', CAST(175.00 AS DOUBLE), 'shipped', DATE '2026-03-01', TIMESTAMP '2026-03-01 09:30:00'),
        ('ORD-009', NULL, 'PROD-004', CAST(95.00 AS DOUBLE), 'pending', DATE '2026-02-25', TIMESTAMP '2026-02-25 15:00:00')
    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
""")

# COMMAND ----------

# Exercise 2: Orders per customer for RANK vs DENSE_RANK comparison
# 6 customers with varying order counts. Ties in order count create rank gaps.
# CUST-001: 5 orders, CUST-002: 4 orders, CUST-003: 4 orders (tie), CUST-004: 3 orders,
# CUST-005: 3 orders (tie), CUST-006: 1 order
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex2_orders AS
    SELECT * FROM VALUES
        ('ORD-201', 'CUST-001', CAST(100.00 AS DOUBLE), DATE '2026-01-01'),
        ('ORD-202', 'CUST-001', CAST(150.00 AS DOUBLE), DATE '2026-01-15'),
        ('ORD-203', 'CUST-001', CAST(200.00 AS DOUBLE), DATE '2026-02-01'),
        ('ORD-204', 'CUST-001', CAST(80.00 AS DOUBLE), DATE '2026-02-15'),
        ('ORD-205', 'CUST-001', CAST(120.00 AS DOUBLE), DATE '2026-03-01'),
        ('ORD-206', 'CUST-002', CAST(90.00 AS DOUBLE), DATE '2026-01-05'),
        ('ORD-207', 'CUST-002', CAST(110.00 AS DOUBLE), DATE '2026-01-20'),
        ('ORD-208', 'CUST-002', CAST(75.00 AS DOUBLE), DATE '2026-02-10'),
        ('ORD-209', 'CUST-002', CAST(130.00 AS DOUBLE), DATE '2026-03-05'),
        ('ORD-210', 'CUST-003', CAST(200.00 AS DOUBLE), DATE '2026-01-10'),
        ('ORD-211', 'CUST-003', CAST(60.00 AS DOUBLE), DATE '2026-01-25'),
        ('ORD-212', 'CUST-003', CAST(140.00 AS DOUBLE), DATE '2026-02-15'),
        ('ORD-213', 'CUST-003', CAST(95.00 AS DOUBLE), DATE '2026-03-10'),
        ('ORD-214', 'CUST-004', CAST(180.00 AS DOUBLE), DATE '2026-01-12'),
        ('ORD-215', 'CUST-004', CAST(50.00 AS DOUBLE), DATE '2026-02-20'),
        ('ORD-216', 'CUST-004', CAST(220.00 AS DOUBLE), DATE '2026-03-15'),
        ('ORD-217', 'CUST-005', CAST(160.00 AS DOUBLE), DATE '2026-01-18'),
        ('ORD-218', 'CUST-005', CAST(0.00 AS DOUBLE), DATE '2026-02-25'),
        ('ORD-219', 'CUST-005', CAST(85.00 AS DOUBLE), DATE '2026-03-20'),
        ('ORD-220', 'CUST-006', CAST(300.00 AS DOUBLE), DATE '2026-02-01')
    AS t(order_id, customer_id, amount, order_date)
""")

# COMMAND ----------

# Exercise 3: Orders per customer for LAG period-over-period comparison
# 12 rows across 3 customers with 4 orders each
# Includes a $0 amount (edge case for amount change)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex3_orders AS
    SELECT * FROM VALUES
        ('ORD-301', 'CUST-001', CAST(100.00 AS DOUBLE), DATE '2026-01-10'),
        ('ORD-302', 'CUST-001', CAST(150.00 AS DOUBLE), DATE '2026-02-10'),
        ('ORD-303', 'CUST-001', CAST(120.00 AS DOUBLE), DATE '2026-03-10'),
        ('ORD-304', 'CUST-001', CAST(200.00 AS DOUBLE), DATE '2026-04-10'),
        ('ORD-305', 'CUST-002', CAST(80.00 AS DOUBLE), DATE '2026-01-15'),
        ('ORD-306', 'CUST-002', CAST(0.00 AS DOUBLE), DATE '2026-02-15'),
        ('ORD-307', 'CUST-002', CAST(60.00 AS DOUBLE), DATE '2026-03-15'),
        ('ORD-308', 'CUST-002', CAST(90.00 AS DOUBLE), DATE '2026-04-15'),
        ('ORD-309', 'CUST-003', CAST(200.00 AS DOUBLE), DATE '2026-01-20'),
        ('ORD-310', 'CUST-003', CAST(175.00 AS DOUBLE), DATE '2026-02-20'),
        ('ORD-311', 'CUST-003', CAST(250.00 AS DOUBLE), DATE '2026-03-20'),
        ('ORD-312', 'CUST-003', CAST(225.00 AS DOUBLE), DATE '2026-04-20')
    AS t(order_id, customer_id, amount, order_date)
""")

# COMMAND ----------

# Exercise 4: Orders for running totals with explicit frame specification
# 10 rows across 3 customers - CUST-001: 3, CUST-002: 4, CUST-003: 3
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex4_orders AS
    SELECT * FROM VALUES
        ('ORD-401', 'CUST-001', CAST(100.00 AS DOUBLE), DATE '2026-01-10'),
        ('ORD-402', 'CUST-001', CAST(150.00 AS DOUBLE), DATE '2026-02-10'),
        ('ORD-403', 'CUST-001', CAST(200.00 AS DOUBLE), DATE '2026-03-10'),
        ('ORD-404', 'CUST-002', CAST(80.00 AS DOUBLE), DATE '2026-01-15'),
        ('ORD-405', 'CUST-002', CAST(120.00 AS DOUBLE), DATE '2026-02-15'),
        ('ORD-406', 'CUST-002', CAST(60.00 AS DOUBLE), DATE '2026-03-15'),
        ('ORD-407', 'CUST-002', CAST(90.00 AS DOUBLE), DATE '2026-04-15'),
        ('ORD-408', 'CUST-003', CAST(200.00 AS DOUBLE), DATE '2026-01-20'),
        ('ORD-409', 'CUST-003', CAST(175.00 AS DOUBLE), DATE '2026-02-20'),
        ('ORD-410', 'CUST-003', CAST(250.00 AS DOUBLE), DATE '2026-03-20')
    AS t(order_id, customer_id, amount, order_date)
""")

# COMMAND ----------

# Exercise 5: Customer spend totals for NTILE quartile bucketing
# 8 customers with varying total spend for clear quartile boundaries (2 per quartile)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex5_customer_spend AS
    SELECT * FROM VALUES
        ('CUST-001', CAST(1200.00 AS DOUBLE)),
        ('CUST-002', CAST(950.00 AS DOUBLE)),
        ('CUST-003', CAST(3500.00 AS DOUBLE)),
        ('CUST-004', CAST(450.00 AS DOUBLE)),
        ('CUST-005', CAST(2800.00 AS DOUBLE)),
        ('CUST-006', CAST(150.00 AS DOUBLE)),
        ('CUST-007', CAST(5000.00 AS DOUBLE)),
        ('CUST-008', CAST(75.00 AS DOUBLE))
    AS t(customer_id, total_spend)
""")

# COMMAND ----------

# Exercise 6: Orders for FIRST_VALUE/LAST_VALUE per customer
# 15 rows across 4 customers - enough depth to test FIRST_VALUE and LAST_VALUE
# CUST-001: 5 orders, CUST-002: 4 orders, CUST-003: 4 orders, CUST-004: 2 orders
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex6_orders AS
    SELECT * FROM VALUES
        ('ORD-601', 'CUST-001', CAST(100.00 AS DOUBLE), 'completed', DATE '2026-01-05'),
        ('ORD-602', 'CUST-001', CAST(250.00 AS DOUBLE), 'completed', DATE '2026-02-10'),
        ('ORD-603', 'CUST-001', CAST(175.00 AS DOUBLE), 'shipped', DATE '2026-03-15'),
        ('ORD-604', 'CUST-001', CAST(300.00 AS DOUBLE), 'completed', DATE '2026-04-20'),
        ('ORD-605', 'CUST-001', CAST(50.00 AS DOUBLE), 'pending', DATE '2026-05-25'),
        ('ORD-606', 'CUST-002', CAST(80.00 AS DOUBLE), 'completed', DATE '2026-01-12'),
        ('ORD-607', 'CUST-002', CAST(0.00 AS DOUBLE), 'cancelled', DATE '2026-02-18'),
        ('ORD-608', 'CUST-002', CAST(190.00 AS DOUBLE), 'shipped', DATE '2026-03-22'),
        ('ORD-609', 'CUST-002', CAST(140.00 AS DOUBLE), 'completed', DATE '2026-04-28'),
        ('ORD-610', 'CUST-003', CAST(400.00 AS DOUBLE), 'completed', DATE '2026-01-08'),
        ('ORD-611', 'CUST-003', CAST(125.00 AS DOUBLE), 'completed', DATE '2026-02-14'),
        ('ORD-612', 'CUST-003', CAST(275.00 AS DOUBLE), 'shipped', DATE '2026-03-19'),
        ('ORD-613', 'CUST-003', CAST(350.00 AS DOUBLE), 'pending', DATE '2026-04-25'),
        ('ORD-614', 'CUST-004', CAST(500.00 AS DOUBLE), 'completed', DATE '2026-01-20'),
        ('ORD-615', 'CUST-004', CAST(220.00 AS DOUBLE), 'shipped', DATE '2026-03-30')
    AS t(order_id, customer_id, amount, status, order_date)
""")

# COMMAND ----------

# Exercise 7: Orders for QUALIFY top-N per group
# 12 rows across 3 customers (4 each) - find top 2 highest-value per customer
# CUST-001 amounts: 100, 250, 175, 300 -> top 2: 300, 250
# CUST-002 amounts: 80, 0, 190, 140 -> top 2: 190, 140
# CUST-003 amounts: 400, 125, 275, 350 -> top 2: 400, 350
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex7_orders AS
    SELECT * FROM VALUES
        ('ORD-701', 'CUST-001', CAST(100.00 AS DOUBLE), DATE '2026-01-05'),
        ('ORD-702', 'CUST-001', CAST(250.00 AS DOUBLE), DATE '2026-02-10'),
        ('ORD-703', 'CUST-001', CAST(175.00 AS DOUBLE), DATE '2026-03-15'),
        ('ORD-704', 'CUST-001', CAST(300.00 AS DOUBLE), DATE '2026-04-20'),
        ('ORD-705', 'CUST-002', CAST(80.00 AS DOUBLE), DATE '2026-01-12'),
        ('ORD-706', 'CUST-002', CAST(0.00 AS DOUBLE), DATE '2026-02-18'),
        ('ORD-707', 'CUST-002', CAST(190.00 AS DOUBLE), DATE '2026-03-22'),
        ('ORD-708', 'CUST-002', CAST(140.00 AS DOUBLE), DATE '2026-04-28'),
        ('ORD-709', 'CUST-003', CAST(400.00 AS DOUBLE), DATE '2026-01-08'),
        ('ORD-710', 'CUST-003', CAST(125.00 AS DOUBLE), DATE '2026-02-14'),
        ('ORD-711', 'CUST-003', CAST(275.00 AS DOUBLE), DATE '2026-03-19'),
        ('ORD-712', 'CUST-003', CAST(350.00 AS DOUBLE), DATE '2026-04-25')
    AS t(order_id, customer_id, amount, order_date)
""")

# COMMAND ----------

# Exercise 8: Events for sessionization (gap-and-island pattern)
# 15 events across 2 users with clear session boundaries (>30 min gaps)
# USER-001: 9 events forming 3 sessions (gap between event 4-5 is 45 min, gap between 7-8 is 60 min)
# USER-002: 6 events forming 2 sessions (gap between event 4-5 is 35 min)
# Includes NULL payload (edge case)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex8_events AS
    SELECT * FROM VALUES
        ('EVT-001', 'USER-001', 'page_view', TIMESTAMP '2026-03-01 09:00:00', 'home'),
        ('EVT-002', 'USER-001', 'click', TIMESTAMP '2026-03-01 09:05:00', 'products'),
        ('EVT-003', 'USER-001', 'page_view', TIMESTAMP '2026-03-01 09:10:00', 'details'),
        ('EVT-004', 'USER-001', 'purchase', TIMESTAMP '2026-03-01 09:20:00', NULL),
        ('EVT-005', 'USER-001', 'page_view', TIMESTAMP '2026-03-01 10:05:00', 'home'),
        ('EVT-006', 'USER-001', 'click', TIMESTAMP '2026-03-01 10:15:00', 'search'),
        ('EVT-007', 'USER-001', 'page_view', TIMESTAMP '2026-03-01 10:25:00', 'results'),
        ('EVT-008', 'USER-001', 'page_view', TIMESTAMP '2026-03-01 11:25:00', 'home'),
        ('EVT-009', 'USER-001', 'click', TIMESTAMP '2026-03-01 11:30:00', 'cart'),
        ('EVT-010', 'USER-002', 'page_view', TIMESTAMP '2026-03-01 14:00:00', 'home'),
        ('EVT-011', 'USER-002', 'click', TIMESTAMP '2026-03-01 14:10:00', 'products'),
        ('EVT-012', 'USER-002', 'page_view', TIMESTAMP '2026-03-01 14:20:00', 'details'),
        ('EVT-013', 'USER-002', 'purchase', TIMESTAMP '2026-03-01 14:25:00', NULL),
        ('EVT-014', 'USER-002', 'page_view', TIMESTAMP '2026-03-01 15:00:00', 'home'),
        ('EVT-015', 'USER-002', 'click', TIMESTAMP '2026-03-01 15:10:00', 'search')
    AS t(event_id, user_id, event_type, event_ts, payload)
""")

# COMMAND ----------

# Drop output tables from previous runs (idempotent cleanup)
for tbl in [
    "window_ex1_deduped",
    "window_ex2_ranked",
    "window_ex3_changes",
    "window_ex4_running_totals",
    "window_ex5_quartiles",
    "window_ex6_first_last",
    "window_ex7_top2",
    "window_ex8_sessions",
]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

print(f"Window Functions setup complete. Exercise tables created in {CATALOG}.{SCHEMA}.")
print(f"  - window_ex1_orders: 12 rows (ROW_NUMBER dedup exercise)")
print(f"  - window_ex2_orders: 20 rows (RANK vs DENSE_RANK exercise)")
print(f"  - window_ex3_orders: 12 rows (LAG exercise)")
print(f"  - window_ex4_orders: 10 rows (running totals exercise)")
print(f"  - window_ex5_customer_spend: 8 rows (NTILE exercise)")
print(f"  - window_ex6_orders: 15 rows (FIRST_VALUE/LAST_VALUE exercise)")
print(f"  - window_ex7_orders: 12 rows (QUALIFY top-N exercise)")
print(f"  - window_ex8_events: 15 rows (sessionization exercise)")
