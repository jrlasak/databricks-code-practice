# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Time Travel & Restore
# MAGIC Creates per-exercise tables with version history. Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "time_travel"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"
# Every tt_ex*_orders table is dropped and re-created, so each run starts a fresh history at version 0.

# Output tables are created only by the exercise cells: drop them so an unsolved exercise fails its check on every run
for _t in ["tt_ex1_result", "tt_ex2_result", "tt_ex3_history", "tt_ex5_changes", "tt_ex6_audit", "tt_ex8_report"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_t}")

# Exercise 1: Query by Version Number
# v0: 5 rows from base
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex1_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex1_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# UPDATE 2 orders
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex1_orders
    SET status = 'shipped', amount = amount + 10
    WHERE order_id IN ('ORD-001', 'ORD-002')
""")
# DELETE 1 order
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.tt_ex1_orders
    WHERE order_id = 'ORD-005'
""")

# Exercise 2: Query by Timestamp
# v0: 5 rows
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex2_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex2_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# UPDATE ORD-003
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex2_orders
    SET status = 'shipped', amount = 200.00
    WHERE order_id = 'ORD-003'
""")
# DELETE ORD-003
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.tt_ex2_orders
    WHERE order_id = 'ORD-003'
""")

# Checkpoint 3: DESCRIBE HISTORY
# v0: 5 rows
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex3_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex3_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# UPDATE
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex3_orders
    SET status = 'shipped'
    WHERE order_id = 'ORD-001'
""")
# DELETE
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.tt_ex3_orders
    WHERE order_id = 'ORD-005'
""")
# INSERT
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.tt_ex3_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")
# Record actual version numbers
_hist3 = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex3_orders").orderBy("version").collect()
TT_EX3_TOTAL_VERSIONS = len(_hist3)
TT_EX3_LATEST_OPERATION = _hist3[-1].operation  # Operation of the highest version (checked by the Checkpoint 3 validate cell)
TT_EX3_DML_COUNT = sum(1 for r in _hist3 if r.operation != 'OPTIMIZE')

# COMMAND ----------

# Exercise 4: RESTORE Table
# v0: 5 rows, good data
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex4_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex4_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Bad update - all amounts zeroed out
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex4_orders
    SET amount = 0.00
""")

# Exercise 5: Diff Two Versions
# v0: 5 rows
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex5_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex5_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Update ORD-001
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex5_orders
    SET amount = amount + 50, status = 'shipped'
    WHERE order_id = 'ORD-001'
""")
# Delete ORD-005
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.tt_ex5_orders
    WHERE order_id = 'ORD-005'
""")
# Insert ORD-101
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.tt_ex5_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 49.99, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")

# Exercise 6: Audit Trail
# v0: 5 rows, all amounts < 500
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex6_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex6_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Normal update
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex6_orders
    SET status = 'shipped'
    WHERE order_id = 'ORD-001'
""")
# Bad insert - fraudulent order with abnormal amount
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.tt_ex6_orders VALUES
    ('ORD-999', 'CUST-099', 'PROD-001', 9999.99, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")
# Normal update
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex6_orders
    SET status = 'completed'
    WHERE order_id = 'ORD-002'
""")
# Version that introduced the order with amount > 1000 (checked by the Exercise 6 validate cell)
_hist6 = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex6_orders").orderBy("version").collect()
TT_EX6_BAD_VERSION = None
for r in _hist6:
    if r.operation == 'WRITE':
        _check = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.tt_ex6_orders VERSION AS OF {r.version} WHERE order_id = 'ORD-999'").collect()[0][0]
        if _check > 0:
            TT_EX6_BAD_VERSION = r.version
            break

# COMMAND ----------

# Exercise 7: Selective Undo via MERGE + Time Travel
# v0: 5 rows
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex7_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex7_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Bad update - cancels ORD-003 and ORD-005 (amount < 100)
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex7_orders
    SET status = 'cancelled'
    WHERE amount < 100
""")
# Valid change after the bad update: ORD-001 shipped (must survive the undo)
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex7_orders
    SET status = 'shipped'
    WHERE order_id = 'ORD-001'
""")

# Exercise 8: Reproducible Reporting
# v0: 5 rows
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.tt_ex8_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.tt_ex8_orders
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Insert 3 known orders (simulates end-of-day state)
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.tt_ex8_orders VALUES
    ('ORD-101', 'CUST-010', 'PROD-005', 250.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
    ('ORD-102', 'CUST-011', 'PROD-003', 175.00, 'completed', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00'),
    ('ORD-103', 'CUST-012', 'PROD-001', 75.00, 'pending', DATE '2026-03-01', TIMESTAMP '2026-03-01 10:00:00')
""")
# Bad bulk update corrupts some amounts (multiplied by 100)
spark.sql(f"""
    UPDATE {CATALOG}.{SCHEMA}.tt_ex8_orders
    SET amount = amount * 100
    WHERE order_id IN ('ORD-101', 'ORD-102')
""")
# Record actual version of the INSERT (last known-good state)
_hist8 = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.tt_ex8_orders").orderBy("version").collect()
TT_EX8_INSERT_V = next(r.version for r in _hist8 if r.operation == 'WRITE')

# COMMAND ----------

print("Setup complete. All exercise tables created with version history.")
