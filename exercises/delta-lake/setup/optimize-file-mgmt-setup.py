# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: OPTIMIZE & File Management
# MAGIC Creates one table per exercise in `db_code.optimize_file_mgmt`. `opt_ex1_orders` to `opt_ex4_orders` and `opt_ex8_orders` are built from 10 two-row INSERTs, so each starts with 10 small files.
# MAGIC The exercise notebook runs this for you with `%run`.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "optimize_file_mgmt"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

def _create_fragmented_table(name, n_files=10):
    """Create a table with n_files small files: one INSERT of two orders per file.
    Dropped first so the history restarts at version 0 (CREATE OR REPLACE would keep earlier OPTIMIZE rows)."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{name}")
    spark.sql(f"CREATE TABLE {CATALOG}.{SCHEMA}.{name} TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false') AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE 1=0")
    _all_ids = [r.order_id for r in spark.sql(f"SELECT DISTINCT order_id FROM {CATALOG}.{BASE_SCHEMA}.orders ORDER BY order_id LIMIT {n_files * 2}").collect()]
    for i in range(0, len(_all_ids), 2):
        batch_ids = ",".join(f"'{x}'" for x in _all_ids[i:i+2])
        spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{name} SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id IN ({batch_ids})")

# COMMAND ----------

# Output tables are created only by the exercise cells: drop them so an unsolved exercise fails its check on every run
for _t in ["opt_ex1_detail", "opt_ex4_comparison", "opt_ex7_report", "opt_ex8_analysis"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_t}")

# Checkpoint 1 and Exercises 2-4: ORD-001 to ORD-020, inserted 2 rows at a time, so 10 small files per table
for name in ['opt_ex1_orders', 'opt_ex2_orders', 'opt_ex3_orders', 'opt_ex4_orders']:
    _create_fragmented_table(name)

# COMMAND ----------

# Exercise 5: ORD-001 to ORD-005 in one CTAS write (dropped first so no earlier retention property survives a rerun)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.opt_ex5_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.opt_ex5_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# Exercise 6: ORD-001 to ORD-005, then ORD-001 and ORD-002 updated and ORD-005 deleted, leaving files VACUUM can remove
# Dropped first so no earlier VACUUM row or retention property survives a rerun
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.opt_ex6_orders")
spark.sql(f"""
    CREATE TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Create stale files via UPDATE/DELETE operations
spark.sql(f"UPDATE {CATALOG}.{SCHEMA}.opt_ex6_orders SET status = 'shipped' WHERE order_id = 'ORD-001'")
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.opt_ex6_orders WHERE order_id = 'ORD-005'")
spark.sql(f"UPDATE {CATALOG}.{SCHEMA}.opt_ex6_orders SET amount = amount + 10 WHERE order_id = 'ORD-002'")

# Checkpoint 7: ORD-001 to ORD-005 in one CTAS write
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex7_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# Exercise 8: 10 small files like Checkpoint 1, then compacted by OPTIMIZE
_create_fragmented_table('opt_ex8_orders')
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex8_orders")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
