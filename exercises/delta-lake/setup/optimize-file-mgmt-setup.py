# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: OPTIMIZE & File Management
# MAGIC Creates per-exercise tables. Some are intentionally fragmented (many small files)
# MAGIC to demonstrate OPTIMIZE. Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "optimize_file_mgmt"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

def _create_fragmented_table(name, n_files=10):
    """Create a table with many small files by doing individual inserts."""
    spark.sql(f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{name} TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false') AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE 1=0")
    _all_ids = [r.order_id for r in spark.sql(f"SELECT DISTINCT order_id FROM {CATALOG}.{BASE_SCHEMA}.orders ORDER BY order_id LIMIT {n_files * 2}").collect()]
    for i in range(0, len(_all_ids), 2):
        batch_ids = ",".join(f"'{x}'" for x in _all_ids[i:i+2])
        spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{name} SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id IN ({batch_ids})")

# COMMAND ----------

# Exercises 1-4: fragmented tables (many small files for OPTIMIZE demos)
for name in ['opt_ex1_orders', 'opt_ex2_orders', 'opt_ex3_orders', 'opt_ex4_orders']:
    _create_fragmented_table(name)

# COMMAND ----------

# Exercise 5: normal table (for setting retention property)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex5_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# Exercise 6: table with stale files (for VACUUM demo)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex6_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
# Create stale files via UPDATE/DELETE operations
spark.sql(f"UPDATE {CATALOG}.{SCHEMA}.opt_ex6_orders SET status = 'shipped' WHERE order_id = 'ORD-001'")
spark.sql(f"DELETE FROM {CATALOG}.{SCHEMA}.opt_ex6_orders WHERE order_id = 'ORD-005'")
spark.sql(f"UPDATE {CATALOG}.{SCHEMA}.opt_ex6_orders SET amount = amount + 10 WHERE order_id = 'ORD-002'")

# Exercise 7: normal table (for DESCRIBE DETAIL report)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.opt_ex7_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# Exercise 8: fragmented table pre-optimized (for history analysis)
_create_fragmented_table('opt_ex8_orders')
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.opt_ex8_orders")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
