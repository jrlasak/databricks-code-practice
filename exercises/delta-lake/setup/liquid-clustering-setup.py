# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Liquid Clustering
# MAGIC Creates per-exercise tables for liquid clustering practice.
# MAGIC Runs automatically through `%run` from `05_liquid-clustering.py`; you never run it by hand.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "liquid_clustering"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

def _create_fragmented_clustered_table(name, cluster_col="status"):
    """Create an empty liquid clustered table, then insert the first 20 order_ids two per INSERT: 10 INSERTs, about 10 small files.
    Dropped first so the history restarts at version 0 (CREATE OR REPLACE would keep earlier OPTIMIZE rows)."""
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{name}")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.{SCHEMA}.{name}
        CLUSTER BY ({cluster_col})
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE 1=0
    """)
    _ids = [r.order_id for r in spark.sql(f"SELECT DISTINCT order_id FROM {CATALOG}.{BASE_SCHEMA}.orders ORDER BY order_id LIMIT 20").collect()]
    for i in range(0, len(_ids), 2):
        batch = ",".join(f"'{x}'" for x in _ids[i:i+2])
        spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{name} SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id IN ({batch})")

# COMMAND ----------

# Output tables are created only by the exercise cells: drop them so an unsolved exercise fails its check on every run
for _t in ["lc_ex1_orders", "lc_ex3_report", "lc_ex5_proof", "lc_ex6_orders", "lc_ex7_clustered"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{_t}")

# Exercise 2: fragmented clustered table (for OPTIMIZE)
_create_fragmented_clustered_table("lc_ex2_orders")

# Checkpoint 3: clustered + optimized table (for inspection)
_create_fragmented_clustered_table("lc_ex3_orders")
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex3_orders")

# Exercise 4: unpartitioned table, previously ZORDER'd (for migration)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex4_orders
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex4_orders ZORDER BY (status)")

# Exercise 5: fragmented clustered table (to show ALTER is metadata-only)
_create_fragmented_clustered_table("lc_ex5_orders")

# Exercise 7: partitioned table (for migration to clustering)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex7_partitioned (
        order_id STRING, customer_id STRING, product_id STRING,
        amount DOUBLE, status STRING, order_date DATE, updated_at TIMESTAMP
    ) USING DELTA PARTITIONED BY (status)
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.lc_ex7_partitioned
    SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN {BASE_IDS}
""")

# COMMAND ----------

print("Setup complete. All exercise tables created.")
