# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Liquid Clustering
# MAGIC Creates per-exercise tables for liquid clustering practice.
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "liquid_clustering"
BASE_SCHEMA = "delta_lake"

BASE_IDS = "('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')"

def _create_fragmented_clustered_table(name, cluster_col="status"):
    """Create a liquid clustered table with many small files."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{name}
        CLUSTER BY ({cluster_col})
        TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE 1=0
    """)
    _ids = [r.order_id for r in spark.sql(f"SELECT DISTINCT order_id FROM {CATALOG}.{BASE_SCHEMA}.orders ORDER BY order_id LIMIT 20").collect()]
    for i in range(0, len(_ids), 2):
        batch = ",".join(f"'{x}'" for x in _ids[i:i+2])
        spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{name} SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders WHERE order_id IN ({batch})")

# COMMAND ----------

# Exercise 2: fragmented clustered table (for OPTIMIZE)
_create_fragmented_clustered_table("lc_ex2_orders")

# Exercise 3: clustered + optimized table (for inspection)
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
