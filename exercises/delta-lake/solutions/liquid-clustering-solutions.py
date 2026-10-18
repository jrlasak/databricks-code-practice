# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Liquid Clustering - Solutions
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 1
# MAGIC
# MAGIC Reference solutions, hints and common mistakes for each exercise in `05_liquid-clustering.py`.
# MAGIC Try each exercise before you read its section here.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Create a Liquid Clustered Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Liquid clustering replaces both partitioning (`PARTITIONED BY`) and `ZORDER`
# MAGIC 2. Add `CLUSTER BY (column)` between the table name and `AS SELECT`
# MAGIC 3. The syntax is: `CREATE TABLE name CLUSTER BY (col) AS SELECT ...`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `PARTITIONED BY` instead of `CLUSTER BY` (partitioning is a different layout, and a table cannot have both)
# MAGIC - Putting CLUSTER BY after AS SELECT (must come before)

# COMMAND ----------

# EXERCISE_KEY: lc_ex1
CATALOG = "db_code"
SCHEMA = "liquid_clustering"
BASE_SCHEMA = "delta_lake"

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex1_orders
    CLUSTER BY (status)
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Trigger Clustering with OPTIMIZE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same OPTIMIZE syntax as regular compaction
# MAGIC 2. No ZORDER BY clause: OPTIMIZE reads the clustering keys from the table definition
# MAGIC 3. OPTIMIZE both compacts files AND applies clustering layout
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Adding `ZORDER BY` to OPTIMIZE on a liquid clustered table (fails: `ZORDER BY` can't be used on tables that use liquid clustering)
# MAGIC - Expecting small writes to be clustered without running OPTIMIZE (only writes above a size threshold cluster on write)

# COMMAND ----------

# EXERCISE_KEY: lc_ex2
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex2_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: Verify Clustering Configuration
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` and look for the `clusteringColumns` column
# MAGIC 2. It's an array of column names used for clustering
# MAGIC 3. Also check `numFiles` to see the result of optimization
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing `partitionColumns` with `clusteringColumns` (different arrays)

# COMMAND ----------

# EXERCISE_KEY: lc_ex3
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex3_orders").collect()[0]
clustering_col = detail.clusteringColumns[0]
num_files = detail.numFiles

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex3_report AS
    SELECT '{clustering_col}' AS clustering_col, CAST({num_files} AS LONG) AS num_files
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Migrate from ZORDER to Liquid Clustering
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. You can add liquid clustering in place to an unpartitioned Delta table like this one. A partitioned table needs `ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY` (Databricks Runtime 18.1+) or a copy into a new table (Exercise 7)
# MAGIC 2. `ALTER TABLE ... CLUSTER BY (column)` adds liquid clustering
# MAGIC 3. The ALTER does not cluster rows already written: run `OPTIMIZE ... FULL` once, then later OPTIMIZE runs keep new data clustered
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running plain `ALTER TABLE ... CLUSTER BY` on a partitioned table (fails; see Exercise 7)
# MAGIC - Following the ALTER with plain `OPTIMIZE` (clustering is not applied to previously written data; run `OPTIMIZE ... FULL` once)

# COMMAND ----------

# EXERCISE_KEY: lc_ex4
# Enable liquid clustering (metadata only)
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.lc_ex4_orders CLUSTER BY (status)")

# In production, follow the ALTER with one OPTIMIZE FULL: it clusters the rows already in the table
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex4_orders FULL")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Change Clustering Keys (Metadata-Only)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run `DESCRIBE DETAIL` to see `numFiles` before any changes
# MAGIC 2. `ALTER TABLE ... CLUSTER BY` is instant - it only changes metadata
# MAGIC 3. Run `DESCRIBE DETAIL` again after ALTER: `numFiles` is identical
# MAGIC 4. After a key change, `OPTIMIZE ... FULL` re-clusters every existing row; plain `OPTIMIZE` is incremental and leaves data already clustered by the old key as it is
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting files to change after ALTER (they don't until OPTIMIZE)
# MAGIC - Relying on plain `OPTIMIZE` after changing keys (it is incremental: data already clustered by `status` keeps that layout; `OPTIMIZE ... FULL` rewrites every row by `order_date`)

# COMMAND ----------

# EXERCISE_KEY: lc_ex5
# Step 1: Check files before
files_before_alter = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

# Step 2: Change clustering key (metadata-only)
spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.lc_ex5_orders CLUSTER BY (order_date)")

# Step 3: Check files after ALTER (unchanged)
files_after_alter = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

# Step 4: OPTIMIZE FULL re-clusters existing rows with the new key
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex5_orders FULL")

# Step 5: Check files after OPTIMIZE FULL - now compacted
files_after_optimize = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").select("numFiles").collect()[0][0]

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex5_proof AS
    SELECT CAST({files_before_alter} AS LONG) AS files_before_alter,
           CAST({files_after_alter} AS LONG) AS files_after_alter,
           CAST({files_after_optimize} AS LONG) AS files_after_optimize
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Column Clustering Keys
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. List multiple columns in CLUSTER BY: `CLUSTER BY (col1, col2)`
# MAGIC 2. Up to 4 clustering columns are supported
# MAGIC 3. Key order does not matter: `CLUSTER BY (status, customer_id)` and `CLUSTER BY (customer_id, status)` both pass
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using more than 4 columns (limit is 4)
# MAGIC - Clustering on a column nobody filters by (keys should be the columns your queries filter on most; high cardinality is fine)

# COMMAND ----------

# EXERCISE_KEY: lc_ex6
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex6_orders
    CLUSTER BY (status, customer_id)
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.orders
    WHERE order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Migrate Partitioned Table to Liquid Clustering
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Plain `ALTER TABLE ... CLUSTER BY` fails on a partitioned table. Databricks Runtime 18.1+ can convert it in place with `ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY`; here you build a new table instead
# MAGIC 2. `CREATE TABLE ... CLUSTER BY (status) AS SELECT * FROM db_code.liquid_clustering.lc_ex7_partitioned` creates and fills it in one statement
# MAGIC 3. Run `OPTIMIZE` on `lc_ex7_clustered` afterwards
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running plain `ALTER TABLE ... CLUSTER BY` on `lc_ex7_partitioned` (fails on a partitioned table)
# MAGIC - Forgetting to OPTIMIZE after creation (a 5-row copy is far below the cluster-on-write threshold, so nothing is clustered until OPTIMIZE)
# MAGIC - Losing data by not copying all rows (always verify row counts)

# COMMAND ----------

# EXERCISE_KEY: lc_ex7
# Step 1: Create new clustered table with data from partitioned
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex7_clustered
    CLUSTER BY (status)
    AS SELECT * FROM {CATALOG}.{SCHEMA}.lc_ex7_partitioned
""")

# Step 2: OPTIMIZE to apply clustering
spark.sql(f"OPTIMIZE {CATALOG}.{SCHEMA}.lc_ex7_clustered")
