# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Liquid Clustering
# MAGIC **Topic**: Delta Lake | **Exercises**: 6 | **Checkpoints**: 1 | **Total Time**: ~70 min
# MAGIC
# MAGIC Practice the recommended data layout strategy for Delta tables. Liquid clustering
# MAGIC replaces ZORDER and manual partitioning with automatic, adaptive data organization
# MAGIC that doesn't require you to pick partition boundaries upfront.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/liquid-clustering-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.delta_lake.orders` - order records
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Primary key |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/liquid-clustering-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise tables are in `{CATALOG}.{SCHEMA}` (liquid_clustering schema).
# MAGIC Base tables (orders, customers) are in `{CATALOG}.{BASE_SCHEMA}` (delta_lake schema).
# MAGIC - Exercises 1, 6: no pre-created tables (you create them from scratch using base orders)
# MAGIC - `lc_ex2_orders`: fragmented clustered table (for OPTIMIZE)
# MAGIC - `lc_ex3_orders`: clustered + optimized (for inspection)
# MAGIC - `lc_ex4_orders`: unpartitioned, previously ZORDER'd (for migration)
# MAGIC - `lc_ex5_orders`: fragmented, clustered by `status` (for metadata-only key change demo)
# MAGIC - `lc_ex7_partitioned`: partitioned by `status` (for full migration)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Create a Liquid Clustered Table
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Create a new Delta table with liquid clustering enabled on the `status` column.
# MAGIC Copy data from the base orders table.
# MAGIC
# MAGIC **Expected Output**: `lc_ex1_orders` exists with 5 rows and `CLUSTER BY (status)`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `lc_ex1_orders` with `CLUSTER BY (status)`
# MAGIC 2. Copy rows where `order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')` from base orders

# COMMAND ----------

# EXERCISE_KEY: lc_ex1
# TODO: Create a liquid clustered table with CLUSTER BY (status)

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex1_orders")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex1_orders").collect()[0]
clustering = detail.clusteringColumns
assert clustering is not None and len(clustering) > 0, "Table should have clustering columns"
assert "status" in clustering, f"Should be clustered by status, got {clustering}"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Trigger Clustering with OPTIMIZE
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Liquid clustering is applied when you run OPTIMIZE. The setup created a fragmented
# MAGIC clustered table with 10+ small files. Run OPTIMIZE to compact and cluster the data.
# MAGIC
# MAGIC **Table** (`lc_ex2_orders`): ~20 rows across 10+ files, CLUSTER BY (status).
# MAGIC
# MAGIC **Expected Output**: After OPTIMIZE, the table has 1-2 files (compacted and clustered).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `OPTIMIZE` on `lc_ex2_orders`

# COMMAND ----------

# EXERCISE_KEY: lc_ex2
# TODO: Run OPTIMIZE to trigger liquid clustering

# Your code here


# COMMAND ----------

# Validate Exercise 2
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex2_orders").collect()[0]

assert detail.numFiles <= 2, f"After OPTIMIZE, expected 1-2 files, got {detail.numFiles}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checkpoint 3: Verify Clustering Configuration
# MAGIC **Time**: ~5 min
# MAGIC
# MAGIC Inspect a clustered table's configuration using `DESCRIBE DETAIL`. The output
# MAGIC includes a `clusteringColumns` field showing which columns are used for clustering.
# MAGIC
# MAGIC **Table** (`lc_ex3_orders`): clustered by `status`, already optimized.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` on `lc_ex3_orders`
# MAGIC 2. Find the `clusteringColumns` value and the `numFiles` count
# MAGIC 3. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: lc_ex3
# TODO: Run DESCRIBE DETAIL on lc_ex3_orders, then fill in what you observe

clustering_col = ""    # Replace: the column name from clusteringColumns (e.g., "status")
num_files = 0          # Replace: numFiles after optimization

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex3_report AS
    SELECT '{clustering_col}' AS clustering_col, CAST({num_files} AS LONG) AS num_files
""")

# COMMAND ----------

# Validate Exercise 3
row = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex3_report").collect()[0]

assert row.clustering_col == "status", f"Clustering column should be 'status', got '{row.clustering_col}'"
assert row.num_files >= 1, f"Should have at least 1 file, got {row.num_files}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Migrate from ZORDER to Liquid Clustering
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC This table was previously maintained with `OPTIMIZE ZORDER BY (status)`.
# MAGIC Migrate it to liquid clustering, which is the recommended replacement.
# MAGIC
# MAGIC **Table** (`lc_ex4_orders`): 5 rows, unpartitioned, previously ZORDER'd by status.
# MAGIC
# MAGIC **Expected Output**: `lc_ex4_orders` has `CLUSTER BY (status)` enabled. Future
# MAGIC OPTIMIZE calls will use liquid clustering instead of ZORDER.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `ALTER TABLE` to enable liquid clustering on the `status` column
# MAGIC
# MAGIC **Approach**: ALTER TABLE supports adding CLUSTER BY to existing unpartitioned tables.

# COMMAND ----------

# EXERCISE_KEY: lc_ex4
# TODO: Add liquid clustering to replace ZORDER

# Your code here


# COMMAND ----------

# Validate Exercise 4
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex4_orders").collect()[0]
clustering = detail.clusteringColumns

assert clustering is not None and len(clustering) > 0, "Table should have clustering columns after migration"
assert "status" in clustering, f"Should be clustered by status, got {clustering}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Change Cluster Keys (Metadata-Only)
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Change the clustering key from `status` to `order_date`. A key advantage of liquid
# MAGIC clustering over partitioning: changing keys is a **metadata-only operation** - no files
# MAGIC are rewritten until the next OPTIMIZE.
# MAGIC
# MAGIC **Table** (`lc_ex5_orders`): ~20 rows across 10+ files, currently `CLUSTER BY (status)`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Run `DESCRIBE DETAIL` and note the current `numFiles`
# MAGIC 2. Use `ALTER TABLE` to change the clustering key to `order_date`
# MAGIC 3. Run `DESCRIBE DETAIL` again - notice `numFiles` is unchanged (metadata-only!)
# MAGIC 4. Run `OPTIMIZE` to actually re-cluster the data with the new key
# MAGIC 5. Fill in the placeholder values below

# COMMAND ----------

# EXERCISE_KEY: lc_ex5
# TODO: Check files, change key, check files again (same!), OPTIMIZE, check files (compacted!)

files_before_alter = 0   # Replace: numFiles before ALTER TABLE
# Write your ALTER TABLE CLUSTER BY here

files_after_alter = 0    # Replace: numFiles after ALTER (should equal files_before_alter!)
# Write your OPTIMIZE here

files_after_optimize = 0 # Replace: numFiles after OPTIMIZE

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.lc_ex5_proof AS
    SELECT CAST({files_before_alter} AS LONG) AS files_before_alter,
           CAST({files_after_alter} AS LONG) AS files_after_alter,
           CAST({files_after_optimize} AS LONG) AS files_after_optimize
""")

# COMMAND ----------

# Validate Exercise 5
detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex5_orders").collect()[0]
assert "order_date" in detail.clusteringColumns, \
    f"Should be clustered by order_date, got {detail.clusteringColumns}"

proof = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex5_proof").collect()[0]
assert proof.files_before_alter >= 8, \
    f"Should start with 8+ files, got {proof.files_before_alter}"
assert proof.files_before_alter == proof.files_after_alter, \
    f"ALTER is metadata-only: files should be unchanged ({proof.files_before_alter} vs {proof.files_after_alter})"
assert proof.files_after_optimize <= 2, \
    f"After OPTIMIZE, should have 1-2 files, got {proof.files_after_optimize}"

print("Exercise 5 passed! ALTER TABLE was metadata-only, OPTIMIZE did the actual rewrite.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Column Cluster Keys
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Create a table clustered on multiple columns. Liquid clustering supports up to 4
# MAGIC clustering columns, which co-locate data for queries filtering on any combination.
# MAGIC
# MAGIC **Expected Output**: `lc_ex6_orders` exists with 5 rows and `CLUSTER BY (status, customer_id)`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `lc_ex6_orders` with `CLUSTER BY (status, customer_id)`
# MAGIC 2. Copy rows where `order_id IN ('ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005')` from base orders

# COMMAND ----------

# EXERCISE_KEY: lc_ex6
# TODO: Create a table with multi-column liquid clustering

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex6_orders")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex6_orders").collect()[0]
clustering = detail.clusteringColumns
assert "status" in clustering, f"Should include status in clustering, got {clustering}"
assert "customer_id" in clustering, f"Should include customer_id in clustering, got {clustering}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Migrate Partitioned Table to Liquid Clustering
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC A table is currently `PARTITIONED BY (status)`. You can't add CLUSTER BY to a
# MAGIC partitioned table directly - you need to recreate it. Migrate the data to a new
# MAGIC liquid clustered table.
# MAGIC
# MAGIC **Source** (`lc_ex7_partitioned`): 5 rows, PARTITIONED BY (status).
# MAGIC
# MAGIC **Expected Output**: `lc_ex7_clustered` exists with same 5 rows, CLUSTER BY (status),
# MAGIC and no partitioning.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Create `lc_ex7_clustered` with `CLUSTER BY (status)` (no partitioning)
# MAGIC 2. Copy all data from `lc_ex7_partitioned`
# MAGIC 3. Run `OPTIMIZE` on the new table
# MAGIC
# MAGIC **Approach**: You can't ALTER a partitioned table to add CLUSTER BY. The migration
# MAGIC path is: create new table with CLUSTER BY, INSERT SELECT from old, OPTIMIZE.

# COMMAND ----------

# EXERCISE_KEY: lc_ex7
# TODO: Create new clustered table, copy data from partitioned, OPTIMIZE

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.lc_ex7_clustered")
assert result.count() == 5, f"Expected 5 rows, got {result.count()}"

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.lc_ex7_clustered").collect()[0]
clustering = detail.clusteringColumns
assert "status" in clustering, f"Should be clustered by status, got {clustering}"
# Verify no partitioning
assert len(detail.partitionColumns) == 0, \
    f"Should not be partitioned, got partitionColumns={detail.partitionColumns}"

print("Exercise 7 passed!")
