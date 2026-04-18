# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Optimization Project
# MAGIC
# MAGIC This notebook is a guided, hands‑on lab to explore core Delta Lake optimization techniques in Databricks. You'll iteratively: generate data → profile baseline performance → apply optimizations → observe impact in the Spark UI and table metadata.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end you should be able to:
# MAGIC - Select appropriate physical layout strategies (partitioning, Z-Ordering, Liquid Clustering)
# MAGIC - Diagnose small-file and data-skipping issues
# MAGIC - Apply manual vs automatic compaction
# MAGIC - Use VACUUM safely for storage hygiene
# MAGIC - Read/interpret Spark UI metrics (files scanned, data read, predicate pushdown, scan time)
# MAGIC
# MAGIC ## Flow Overview
# MAGIC 1. Environment + table name registry
# MAGIC 2. Data generation (idempotent)
# MAGIC 3. Comparing Partitioning, Z-Ordering, Liquid Clustering
# MAGIC   - Baseline query
# MAGIC   - Partitioning impact
# MAGIC   - Z-Ordering for multi-dimensional skipping
# MAGIC   - Liquid Clustering (adaptive layout)
# MAGIC 4. Handling small and orphaned files
# MAGIC   - Manual compaction
# MAGIC   - Auto compaction 
# MAGIC   - VACUUM lifecycle management
# MAGIC 5. Cleanup
# MAGIC
# MAGIC > Tip: After each transformation, open the Spark UI and record: files scanned, total data read MB, and wall-clock time. Building a mini table of results reinforces the concepts.
# MAGIC
# MAGIC ## Setup
# MAGIC We first define the catalog & schema (database) and then a centralized registry of logical table names to keep code DRY and readable. Ensure your workspace permissions allow catalog/schema creation.

# COMMAND ----------

# Configuration
CATALOG_NAME = "delta_optimization_project"
SCHEMA_NAME = "sales_data"

# Create the catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------

# Centralized table name registry & helper utilities
TABLES = {
    "raw": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_raw",
    "partitioned": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_partitioned",
    "raw_zorder": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_raw_zorder",
    "to_compact": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_to_compact",
    "auto_compact": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_auto_compact",
    "liquid_clustered": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_liquid_clustered",
}

def tbl(key: str) -> str:
    """Return fully qualified table name by logical key."""
    return TABLES[key]

from typing import Optional
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation: Generate Synthetic Sales Data
# MAGIC
# MAGIC We create a moderately large, semi-realistic sales fact dataset to exercise partition pruning and data skipping. Characteristics:
# MAGIC - 50M rows (adjustable) to give enough files for optimization to matter
# MAGIC - Mixed cardinalities: `country` (low), `customer_id` (medium/high), `product_id` (medium) for contrasting layout strategies
# MAGIC - Temporal column `sale_date` enabling potential future partitioning or clustering experiments
# MAGIC
# MAGIC ### Design Notes
# MAGIC - Data generation is idempotent: if the base table exists we skip regeneration (saves cluster time)
# MAGIC - We intentionally over-partition the initial write (`num_partitions` + low `maxRecordsPerFile`) to create many small files and highlight gains from compaction
# MAGIC
# MAGIC ### Your Tasks
# MAGIC 1. Run the cell.
# MAGIC 2. If generation runs, note: number of partitions written, approximate file count (later visible via `DESCRIBE DETAIL` or storage listing).
# MAGIC 3. If skipped, confirm reuse message printed.
# MAGIC
# MAGIC Proceed once the table exists.

# COMMAND ----------

# Generate synthetic data at scale using Spark (executor-friendly, no pandas/Faker)
import pyspark.sql.functions as F

# If the target table already exists, skip expensive generation and reuse it.
# This makes the notebook idempotent for iterative runs.
raw_exists = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_NAME} LIKE 'sales_raw'").count() > 0
if raw_exists:
    print(f"Table {tbl('raw')} already exists; skipping data generation.")
else:
    # Number of records to generate (adjust for your cluster)
    num_records = 50_000_000

    # Controls the number of output files (one file per output partition, roughly).
    # Increase partitions to create more small files; decrease to reduce file count.
    num_partitions = 1000

    # Small list of countries used for sampling (kept on driver only)
    countries = ['United States','United Kingdom','Germany','France','Canada','Australia','India','Brazil','Japan','Netherlands']

    # Build the DataFrame using Spark primitives (scales across executors)
    df = (
        spark.range(num_records)
        .repartition(num_partitions)
        .withColumnRenamed('id', 'seq')
        .withColumn(
            'transaction_id',
            F.concat(
                F.lit('txn_'),
                F.col('seq').cast('string'),
                F.lit('_'),
                # cast the double returned by rand() to string before md5 to avoid datatype mismatch
                F.substring(F.md5(F.rand(12345).cast('string')), 1, 8)
            )
        )
        .withColumn('customer_id', (F.floor(F.rand(42) * 1001) + 1000).cast('int'))
        .withColumn('product_id', (F.floor(F.rand(99) * 401) + 100).cast('int'))
        .withColumn('sale_date', F.date_sub(F.current_date(), F.floor(F.rand(7) * 730).cast('int')))
        .withColumn('quantity', (F.floor(F.rand(11) * 10) + 1).cast('int'))
        .withColumn('unit_price', F.round(F.rand(13) * 190.0 + F.lit(10.5), 2))
        .withColumn(
            'country',
            F.element_at(
                F.array(*[F.lit(c) for c in countries]),
                (F.floor(F.rand(21) * len(countries)) + 1).cast('int')
            )
        )
    )
    # Write the data to a Delta table. Use maxRecordsPerFile to force small files in a performant, distributed write.
    (df.write
       .format('delta')
       .option('maxRecordsPerFile', 5000)
       .mode('overwrite')
       .saveAsTable(tbl('raw'))
    )

# COMMAND ----------

df_raw = spark.read.table(tbl('raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Compare Partitioning, Z-Order, and Liquid Clustering

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Query (Unoptimized Table)
# MAGIC
# MAGIC We establish a performance baseline before altering layout. This isolates the effect of later optimizations.
# MAGIC
# MAGIC ### Query Pattern
# MAGIC Filter on a specific `(customer_id, product_id)` pair. This mimics a targeted lookup often seen in downstream analytics or service patterns.
# MAGIC
# MAGIC ### What to Capture (Create a simple log table for yourself)
# MAGIC | Metric | Value | How to Find |
# MAGIC |--------|-------|-------------|
# MAGIC | Files scanned | ? | Spark UI -> SQL/Dataframe -> Scan node |
# MAGIC | Bytes read | ? | Spark UI -> Scan details |
# MAGIC | Duration (s) | ? | Job / Stage timeline |

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to capture baseline

# COMMAND ----------

df_raw.where(
    (col("customer_id") == 1500)
    & (col("product_id") == 250)
    & (col("country") == "United States")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Open Spark UI after it finishes.
# MAGIC 2. Record metrics above.
# MAGIC 3. Keep this snapshot; you'll compare after each optimization layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 1: Partitioning
# MAGIC
# MAGIC Partitioning physically groups data into directory paths by one or more columns. Spark can then prune entire directories early when a query predicate fixes specific partition values, reducing file listing & scan work.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Efficient directory pruning for highly reused, low/moderate-cardinality filters
# MAGIC - Can bound per-query file counts for time/region sharded workloads
# MAGIC
# MAGIC ### When to Use
# MAGIC - Clear, consistent filtering patterns (e.g. date, region, category)
# MAGIC - Low cardinality columns (generally ≲ 1K distinct values across full dataset)
# MAGIC - Large tables where pruning meaningfully reduces scan
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - High-cardinality or rapidly growing distinct sets (user ids, fine-grain timestamps)
# MAGIC - Workloads without stable filter patterns
# MAGIC - Columns that mutate frequently (causes skew / small files)
# MAGIC
# MAGIC ### Trade-offs & Pitfalls
# MAGIC - Over-partitioning -> many small files + metadata overhead
# MAGIC - Too coarse partition -> limited pruning benefit
# MAGIC - Combining with Z-Order: partition first on coarse dimension (e.g. date) then Z-Order inside each partition for additional skipping on high-card columns
# MAGIC
# MAGIC We choose `country` (~10 values) for demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC Create a partitioned table

# COMMAND ----------

(df_raw.write.format("delta")
 .mode("overwrite")
 .partitionBy("country")
 .saveAsTable(tbl('partitioned')))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the same query on the partitioned table

# COMMAND ----------

(spark.read.table(tbl('partitioned'))
      .where(    
      (col("customer_id") == 1500) 
      & (col("product_id") == 250) 
      & (col("country") == 'United States')
      )
      .show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Partitioning:**
# MAGIC
# MAGIC Go to the Spark UI and compare the execution plan with the baseline. You'll notice that a filter on the partition key (`country`) was present and Spark was able to prune entire directories, significantly reducing the amount of data scanned.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 2: Z-Order
# MAGIC
# MAGIC Z-Ordering re-writes data files using a multi-dimensional space-filling curve (Z-order) so rows with similar values for selected columns co-locate. Think of it like organizing a library so related books (rows) live on the same shelf (file) — Spark can then skip whole shelves.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Improves data skipping without exploding partition counts
# MAGIC - Helps multi-column filtering patterns (e.g., customer_id AND product_id)
# MAGIC - Reduces number of files scanned for selective queries
# MAGIC
# MAGIC ### When to Use
# MAGIC - Multi-column filters in your queries
# MAGIC - Tables with 2–4 frequently queried, moderate/high-cardinality columns
# MAGIC - Data that doesn’t change constantly (batch / micro-batch growth)
# MAGIC - Need clustering on columns that would be poor physical partitions
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Single-column selective queries (simple sorting / partitioning may suffice)
# MAGIC - Very frequent small incremental writes (rewrite overhead can outweigh benefit)
# MAGIC - More than 4 columns (diminishing returns & larger shuffle)
# MAGIC - Columns with extremely high entropy (e.g. UUID) where locality gain is minimal
# MAGIC
# MAGIC ### Column Choice Tips
# MAGIC | Scenario | Better Partition | Better Z-Order |
# MAGIC |----------|------------------|----------------|
# MAGIC | Very low cardinality | Yes | Usually unnecessary |
# MAGIC | High cardinality (but queried) | No | Often yes |
# MAGIC | Evolving / mixed predicates | Limited | Strong |
# MAGIC | Need adaptive maintenance | Sometimes | Consider Liquid Clustering |
# MAGIC
# MAGIC We Z-Order on `country`, `customer_id`, `product_id` to boost skipping for the predicate used in our benchmark query.
# MAGIC
# MAGIC > Operational Hint: Re-run Z-ORDER after significant (e.g. 10–20%) new data growth in the clustered dimensions or after large backfills.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create Z-Ordered copy of raw table.

# COMMAND ----------

from delta.tables import *

spark.sql(f"DROP TABLE IF EXISTS {tbl('raw_zorder')}")

# Create a separate table from sales_raw to apply Z-Ordering to (keeps sales_partitioned untouched)
spark.sql(f"CREATE TABLE {tbl('raw_zorder')} USING DELTA AS SELECT * FROM {tbl('raw')}")

# Convert the new table to a DeltaTable object
delta_table = DeltaTable.forName(spark, tbl('raw_zorder'))

# Apply Z-Ordering on high-cardinality columns for the raw copy
delta_table.optimize().executeZOrderBy("country", "customer_id", "product_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Rerun the query to see the effect of Z-Ordering on the z-ordered raw table

# COMMAND ----------

(spark.read.table(tbl('raw_zorder'))
      .where((col("customer_id") == 1500) & (col("product_id") == 250) & (col("country") == "United States"))
      .show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Z-Ordering:**
# MAGIC
# MAGIC In the Spark UI, look at the "Details" for the scan phase. You should see a significant reduction in the number of files read, demonstrating the effectiveness of data skipping.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 3: Liquid Clustering
# MAGIC
# MAGIC Liquid Clustering dynamically reorganizes data based on query patterns, adapting to your workload without manual tuning. It decouples logical clustering from static partitions — Databricks maintains clustering as data evolves, avoiding rigid directory hierarchies while still enabling data skipping.
# MAGIC
# MAGIC ### Advantages vs Partitioning / Z-Order
# MAGIC - Adaptive: automatically reorganizes incremental data
# MAGIC - Multi-column "soft" clustering without explosion in partitions
# MAGIC - Plays well with schema evolution and changing query shapes
# MAGIC
# MAGIC ### When to Use
# MAGIC - Evolving query patterns
# MAGIC - Multiple clustering columns needed
# MAGIC - Want to replace both partitioning and Z-Ordering
# MAGIC - High churn / streaming upserts
# MAGIC - Many predicates across overlapping column sets
# MAGIC - Avoiding maintenance burden of periodic manual Z-Order runs
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Simple, stable query patterns (a single partition key may be cheaper)
# MAGIC - Tables smaller than ~1GB where overhead outweighs benefit
# MAGIC - Workloads requiring deterministic physical ordering for downstream consumers
# MAGIC
# MAGIC > Consider long-term ops: which technique minimizes recurring maintenance for your workload? Liquid Clustering can reduce hands-on maintenance for dynamic workloads but adds some runtime cost and operational monitoring requirements.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create clustered table and load data.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tbl('liquid_clustered')} (
  seq LONG,
  transaction_id STRING,
  customer_id INT,
  product_id INT,
  sale_date DATE,
  quantity INT,
  unit_price DOUBLE,
  country STRING
)
USING DELTA
CLUSTER BY (country, customer_id, product_id)
""")

# Insert data into the clustered table
df_raw.write.format("delta").mode("overwrite").saveAsTable(tbl('liquid_clustered'))

# COMMAND ----------

# MAGIC %md
# MAGIC Run predicate query; record scan metrics. Compare with Z-Order table results.

# COMMAND ----------

# Rerun the query on the liquid clustered table
(spark.read.table(tbl('liquid_clustered'))
      .where((col("customer_id") == 1500) & (col("product_id") == 250) & (col("country") == "United States"))
      .show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Liquid Clustering:**
# MAGIC
# MAGIC 1.  Run the query on the liquid clustered table.
# MAGIC 2.  Examine the Spark UI. Liquid Clustering provides similar data skipping benefits as Z-Ordering but is more adaptive to changes in your data and queries over time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note the results for comparison
# MAGIC | Metric | Raw | Partitioned | Z-order | Liquid |
# MAGIC |--------|-------|--------|--------|-------|
# MAGIC | Files scanned | ? | ? | ? | ? |
# MAGIC | Bytes read | ? | ? | ? | ? |
# MAGIC | Duration (s) | ? | ? | ? | ? |

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Handling small and orphaned files 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 1: Manual Compaction (OPTIMIZE)
# MAGIC
# MAGIC Small files inflate job overhead (task scheduling, metadata reads, open/close costs) and can limit effective predicate pushdown (more file headers to scan). `OPTIMIZE` rewrites many small files into fewer larger ones (Databricks default target ~256MB unless overridden) without changing logical content.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Reduces per-task scheduling overhead
# MAGIC - Improves scan & filter efficiency by reducing file count
# MAGIC - Prepares table for better Z-Ordering / clustering effectiveness
# MAGIC
# MAGIC ### When to Use
# MAGIC - Many small files (rule of thumb: lots of files < 100MB)
# MAGIC - Bursty or streaming ingestion producing tiny batch outputs
# MAGIC - Before heavy analytical / BI workloads to lower latency
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Files already in healthy size band (~100MB–1GB depending on workload)
# MAGIC - Ultra-low latency streaming where rewrite cost is disruptive
# MAGIC - Limited compute window / cost constraints (schedule off-peak instead)
# MAGIC
# MAGIC > Rule of thumb: Target 100–500MB average file size for large analytical tables. Monitor via `DESCRIBE DETAIL` (numFiles & sizeInBytes / numFiles).
# MAGIC
# MAGIC ### Simulation Approach
# MAGIC We append tiny random samples repeatedly to manufacture fragmentation, then run `OPTIMIZE` to compact.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Run loop to append small data.

# COMMAND ----------

# Simulate small file creation by writing in a loop
for i in range(10):
    (df_raw.sample(fraction=0.001)
     .write.format("delta")
     .mode("append")
     .saveAsTable(tbl('to_compact')))

# COMMAND ----------

# MAGIC %md
# MAGIC List number of files

# COMMAND ----------

# Check the number of files
spark.sql(f"DESCRIBE DETAIL {tbl('to_compact')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Execute `OPTIMIZE` (compaction only — no Z-order here).

# COMMAND ----------

# Now, perform manual compaction
delta_table_to_compact = DeltaTable.forName(spark, tbl('to_compact'))
delta_table_to_compact.optimize().executeCompaction()

# COMMAND ----------

# MAGIC %md
# MAGIC Re-list object count

# COMMAND ----------

# Check the number of files
spark.sql(f"DESCRIBE DETAIL {tbl('to_compact')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Compaction:**
# MAGIC
# MAGIC - Observe the reduction in the number of files after running `OPTIMIZE`. This leads to more efficient reads as Spark needs to open and process fewer files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 2: Auto Compaction
# MAGIC
# MAGIC Auto Optimize (optimizeWrite + autoCompact) reduces operational toil: it sizes files during write and schedules asynchronous compaction of lingering small files.
# MAGIC
# MAGIC ### Components
# MAGIC - `delta.autoOptimize.optimizeWrite`: coalesces output partitions at commit time
# MAGIC - `delta.autoOptimize.autoCompact`: background merge of residual small files after commit
# MAGIC
# MAGIC ### When to Use
# MAGIC - Streaming pipelines or frequent micro-batch ingestion
# MAGIC - Continuous incremental loads where manual scheduling adds complexity
# MAGIC - Teams lacking bandwidth for manual compaction orchestration
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Large, infrequent batch jobs already producing good-sized files
# MAGIC - Cost-sensitive environments (background merges add compute)
# MAGIC - Need for bespoke compaction policies / ordering (custom logic required)
# MAGIC
# MAGIC ### Caveats
# MAGIC - Background lag: immediate file count may remain high briefly
# MAGIC - Not a substitute for Z-Ordering / Liquid Clustering (does not cluster for skipping)
# MAGIC - Does not fix skew or suboptimal partition strategy
# MAGIC
# MAGIC Auto Compaction automatically triggers compaction during writes without manual intervention; you primarily verify its effect via file counts and table history (each micro-batch ideally yields a handful of well-sized files instead of many tiny ones).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create table with properties enabled.
# MAGIC

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {tbl('auto_compact')}")

spark.sql(f"""
CREATE TABLE {tbl('auto_compact')}
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
AS SELECT * FROM {tbl('raw')}
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of files.
# MAGIC
# MAGIC You can see that the original table contained hundreds/thousands of small files, but the auto_compact table contains now just 1 (if you run this notebook for the first time)

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {tbl('raw')}").select("numFiles").show()
spark.sql(f"DESCRIBE DETAIL {tbl('auto_compact')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC See the number of rows before performing many small appends

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {tbl('auto_compact')}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Perform 55 small appends (approx 250-500 rows per batch)

# COMMAND ----------

for i in range(55):
    (df_raw.sample(fraction=0.00001)
     .write.format("delta")
     .mode("append")
     .saveAsTable(tbl('auto_compact')))

# COMMAND ----------

# MAGIC %md
# MAGIC Count the number of rows to confirm that new data was appended

# COMMAND ----------

spark.sql(f"SELECT count(*) FROM {tbl('auto_compact')}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Auto Compaction:**
# MAGIC
# MAGIC Delta Lake's auto-optimization features manage file sizes during the write phase, preventing the creation of many small files. To check how many files were written during each write, inspect the table's history.
# MAGIC
# MAGIC 1. Query the table's history using `DESCRIBE HISTORY`.
# MAGIC 2. Look at the `operation` and `operationMetrics` columns.
# MAGIC 3. Notice that each WRITE operation results in just one new file (`"numFiles":"1"`).
# MAGIC
# MAGIC The `delta.autoOptimize.optimizeWrite` property efficiently handles file sizes as data is written. The `autoCompact` feature triggers a separate OPTIMIZE operation only if many small files accumulate. Since `optimizeWrite` is managing this, you might not see separate compaction steps in the history.
# MAGIC
# MAGIC To verify:
# MAGIC - Browse the history and check the number of files written during each write.
# MAGIC - You should find the OPTIMIZE command if autoCompact was triggered.
# MAGIC
# MAGIC Note: Immediately after a write, the file count may still be high briefly. Re-run the query after some time to see the difference.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl('auto_compact')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Check number of files

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {tbl('auto_compact')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 3: VACUUM
# MAGIC
# MAGIC `VACUUM` removes obsolete files (not referenced by the current Delta log) to reclaim storage & trim metadata. It does NOT rewrite active data; it purges old snapshots beyond retention.
# MAGIC
# MAGIC ### Safety & Retention
# MAGIC - Default retention: 7 days (168h) to protect time travel & long-running readers
# MAGIC - Never shorten in production unless you fully understand recovery/RPO requirements
# MAGIC
# MAGIC ### When to Use
# MAGIC - After major rewrite / delete / update operations producing many stale files
# MAGIC - Storage costs are material and retention window already satisfied
# MAGIC - Old versions no longer needed for compliance or time travel queries
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Need historical versions for audits / debugging within retention horizon
# MAGIC - Active streaming or batch readers may still reference older snapshots
# MAGIC - Immediately after heavy DML when retention window not met
# MAGIC
# MAGIC > Production Practice: Retain at least 7 days (often 14–30+ for regulated domains). Use table history & storage metrics to justify more frequent vacuuming.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Perform updates/deletes before VACUUM to create orphaned files.

# COMMAND ----------

# Example update: Increase quantity by 10 for a specific customer
spark.sql(f"""
UPDATE {tbl('partitioned')}
SET quantity = quantity + 10
WHERE customer_id >= 1500
""")

# Example delete: Remove records for a specific product
spark.sql(f"""
DELETE FROM {tbl('partitioned')}
WHERE product_id <= 250
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Check number of files before vacuum

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {tbl('partitioned')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Run VACUUM
# MAGIC
# MAGIC > **Note:**  
# MAGIC > In Databricks Free Edition, you cannot disable the `retentionDurationCheck` safety feature.  
# MAGIC > This means you may need to wait up to **7 days** after performing update or delete operations before orphaned files can be removed by VACUUM.

# COMMAND ----------

# Disable the retention check
#spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# VACUUM the partitioned table
delta_table_to_vacuum = DeltaTable.forName(spark, tbl('partitioned'))
delta_table_to_vacuum.vacuum()

# Re-enable the retention check
#spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Check number of files after vacuum

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL {tbl('partitioned')}").select("numFiles").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of VACUUM:**
# MAGIC
# MAGIC - The `VACUUM` command cleans up files that are no longer part of the current version of the table. You can check the file system before and after running `VACUUM` on a table that has undergone several modifications to see the effect.

# COMMAND ----------

# MAGIC %md
# MAGIC # Project Conclusion
# MAGIC
# MAGIC You applied multiple, complementary Delta optimization techniques and observed their impact empirically.
# MAGIC
# MAGIC (See preceding section: Common Mistakes & Anti-Patterns for operational guardrails.)
# MAGIC
# MAGIC ### Comparative Summary (Qualitative)
# MAGIC | Technique | Primary Benefit | Ideal Use Case | Ongoing Maintenance |
# MAGIC |-----------|-----------------|----------------|---------------------|
# MAGIC | Partitioning | Directory pruning | Low-cardinality, common filters | Low once chosen |
# MAGIC | Z-Ordering | Multi-column data skipping | Repeated predicates across moderate/high-card columns | Periodic re-run as data grows |
# MAGIC | Manual Compaction | Fewer larger files | Backfill / bursty small writes | Run as needed (monitor file size dist) |
# MAGIC | Auto Optimize | Reduce small files proactively | Continuous ingestion workloads | Minimal |
# MAGIC | Liquid Clustering | Adaptive layout | Dynamic predicates & evolving schemas | Managed (monitor clustering metrics) |
# MAGIC | VACUUM | Storage reclamation | Any table with churn | Scheduled (respect retention) |
# MAGIC
# MAGIC ### Key Heuristics
# MAGIC - Start simple: partition only when clear benefit
# MAGIC - Use Z-Order or Liquid Clustering to refine skipping without over-partitioning
# MAGIC - Monitor file counts & avg file size (DESCRIBE DETAIL) for compaction signals
# MAGIC - Track clustering freshness (last Z-Order or clustering maintenance)
# MAGIC - Align time travel retention with recovery & audit requirements
# MAGIC
# MAGIC ### Common Mistakes & Anti-Patterns
# MAGIC
# MAGIC 1. Over-partitioning modest datasets (creates thousands of tiny directories & files)
# MAGIC 2. Z-Ordering on too many columns (shuffle blow-up; diminishing returns >4)
# MAGIC 3. Skipping VACUUM in cost-sensitive environments (latent storage bloat)
# MAGIC 4. Using Liquid Clustering on tiny tables (<1GB) where overhead > benefit
# MAGIC 5. Running manual OPTIMIZE on tables already governed by auto compaction (duplicate cost)
# MAGIC 6. Z-Ordering on extremely high-cardinality random IDs / high-precision timestamps (low locality gain)
# MAGIC 7. Not monitoring average file size drift after optimizations (missing 100MB–1GB target band)
# MAGIC 8. Blanket applying the same strategy to every table without considering access patterns
# MAGIC 9. Forgetting to re-run Z-Order after large backfills (stale clustering)
# MAGIC 10. Lowering VACUUM retention below recovery requirements (operational risk)
# MAGIC
# MAGIC > Recommendation: Maintain a lightweight observability sheet (table, avg file size, numFiles, last OPTIMIZE/Z-ORDER run, retention) to proactively detect drift.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suggested Next Experiments
# MAGIC 1. Add a date-based partition layer and compare with country partitioning
# MAGIC 2. Introduce updates/deletes & measure impact on small file generation
# MAGIC 3. Benchmark query patterns with and without Z-Order refresh after significant data growth
# MAGIC 4. Use Photon runtime and compare scan metrics
# MAGIC 5. Track metrics programmatically and plot improvements over time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Run the following cell to drop the catalog and all associated tables created during this project.

# COMMAND ----------

# Drop the catalog and all its contents
#spark.sql(f"DROP CATALOG IF EXISTS {CATALOG_NAME} CASCADE")
