# Databricks notebook source
# MAGIC %md
# MAGIC # 02: File Maintenance
# MAGIC **Duration**: about 45 minutes
# MAGIC
# MAGIC This read-and-run deep-dive works on the files themselves: compacting small files by hand, letting Databricks compact them as you write, and deleting files a table no longer references. There are no exercises: you run every cell and read the file counts and table history it prints.
# MAGIC
# MAGIC Run `00_Setup_Environment` and `01_Data_Layout` first: the VACUUM section modifies `sales_partitioned`, which `01_Data_Layout` builds.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end you should be able to:
# MAGIC - Diagnose a small-file problem from `numFiles` and `sizeInBytes` in `DESCRIBE DETAIL`
# MAGIC - Apply manual compaction (`OPTIMIZE`) and contrast it with auto compaction
# MAGIC - Find auto compaction in `DESCRIBE HISTORY`
# MAGIC - Use VACUUM safely: what it deletes, when, and why `numFiles` does not change
# MAGIC
# MAGIC ## Flow Overview
# MAGIC - Technique 4: Manual Compaction (OPTIMIZE)
# MAGIC - Technique 5: Auto Optimize (optimized writes + auto compaction)
# MAGIC - Technique 6: VACUUM
# MAGIC - Conclusion and suggested next experiments

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 4: Manual Compaction (OPTIMIZE)
# MAGIC
# MAGIC Small files inflate job overhead: every file costs an open, a footer read and a transaction-log entry, and Spark plans more scan tasks for many small files than for a few large ones. They also spread a query's rows over more files to read. `OPTIMIZE` rewrites many small files into fewer, larger ones without changing the logical content. Databricks autotunes the target: 256 MB for tables under 2.56 TB, growing to 1 GB above 10 TB.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Reduces per-task scheduling overhead
# MAGIC - Improves scan & filter efficiency by reducing file count
# MAGIC - Prepares table for better Z-Ordering / clustering effectiveness
# MAGIC
# MAGIC ### When to Use
# MAGIC - Many files far below the target size
# MAGIC - Bursty or streaming ingestion producing tiny batch outputs
# MAGIC - Before heavy analytical / BI workloads to lower latency
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Files already near the autotuned target (256 MB for tables under 2.56 TB, growing to 1 GB above 10 TB)
# MAGIC - Ultra-low latency streaming where rewrite cost is disruptive
# MAGIC - Limited compute window / cost constraints (schedule off-peak instead)
# MAGIC
# MAGIC > Rule of thumb: compare `sizeInBytes / numFiles` from `DESCRIBE DETAIL` with the 256 MB target Databricks uses for tables under 2.56 TB.
# MAGIC
# MAGIC ### Simulation Approach
# MAGIC We append 10 small random samples to `sales_to_compact` to manufacture fragmentation, then run `OPTIMIZE` to compact.
# MAGIC
# MAGIC > Unity Catalog managed tables (every table in this lab) also get background auto compaction, which can compact these small files on its own. That is why the cells below check `numFiles` before you run `OPTIMIZE`.

# COMMAND ----------

# MAGIC %md
# MAGIC Append 10 random samples of about 10,000 rows each (0.1% of `sales_raw`) to `sales_to_compact`. The cell drops the table first, so every run starts from the same 10 appends.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {tbl('to_compact')}")

# Simulate small file creation: 10 appends of a 0.1% sample each
for i in range(10):
    (spark.read.table(tbl('raw')).sample(fraction=0.001, seed=i)
     .write.format("delta")
     .mode("append")
     .saveAsTable(tbl('to_compact')))

rows = spark.read.table(tbl('to_compact')).count()
print(f"sales_to_compact rows: {rows:,} (expect 99,000-101,000)")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the file count (`numFiles`) and the average file size after the 10 appends.

# COMMAND ----------

d = spark.sql(f"DESCRIBE DETAIL {tbl('to_compact')}").first()
files_before = d["numFiles"]
print(f"sales_to_compact files before OPTIMIZE: {files_before:,} (expect 20-40), average {d['sizeInBytes'] / files_before / 1024**2:.1f} MB per file (target 256 MB)")

# COMMAND ----------

# MAGIC %md
# MAGIC Run `OPTIMIZE` (compaction only, no Z-Order here).

# COMMAND ----------

spark.sql(f"OPTIMIZE {tbl('to_compact')}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check `numFiles` again: it should be far lower than the count above, because `OPTIMIZE` rewrote the small files into one file. About 100,000 narrow rows are far below the 256 MB target, so they fit in a single file.

# COMMAND ----------

d = spark.sql(f"DESCRIBE DETAIL {tbl('to_compact')}").first()
files_after = d["numFiles"]
print(f"sales_to_compact files after OPTIMIZE: {files_after:,} (expect 1; compare with the count before OPTIMIZE above), average {d['sizeInBytes'] / files_after / 1024**2:.1f} MB per file (target 256 MB)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Compaction:**
# MAGIC
# MAGIC The 10 appends of about 10,000 rows each wrote about 30 small files of about 0.1 MB; `OPTIMIZE` rewrote them into one file of about 1.7 MB. Fewer files means fewer tasks, fewer file opens and fewer log entries for every later read.
# MAGIC
# MAGIC The average file size is the diagnosis: before `OPTIMIZE` it is a small fraction of the 256 MB target, which is the small-file signal to look for on your own tables. After `OPTIMIZE` it is still below 256 MB only because the whole table fits in one file.
# MAGIC
# MAGIC If the count before `OPTIMIZE` was already 1, background auto compaction got there first: look for an `OPTIMIZE` row in `DESCRIBE HISTORY` on `sales_to_compact`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 5: Auto Optimize (optimized writes + auto compaction)
# MAGIC
# MAGIC On Unity Catalog managed tables (every table in this lab), leave the two properties below unset: Databricks then tunes file sizes and runs background auto compaction, and recommends that path, or predictive optimization. We set them here so you can watch auto compaction happen in the table history. On external tables they are still the lever you set; `'auto'` is the recommended value for `autoCompact`, and this lab uses `'true'` (a fixed 128 MB target) so the result is predictable.
# MAGIC
# MAGIC With the properties set, optimized writes size files during the write, and auto compaction compacts small files synchronously on the same compute right after the write commits.
# MAGIC
# MAGIC ### Components
# MAGIC - `delta.autoOptimize.optimizeWrite`: shuffles data before the write so each partition is written as fewer, larger files (128 MB target)
# MAGIC - `delta.autoOptimize.autoCompact`: merges residual small files right after the commit, once enough have accumulated (the threshold is `spark.databricks.delta.autoCompact.minNumFiles`, 50 files by default)
# MAGIC
# MAGIC ### When to Use
# MAGIC - Streaming pipelines or frequent micro-batch ingestion into external tables
# MAGIC - Continuous incremental loads where manual scheduling adds complexity
# MAGIC - Teams lacking bandwidth for manual compaction orchestration
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Unity Catalog managed tables: leave the properties unset and let background auto compaction run
# MAGIC - Large, infrequent batch jobs already producing good-sized files
# MAGIC - Latency-sensitive writes: optimized writes add a shuffle, and each triggered compaction adds write latency and compute
# MAGIC - Need for bespoke compaction policies / ordering (custom logic required)
# MAGIC
# MAGIC ### Caveats
# MAGIC - Not a substitute for Z-Order / Liquid Clustering (does not cluster for skipping)
# MAGIC - Does not fix skew or suboptimal partition strategy
# MAGIC
# MAGIC Verify it with the file count (`DESCRIBE DETAIL`) and the table history (`DESCRIBE HISTORY`): each small append should add one file, and an `OPTIMIZE` row appears once enough small files accumulate.

# COMMAND ----------

# MAGIC %md
# MAGIC Create `sales_auto_compact` from `sales_raw` with `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` set to `true`. The copy takes about 10 seconds.

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
# MAGIC Compare the file counts. `sales_raw` holds about 2,000 small files (200 write partitions x 10 files of 5,000 rows). Optimized writes size files during the write, so `sales_auto_compact` holds all 10M rows in a single file.

# COMMAND ----------

raw_files = spark.sql(f"DESCRIBE DETAIL {tbl('raw')}").first()["numFiles"]
auto_files = spark.sql(f"DESCRIBE DETAIL {tbl('auto_compact')}").first()["numFiles"]
print(f"sales_raw files: {raw_files:,} (expect about 2,000; anything from 1,800 up means the small files are intact)")
print(f"sales_auto_compact files after the copy: {auto_files:,} (expect 1)")

# COMMAND ----------

# MAGIC %md
# MAGIC Count the rows before the small appends.

# COMMAND ----------

rows_before = spark.read.table(tbl('auto_compact')).count()
print(f"sales_auto_compact rows before the appends: {rows_before:,} (expect 10,000,000)")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform 55 small appends, each a seeded 0.005% sample of `sales_raw`. On this data each sample returns about 450 rows, a little under the 500 that 0.005% of 10M suggests, and the row check below expects that. The appends run one after another, for about 2 to 3 minutes in total.

# COMMAND ----------

for i in range(55):
    (spark.read.table(tbl('raw')).sample(fraction=0.00005, seed=100 + i)
     .write.format("delta")
     .mode("append")
     .saveAsTable(tbl('auto_compact')))

# COMMAND ----------

# MAGIC %md
# MAGIC Count the rows again to confirm the appends landed: 10,000,000 plus 55 x about 450 rows.

# COMMAND ----------

rows_after = spark.read.table(tbl('auto_compact')).count()
print(f"sales_auto_compact rows after the appends: {rows_after:,} (expect 10,023,500-10,027,500: 10,000,000 plus 55 x about 450)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Auto Optimize:**
# MAGIC
# MAGIC Optimized writes sized each small append as one file. The 55 one-file appends push the table past the auto compaction threshold (`minNumFiles`, see Components above), so auto compaction runs right after the 50th append commits and merges those 50 one-file appends into 1 file.
# MAGIC
# MAGIC The query below filters the history to the rows that matter:
# MAGIC 1. `files_written` shows how many files each `WRITE` added.
# MAGIC 2. Look for the `OPTIMIZE` row with `auto_compaction = true`: that is auto compaction merging the small appends.
# MAGIC 3. `files_removed` and `files_added` on that row show how many small files it merged, and into how few.

# COMMAND ----------

display(spark.sql(f"""
SELECT version, operation, operationParameters['auto'] AS auto_compaction,
       operationMetrics['numFiles'] AS files_written,
       operationMetrics['numRemovedFiles'] AS files_removed,
       operationMetrics['numAddedFiles'] AS files_added
FROM (DESCRIBE HISTORY {tbl('auto_compact')})
WHERE operation IN ('CREATE TABLE AS SELECT', 'WRITE', 'OPTIMIZE')
ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Check `numFiles` for `sales_auto_compact` after the 55 appends. Expect 7: the 1 file from the copy, the 1 file auto compaction merged the first 50 appends into, and the 5 appends written after it.

# COMMAND ----------

auto_files_after = spark.sql(f"DESCRIBE DETAIL {tbl('auto_compact')}").first()["numFiles"]
print(f"sales_auto_compact files after the 55 appends: {auto_files_after:,} (expect 7: 1 from the copy, 1 from auto compaction, 5 later appends)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 6: VACUUM
# MAGIC
# MAGIC `VACUUM` deletes data files the current table version no longer references, once they are older than the retention threshold, to cut storage cost. It does not rewrite active data, and versions older than the retention window can no longer be time travelled to.
# MAGIC
# MAGIC ### Safety & Retention
# MAGIC - Default retention: 7 days (`delta.deletedFileRetentionDuration`), so a multi-day job's uncommitted files and recent time-travel versions survive
# MAGIC - Never shorten in production unless you fully understand recovery/RPO requirements
# MAGIC
# MAGIC ### When to Use
# MAGIC - After major rewrite / delete / update operations that leave many unreferenced files
# MAGIC - Storage costs are material and retention window already satisfied
# MAGIC - Old versions no longer needed for compliance or time travel queries
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Need historical versions for audits / debugging within retention horizon
# MAGIC - Active streaming or batch readers may still reference older snapshots
# MAGIC - Immediately after heavy DML when retention window not met
# MAGIC
# MAGIC > Production Practice: Retain at least 7 days (often 14-30+ for regulated domains). Use table history & storage metrics to justify more frequent vacuuming.

# COMMAND ----------

# MAGIC %md
# MAGIC Run an UPDATE and a DELETE on `sales_partitioned`, then `OPTIMIZE`. The three statements run for about 20 seconds. To repeat this section, re-run the partitioning cell in `01_Data_Layout` first. The row and file counts then start from the same table; the history grid keeps the earlier rows and lists the new ones below them.
# MAGIC
# MAGIC On tables with deletion vectors (on by default for new tables on many workspaces), UPDATE and DELETE do not rewrite the files they touch. They mark the old rows as removed, and UPDATE writes the new row versions to new files. The marked files stay in the current version until a rewrite applies the marks: the `OPTIMIZE` below, or an auto compaction.
# MAGIC
# MAGIC Auto compaction is always enabled for UPDATE and DELETE, and on Free Edition it runs after both. In the history grid below, expect an `OPTIMIZE` row with `auto_compaction = true` right after the UPDATE and another right after the DELETE, each rewriting one or two files. The final `OPTIMIZE` (`auto_compaction = false`) removes 20 files and adds 10, one per country.
# MAGIC
# MAGIC After that rewrite, the replaced files are no longer part of the current version. Those are the unreferenced files VACUUM removes once they are older than the 7-day retention.

# COMMAND ----------

# Update about half the rows: accounts 2,001-4,000
spark.sql(f"""
UPDATE {tbl('partitioned')}
SET quantity = quantity + 10
WHERE customer_id > 2000
""")

# Delete products 100-250: 151 of the 401 products, about 38% of rows
spark.sql(f"""
DELETE FROM {tbl('partitioned')}
WHERE product_id <= 250
""")

# Rewrite the files deletion vectors only marked, so the old files leave the current version
spark.sql(f"OPTIMIZE {tbl('partitioned')}")

rows = spark.read.table(tbl('partitioned')).count()
print(f"sales_partitioned rows after the DELETE: {rows:,} (expect 6,225,000-6,244,000)")

# COMMAND ----------

# MAGIC %md
# MAGIC Check whether the UPDATE and DELETE rewrote files or only wrote deletion vectors. For each UPDATE, DELETE and OPTIMIZE row, the history shows how many files it removed and added, and how many deletion vectors it wrote. Expect `files_removed` 0 on the UPDATE and the DELETE, with 10 and 20 deletion vectors: they marked rows instead of rewriting files. The `OPTIMIZE` rows with `auto_compaction = true` are auto compaction; the one with `false` is the `OPTIMIZE` you ran above.

# COMMAND ----------

display(spark.sql(f"""
SELECT version, operation, operationParameters['auto'] AS auto_compaction,
       operationMetrics['numRemovedFiles'] AS files_removed,
       operationMetrics['numAddedFiles'] AS files_added,
       operationMetrics['numDeletionVectorsAdded'] AS deletion_vectors_added
FROM (DESCRIBE HISTORY {tbl('partitioned')})
WHERE operation IN ('UPDATE', 'DELETE', 'OPTIMIZE')
ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Check `numFiles` before VACUUM.

# COMMAND ----------

files_before_vacuum = spark.sql(f"DESCRIBE DETAIL {tbl('partitioned')}").first()["numFiles"]
print(f"sales_partitioned files before VACUUM: {files_before_vacuum:,} (expect 10: one per country)")

# COMMAND ----------

# MAGIC %md
# MAGIC Run `VACUUM ... DRY RUN` first: it lists the files VACUUM would delete now, without deleting anything. Expect 0: every file the statements above replaced is younger than the 7-day retention.

# COMMAND ----------

would_delete = spark.sql(f"VACUUM {tbl('partitioned')} DRY RUN").count()
print(f"Files VACUUM would delete now: {would_delete:,} (expect 0: the replaced files are younger than 7 days)")

# COMMAND ----------

# MAGIC %md
# MAGIC Run VACUUM.
# MAGIC
# MAGIC > **Note:** VACUUM keeps unreferenced files for the table's `delta.deletedFileRetentionDuration` (default 7 days), and this lab leaves it at the default, so VACUUM deletes nothing until the files are 7 days old. `spark.databricks.delta.retentionDurationCheck.enabled` is only the safety check that blocks a dangerously short retention: classic compute lets you disable it for a demo, serverless compute (including Free Edition) does not, and production never should.

# COMMAND ----------

spark.sql(f"VACUUM {tbl('partitioned')}")

# COMMAND ----------

# MAGIC %md
# MAGIC Check `numFiles` after VACUUM: it matches the count above, because `DESCRIBE DETAIL` counts only the files in the current version, and VACUUM deletes only files that version no longer references.

# COMMAND ----------

files_after_vacuum = spark.sql(f"DESCRIBE DETAIL {tbl('partitioned')}").first()["numFiles"]
print(f"sales_partitioned files after VACUUM: {files_after_vacuum:,} (compare with the count before VACUUM above: they match)")

# COMMAND ----------

# MAGIC %md
# MAGIC Check how many files VACUUM deleted: the `VACUUM END` row of `DESCRIBE HISTORY` records it.

# COMMAND ----------

display(spark.sql(f"""
SELECT version, operation, operationMetrics['numDeletedFiles'] AS files_deleted
FROM (DESCRIBE HISTORY {tbl('partitioned')})
WHERE operation = 'VACUUM END'
ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of VACUUM:**
# MAGIC
# MAGIC `DESCRIBE DETAIL` counts only files in the current version, so `numFiles` does not change after VACUUM. To see the effect, compare the `DRY RUN` count with `files_deleted` on the `VACUUM END` row. Expect 0 deleted files on any compute: the lab keeps the default 7-day retention, and every removed file is younger than that. Run the `DRY RUN` cell again a week from now to see the files VACUUM would clear. If it still shows 0, predictive optimization has probably vacuumed them already: look for a `VACUUM END` row you did not run in `DESCRIBE HISTORY`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Project Conclusion
# MAGIC
# MAGIC You built six tables from the same 10M rows and measured how partitioning, Z-Order and Liquid Clustering change the files two lookups read, with and without `country`, how OPTIMIZE and auto compaction cut the file count, and what VACUUM deletes and when.
# MAGIC
# MAGIC (See Common Mistakes & Anti-Patterns below for operational guardrails.)
# MAGIC
# MAGIC ### Comparative Summary (Qualitative)
# MAGIC | Technique | Primary Benefit | Ideal Use Case | Ongoing Maintenance |
# MAGIC |-----------|-----------------|----------------|---------------------|
# MAGIC | Partitioning | Directory pruning | Tables of 100 TB or more with one stable low-cardinality filter and at least 1 GB per partition, after Liquid Clustering was measured first | Low once chosen |
# MAGIC | Z-Ordering | Multi-column data skipping | Repeated predicates across moderate- or high-cardinality columns | Periodic re-run as data grows |
# MAGIC | Manual Compaction | Fewer larger files | Backfill / bursty small writes | Run as needed (monitor the file size distribution) |
# MAGIC | Auto Optimize properties | Fewer small files on external/legacy tables | Streaming or frequent appends to external tables | None on UC managed tables: leave them unset |
# MAGIC | Liquid Clustering | Data skipping on declared keys, no partitions | New tables, filters on 1-4 columns | `OPTIMIZE` runs often, by you or by predictive optimization |
# MAGIC | VACUUM | Storage reclamation | Any table with churn | Predictive optimization on UC managed tables where enabled; otherwise scheduled (respect retention) |
# MAGIC
# MAGIC ### Key Heuristics
# MAGIC - Never partition under 1 TB; from 1 TB to 100 TB use Liquid Clustering; partition only at 100 TB or more, with at least 1 GB per partition, once Liquid Clustering has been measured
# MAGIC - Use Liquid Clustering for new tables (the Databricks default); keep Z-Order for existing tables you are not migrating yet
# MAGIC - Monitor file counts and average file size (`DESCRIBE DETAIL`) for compaction signals
# MAGIC - Track clustering freshness (last Z-Order or clustering maintenance)
# MAGIC - On Unity Catalog managed tables, check whether predictive optimization is enabled before scheduling OPTIMIZE or VACUUM yourself
# MAGIC - Align time travel retention with recovery & audit requirements
# MAGIC
# MAGIC ### Common Mistakes & Anti-Patterns
# MAGIC
# MAGIC 1. Over-partitioning modest datasets (creates thousands of tiny directories & files)
# MAGIC 2. Z-Ordering on too many columns (shuffle blow-up; diminishing returns >4)
# MAGIC 3. Skipping VACUUM in cost-sensitive environments (latent storage bloat)
# MAGIC 4. Scheduling your own OPTIMIZE or VACUUM job on tables where predictive optimization already runs them (Databricks recommends disabling scheduled OPTIMIZE jobs when it is on)
# MAGIC 5. Z-Ordering on columns no query filters on, or on low-cardinality columns that partition pruning or min/max stats already handle
# MAGIC 6. Not monitoring average file size drift after optimizations (the autotuned target is 256 MB for tables under 2.56 TB, growing to 1 GB above 10 TB)
# MAGIC 7. Blanket applying the same strategy to every table without considering access patterns
# MAGIC 8. Forgetting to re-run Z-Order after large backfills (stale clustering)
# MAGIC 9. Lowering VACUUM retention below recovery requirements (operational risk)
# MAGIC
# MAGIC > Recommendation: Maintain a lightweight observability sheet (table, average file size, numFiles, last OPTIMIZE / Z-Order run, retention) to proactively detect drift.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Suggested Next Experiments
# MAGIC 1. Add a date-based partition layer and compare its scan metrics with country partitioning
# MAGIC 2. Grow the table incrementally and measure how much Z-Order skipping you lose before the next `OPTIMIZE ... ZORDER BY`
# MAGIC 3. Store each run's metrics in a Delta table and plot them over time
# MAGIC 4. Stream new data in with Auto Loader and compare file counts and skipping on the Liquid Clustered table
# MAGIC
# MAGIC When you are done, run `03_Cleanup` to drop the `delta_optimization_project` catalog.
