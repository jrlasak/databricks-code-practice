# Databricks notebook source
# MAGIC %md
# MAGIC # 01: Data Layout
# MAGIC **Duration**: about 1 hour
# MAGIC
# MAGIC This read-and-run deep-dive compares three physical layouts of the same 10M-row `sales_raw` table on two lookups: one account's lines for one product with the account's country, and the same lookup without the country. There are no exercises: you run every cell and read the evidence it produces, the file counts from `DESCRIBE DETAIL` and the query profile of each benchmark query. Run `00_Setup_Environment` first.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end you should be able to:
# MAGIC - Choose between partitioning, Z-Order and Liquid Clustering for a table and its filters
# MAGIC - Tell data skipping apart from compaction when a query reads fewer files
# MAGIC - Read the IO metrics of a query profile: files and bytes read, and how much data was pruned
# MAGIC
# MAGIC ## Flow Overview
# MAGIC 1. Baseline query on `sales_raw`
# MAGIC 2. Technique 1: Partitioning
# MAGIC 3. Technique 2: Z-Order
# MAGIC 4. Technique 3: Liquid Clustering (incremental clustering)
# MAGIC 5. A second lookup without `country`, on all four tables
# MAGIC 6. One comparison table, and what it means
# MAGIC
# MAGIC > How to measure: after each benchmark query, click **See performance** under the cell output, open the query profile, and read the IO metrics (files and bytes read, pruning) and the wall-clock duration. Record them in the comparison table at the end of this notebook.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline Query (Unoptimized Table)
# MAGIC
# MAGIC We establish a performance baseline before changing the layout. This isolates the effect of each later technique.
# MAGIC
# MAGIC ### Query Pattern
# MAGIC This is the account-support lookup: one account (`customer_id = 1500`), one product (`product_id = 250`) and that account's country (`country = 'United States'`). The `benchmark()` helper from `variables` runs this exact filter against any table, so every layout is measured on the same predicate.
# MAGIC
# MAGIC The query returns 9 rows (any one account and product average about 6: 10M rows / (4,000 accounts x 401 products)). The rows do not matter; what matters is how much the scan read to find them.
# MAGIC
# MAGIC ### What to Capture
# MAGIC Files in the table, files read, bytes read, data pruned and duration. Record them in the comparison table at the end of this notebook.
# MAGIC
# MAGIC Run the two cells below: the first shows how many files `sales_raw` holds (`numFiles`) and its size, the second runs the baseline query.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('raw')}").select("numFiles", "sizeInBytes"))

# COMMAND ----------

display(benchmark('raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC Open the query profile (**See performance** under the cell output) and record files read, bytes read, data pruned and duration in the Raw column of the comparison table.
# MAGIC
# MAGIC Expect the scan to read all 2,000 files, about 157 MB, with no files pruned. The rows were written in random order, so every file's min/max range for `customer_id` and `product_id` covers almost the whole range, and data skipping has nothing to skip.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 1: Partitioning
# MAGIC
# MAGIC Partitioning physically groups data into directories by one or more columns. When a query predicate fixes a partition value, Spark can then skip every file in partitions the predicate excludes, without reading their statistics or data.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Efficient directory pruning for highly reused, low/moderate-cardinality filters
# MAGIC - Can bound per-query file counts for time/region sharded workloads
# MAGIC
# MAGIC ### When to Use
# MAGIC - Clear, consistent filtering patterns (e.g. date, region, category)
# MAGIC - Tables of 100 TB or more with one stable, low-cardinality filter, at least 1 GB per partition, and only once Liquid Clustering has been measured. Databricks recommends no partitioning under 1 TB, and Liquid Clustering from 1 TB to 100 TB. We partition a small table here only to see pruning at demo scale.
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - High-cardinality or rapidly growing distinct sets (user ids, fine-grain timestamps)
# MAGIC - Workloads without stable filter patterns
# MAGIC - Columns that mutate frequently (causes skew / small files)
# MAGIC
# MAGIC ### Trade-offs & Pitfalls
# MAGIC - Over-partitioning -> many small files + metadata overhead
# MAGIC - Too coarse partition -> limited pruning benefit
# MAGIC - Combining with Z-Order: partition first on a coarse dimension (e.g. date), then Z-Order inside each partition for additional skipping on high-cardinality columns
# MAGIC
# MAGIC We partition on `country`: exactly 10 values, and the benchmark query filters on it. This table is far too small to partition in production: it is far below 1 TB, and each country partition holds a tenth of it, well under 1 GB (divide the `sizeInBytes` of `sales_raw` above by 10). Treat this as a demonstration of the mechanism, not a layout to copy.

# COMMAND ----------

# MAGIC %md
# MAGIC Create `sales_partitioned`, partitioned by `country`. The write takes about 10 seconds.

# COMMAND ----------

(spark.read.table(tbl('raw')).write.format("delta")
 .mode("overwrite")
 .partitionBy("country")
 .saveAsTable(tbl('partitioned')))

# COMMAND ----------

# MAGIC %md
# MAGIC Check how many files the partitioned copy holds, then run the same query on it.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('partitioned')}").select("numFiles", "sizeInBytes", "partitionColumns"))

# COMMAND ----------

display(benchmark('partitioned'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Partitioning:**
# MAGIC
# MAGIC Open the query profile (**See performance** under the cell output) and compare it with the baseline. The filter on the partition key `country` lets Spark prune 9 of the 10 country directories, so it reads only the `United States` partition: 1 of the 10 files, about 16 MB of the table's 160 MB.
# MAGIC
# MAGIC Files read falls too, but not only because of pruning: the rewrite also changed the file count (`numFiles` above is 10, one file per country, against the 2,000 of `sales_raw`). Judge pruning by bytes read and the pruning metrics, not by files read alone. Inside the `United States` partition, Spark reads the whole partition, here one file: the partition layout does nothing for `customer_id` or `product_id`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 2: Z-Order
# MAGIC
# MAGIC Z-Ordering rewrites data files using a multi-dimensional space-filling curve (Z-Order) so rows with similar values for selected columns co-locate. Think of it like organizing a library so related books (rows) live on the same shelf (file): Spark can then skip whole shelves.
# MAGIC
# MAGIC ### What It Solves
# MAGIC - Improves data skipping without exploding partition counts
# MAGIC - Helps multi-column filtering patterns (e.g., customer_id AND product_id)
# MAGIC - Reduces number of files scanned for selective queries
# MAGIC
# MAGIC ### When to Use
# MAGIC - Multi-column filters in your queries
# MAGIC - Tables with 2-4 frequently queried, moderate/high-cardinality columns
# MAGIC - Data that doesn't change constantly (batch / micro-batch growth)
# MAGIC - Need clustering on columns that would be poor physical partitions
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Single-column selective queries (simple sorting / partitioning may suffice)
# MAGIC - Very frequent small incremental writes (rewrite overhead can outweigh benefit)
# MAGIC - More than 4 columns (diminishing returns & larger shuffle)
# MAGIC - Columns your queries never filter on (Z-Order only helps predicates on the Z-Ordered columns)
# MAGIC
# MAGIC ### Column Choice Tips
# MAGIC | Scenario | Better Partition | Better Z-Order |
# MAGIC |----------|------------------|----------------|
# MAGIC | Very low cardinality | Yes | Usually unnecessary |
# MAGIC | High cardinality (but queried) | No | Often yes |
# MAGIC | Evolving / mixed predicates | Limited | Strong |
# MAGIC | Filter columns may change later | No | Consider Liquid Clustering |
# MAGIC
# MAGIC We Z-Order on `customer_id` and `product_id`, the selective columns in the benchmark predicate. `country` adds nothing: every account belongs to one country, so a filter on `customer_id` already selects one country's rows.
# MAGIC
# MAGIC > Operational Hint: Re-run Z-Order after large backfills or once enough new, un-Z-Ordered data has landed that file skipping on the Z-Order columns visibly degrades (watch bytes read in the query profile). Predictive optimization never runs `OPTIMIZE ... ZORDER BY` for you.

# COMMAND ----------

# MAGIC %md
# MAGIC Create the Z-Ordered copy of `sales_raw`. `delta.targetFileSize = '16mb'` is set when the table is created, before the load, so `OPTIMIZE ... ZORDER BY` writes files near the 16 MB target: about 9 of them for this table of about 160 MB, enough for skipping to show. Without it, OPTIMIZE would compact the whole table into a single file toward the 256 MB target, and skipping would have nothing to skip. This is a demonstration lever: on Unity Catalog managed tables Databricks tunes file sizes automatically, so in production you leave `delta.targetFileSize` unset. This cell runs for about 20 seconds.

# COMMAND ----------

# Copy sales_raw into a new table so Z-Ordering leaves the sales_raw baseline untouched
spark.sql(f"DROP TABLE IF EXISTS {tbl('raw_zorder')}")
spark.sql(f"""
CREATE TABLE {tbl('raw_zorder')} ({SALES_COLUMNS})
USING DELTA
TBLPROPERTIES ('delta.targetFileSize' = '16mb')
""")
spark.sql(f"INSERT INTO {tbl('raw_zorder')} SELECT * FROM {tbl('raw')}")

# Z-Order on the two selective benchmark columns; OPTIMIZE also compacts toward the 16 MB target
spark.sql(f"OPTIMIZE {tbl('raw_zorder')} ZORDER BY (customer_id, product_id)")

# COMMAND ----------

# MAGIC %md
# MAGIC Check how many files the Z-Ordered copy holds, then run the same query on it.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('raw_zorder')}").select("numFiles", "sizeInBytes"))

# COMMAND ----------

display(benchmark('raw_zorder'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Z-Ordering:**
# MAGIC
# MAGIC Open the query profile and compare files read with the number of files in this table (`numFiles` above), not with the baseline's roughly 2,000. Part of the drop from 2,000 is compaction: OPTIMIZE also rewrote the small files into 9 files of about 18 MB. Data skipping is the share of those files the query never opens. Z-Ordered on `customer_id` and `product_id`, the query reads 1 of the 9 files and prunes the other 8: 15-21 MB read of the table's 160 MB.
# MAGIC
# MAGIC Every variant returns the same rows, which confirms that the layout changed and the data did not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique 3: Liquid Clustering
# MAGIC
# MAGIC Liquid Clustering clusters data on the keys you declare in `CLUSTER BY`, and `OPTIMIZE` (run by you or by predictive optimization) clusters new data incrementally. Only `CLUSTER BY AUTO` picks the keys from your query patterns, and it needs a Unity Catalog managed table with predictive optimization enabled. It replaces static partitions: there is no directory hierarchy, and data skipping still works on the clustering keys.
# MAGIC
# MAGIC ### Advantages vs Partitioning / Z-Order
# MAGIC - Incremental: reclusters incremental data on each `OPTIMIZE` without rewriting already-clustered files
# MAGIC - Multi-column "soft" clustering without explosion in partitions
# MAGIC - Clustering keys can be changed with `ALTER TABLE ... CLUSTER BY` (then `OPTIMIZE FULL`) without recreating the table
# MAGIC
# MAGIC ### When to Use
# MAGIC - New tables: Databricks recommends Liquid Clustering for all new tables
# MAGIC - Evolving query patterns
# MAGIC - Multiple clustering columns needed
# MAGIC - Want to replace both partitioning and Z-Ordering
# MAGIC - High churn / streaming upserts
# MAGIC - Many predicates across overlapping column sets
# MAGIC - Avoiding maintenance burden of periodic manual Z-Order runs
# MAGIC
# MAGIC ### When NOT to Use
# MAGIC - Tables of 100 TB or more with one stable filter column and at least 1 GB per partition, where you measured partitioning beating Liquid Clustering (the only case the docs leave open)
# MAGIC - Workloads requiring deterministic physical ordering for downstream consumers
# MAGIC
# MAGIC > Consider long-term ops: which technique minimizes recurring maintenance for your workload? Liquid Clustering removes the manual Z-Order re-runs, but `OPTIMIZE` still has to run often, by you or by predictive optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC Create `sales_liquid_clustered` with `CLUSTER BY (customer_id, product_id)`, the same keys as the Z-Ordered copy, and the same 16 MB target file size. Load it with `INSERT INTO`, which keeps the declared clustering. Clustering on write only applies above a size threshold and ignores `delta.targetFileSize`, so run `OPTIMIZE ... FULL` once after the load: it reclusters every record into files of about 16 MB before you measure. This cell runs for about 30 seconds.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {tbl('liquid_clustered')} ({SALES_COLUMNS})
USING DELTA
CLUSTER BY (customer_id, product_id)
TBLPROPERTIES ('delta.targetFileSize' = '16mb')
""")

# Load with INSERT INTO so the CLUSTER BY definition stays in place
spark.sql(f"INSERT INTO {tbl('liquid_clustered')} SELECT * FROM {tbl('raw')}")

# Recluster every record into files of about 16 MB before measuring
spark.sql(f"OPTIMIZE {tbl('liquid_clustered')} FULL")

# COMMAND ----------

# MAGIC %md
# MAGIC Check how many files the clustered table holds and its clustering keys, then run the same query on it.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('liquid_clustered')}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

display(benchmark('liquid_clustered'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of Liquid Clustering:**
# MAGIC
# MAGIC Open the query profile for the cell above and compare files read (out of `numFiles` above), bytes read and the pruning metrics with the Z-Ordered table. After one load and one `OPTIMIZE FULL`, both layouts skip on the same two keys, so expect a close result: roughly 9 files in the table and 1 or 2 of them read, about 15-21 MB when it reads one file, and more when it opens a second. The exact file boundaries can differ from run to run, so a second file's `customer_id` and `product_id` range can also cover account 1500 and product 250, and the query then opens that file too. The Z-Ordered copy reads 1 of its 9 files.
# MAGIC
# MAGIC The difference shows up later, as new data arrives: Liquid Clustering keeps clustering new data on each `OPTIMIZE` without rewriting already-clustered files, and its keys can be changed without recreating the table, while a Z-Order has to be re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second Lookup: Without `country`
# MAGIC
# MAGIC On the benchmark lookup (with `country`), the three layouts come out about even, because the filter fixes `country` and partitioning prunes on it. The same lookup often arrives without the country: a search by account number and product only. The `benchmark_any_country()` helper from `variables` runs that filter, `customer_id = 1500 AND product_id = 250` with no predicate on `country`, against any table.
# MAGIC
# MAGIC It returns the same 9 rows, because every account belongs to one country and account 1500 is in the United States. What changes is what each layout can skip. Without a `country` predicate, partition pruning excludes no partition, and each country file holds accounts from the whole `customer_id` range, so its min/max statistics cannot exclude it either. Z-Order and Liquid Clustering grouped the rows by `customer_id` and `product_id`, so their file statistics still rule out most files.
# MAGIC
# MAGIC Run the four cells below, one per table. After each one, open the query profile (**See performance** under the cell output) and record files read, bytes read and data pruned in the "without `country`" rows of the comparison table.

# COMMAND ----------

display(benchmark_any_country('raw'))

# COMMAND ----------

display(benchmark_any_country('partitioned'))

# COMMAND ----------

display(benchmark_any_country('raw_zorder'))

# COMMAND ----------

display(benchmark_any_country('liquid_clustered'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Analysis of the second lookup:**
# MAGIC
# MAGIC Compare each table's files read with its `numFiles`. Expect:
# MAGIC
# MAGIC - **Raw**: all 2,000 files again, about 157 MB, 0 pruned.
# MAGIC - **Partitioned**: all 10 of its files, 0 pruned, about 39 MB read. With `country` it read 1 file; without it, no partition is excluded, so every country file is opened. The 39 MB stays well below the table's 160 MB, so judge this lookup by files read out of each table's `numFiles` and by the pruning metrics: 10 of 10 opened and none pruned, against 1 of 9 opened and 8 pruned for Z-Order and 1 or 2 opened, the rest pruned, for Liquid.
# MAGIC - **Z-Order**: still 1 of its 9 files, 8 pruned, about the same 15-21 MB as with `country`.
# MAGIC - **Liquid**: still 1 or 2 of its roughly 9 files, the rest pruned, the same count as with `country`: about 15-21 MB with one file, more with two. Either way, what separates it from the partitioned table is files read and pruning: 1 or 2 opened and the rest pruned, against 10 of 10 opened and none pruned.
# MAGIC
# MAGIC The skipping of Z-Order and Liquid never depended on `country`. On this lookup Z-Order opens a tenth of the files the partitioned table opens and reads about half its bytes; Liquid opens 1 or 2 files instead of 10 and prunes the rest, where the partitioned table prunes none. All four tables return the same 9 rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare the Four Layouts
# MAGIC Fill in one column per table from the `DESCRIBE DETAIL` output and the query profile of each lookup: the benchmark lookup (with `country`) and the second lookup (without `country`).
# MAGIC
# MAGIC | Metric | Where to read it | Raw | Partitioned | Z-Order | Liquid |
# MAGIC |--------|------------------|-----|-------------|---------|--------|
# MAGIC | Files in table | `DESCRIBE DETAIL` (`numFiles`) | ? | ? | ? | ? |
# MAGIC | Files read, with `country` | Query profile -> IO metrics | ? | ? | ? | ? |
# MAGIC | Bytes read, with `country` | Query profile -> IO metrics | ? | ? | ? | ? |
# MAGIC | Data pruned, with `country` | Query profile -> IO metrics (pruning) | ? | ? | ? | ? |
# MAGIC | Duration (s), with `country`, noisy on serverless: compare only large gaps | Query profile -> wall-clock duration | ? | ? | ? | ? |
# MAGIC | Files read, without `country` | Query profile -> IO metrics | ? | ? | ? | ? |
# MAGIC | Bytes read, without `country` | Query profile -> IO metrics | ? | ? | ? | ? |
# MAGIC | Data pruned, without `country` | Query profile -> IO metrics (pruning) | ? | ? | ? | ? |
# MAGIC
# MAGIC Judge each layout by files read out of the files in that table, by bytes read and by the pruning metrics. Raw file counts across tables mostly measure compaction: `sales_raw` holds 2,000 files, the partitioned copy 10, the Z-Ordered copy 9 and the Liquid copy roughly 9.
# MAGIC
# MAGIC **Expected pattern:** with `country`, Raw reads all 2,000 of its files, about 157 MB. Partitioned reads only the `United States` partition: 1 of its 10 files, about 16 MB. Z-Order reads 1 of its 9 files, 15-21 MB. Liquid reads 1 or 2 of its roughly 9 files, about 15-21 MB with one file and more with two. Without `country`, Raw still reads all 2,000 files, about 157 MB. Partitioned reads all 10 of its files, about 39 MB, with none pruned. Z-Order still reads 1 of its 9 files, 15-21 MB, and Liquid the same 1 or 2 files as with `country`, the rest pruned. Both lookups return the same 9 rows on all four tables.
# MAGIC
# MAGIC **What this means:** with `country` in the filter, each of the three layouts cuts the lookup from 2,000 files and about 157 MB to 1 or 2 files of about 15-21 MB each: 1 file for partitioning and Z-Order, 1 or 2 for Liquid. On that lookup they come out about even. Partitioning keeps up only because the filter fixes `country` and each country partition fits in one file. The second lookup shows where they part. Without `country`, partition pruning has nothing to prune, and the partitioned table opens all 10 of its files. Z-Order and Liquid Clustering still skip on `customer_id` and `product_id`, with no directory per value: Z-Order opens 1 file instead of 10, with about half the bytes, and Liquid 1 or 2 files instead of 10. Liquid Clustering is the one to keep when the filter columns change: you change its keys with `ALTER TABLE ... CLUSTER BY` instead of rebuilding a partition layout, and Databricks recommends it for new tables.
# MAGIC
# MAGIC Next: `02_File_Maintenance` compacts, auto-compacts and vacuums the same data.
