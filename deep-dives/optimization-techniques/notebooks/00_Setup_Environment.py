# Databricks notebook source
# MAGIC %md
# MAGIC # 00: Setup Environment
# MAGIC **Duration**: about 15 minutes, including about a minute for the data write
# MAGIC
# MAGIC You own `sales`, the order-line fact table of a wholesale distributor: 10M lines from 4,000 business accounts in 10 countries over the two years to 30 June 2026. The account-support app looks up one account's lines for one product, and on the table as loaded, that lookup reads all of the table's roughly 2,000 files. Each technique in `01_Data_Layout` and `02_File_Maintenance` is a candidate fix, and the measurements tell you which one is worth its maintenance cost.
# MAGIC
# MAGIC This notebook creates the `delta_optimization_project` catalog and the `sales_data` schema, writes the 10M-row `sales_raw` table as about 2,000 small files, and checks that the small files are there. Every comparison in the next two notebooks starts from this table. Your workspace permissions must allow catalog and schema creation.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# Create the catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Sales Data
# MAGIC
# MAGIC We create a semi-realistic sales fact table, large enough to show partition pruning, data skipping and small-file costs. Characteristics:
# MAGIC - 10M rows written as about 2,000 small files, so compaction has something to fix
# MAGIC - Mixed cardinalities: `country` (10 values, one per account), `product_id` (401 values, 100-500), `customer_id` (4,000 values: far too many to partition by)
# MAGIC - `unit_price` fixed per product, between 10.50 and 200.49
# MAGIC - Temporal column `sale_date` covering the two years to 30 June 2026
# MAGIC
# MAGIC ### Design Notes
# MAGIC - Data generation is idempotent: if `sales_raw` exists with 10,000,000 rows in at least 1,800 files, we skip regeneration (saves compute time)
# MAGIC - We intentionally over-partition the write (200 partitions, at most 5,000 rows per file) to create many small files and highlight gains from compaction
# MAGIC
# MAGIC ### Your Tasks
# MAGIC 1. Run the generation cell below. On a fresh catalog it writes 10M rows to `sales_raw` in about a minute and prints `sales_raw rows written: 10,000,000`. On a re-run it skips generation and prints `Table delta_optimization_project.sales_data.sales_raw already exists with 10,000,000 rows in <n> files; skipping data generation.`
# MAGIC 2. Run the check cell after it and confirm `sales_raw` holds about 2,000 files: at least 1,800 means the small files are intact.

# COMMAND ----------

# Generate the synthetic sales data with Spark (runs on the executors, no pandas or Faker)
raw_exists = spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_NAME} LIKE 'sales_raw'").count() > 0
raw_files = spark.sql(f"DESCRIBE DETAIL {tbl('raw')}").first()["numFiles"] if raw_exists else 0
raw_rows = spark.read.table(tbl('raw')).count() if raw_exists else 0

if raw_exists and raw_files >= 1800 and raw_rows == 10_000_000:
    print(f"Table {tbl('raw')} already exists with {raw_rows:,} rows in {raw_files:,} files; skipping data generation.")
else:
    if raw_exists:
        print(f"Table {tbl('raw')} holds {raw_rows:,} rows in {raw_files:,} files, not 10,000,000 rows in about 2,000 small files "
              "(compacted since it was written, or generated with different settings). Regenerating it.")

    # 10M rows: the file-count check below relies on this number
    num_records = 10_000_000

    # 200 partitions x 50,000 rows each; with maxRecordsPerFile=5000 below, each partition
    # is split into 10 files, so about 2,000 small files in total.
    num_partitions = 200

    num_customers = 4_000

    # Each account belongs to one country: countries[customer_id % 10].
    # With this order, account 1500 (the benchmark account) is in the United States.
    countries = ['United States', 'United Kingdom', 'Germany', 'France', 'Canada',
                 'Australia', 'India', 'Brazil', 'Japan', 'Netherlands']

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
                # cast the double returned by rand() to string before md5 to avoid a datatype mismatch
                F.substring(F.md5(F.rand(12345).cast('string')), 1, 8)
            )
        )
        .withColumn('customer_id', (F.floor(F.rand(42) * num_customers) + 1).cast('int'))
        .withColumn('product_id', (F.floor(F.rand(99) * 401) + 100).cast('int'))
        .withColumn('sale_date', F.date_sub(F.lit('2026-06-30').cast('date'), F.floor(F.rand(7) * 730).cast('int')))
        .withColumn('quantity', (F.floor(F.rand(11) * 10) + 1).cast('int'))
        # One fixed price per product, between 10.50 and 200.49
        .withColumn('unit_price', F.round(F.lit(10.5) + F.pmod(F.col('product_id') * 7919, F.lit(19000)) / 100.0, 2))
        .withColumn(
            'country',
            F.element_at(
                F.array(*[F.lit(c) for c in countries]),
                (F.pmod(F.col('customer_id'), F.lit(10)) + 1).cast('int')
            )
        )
    )
    # Write the data to a Delta table. maxRecordsPerFile forces small files in a distributed write.
    (df.write
       .format('delta')
       .option('maxRecordsPerFile', 5000)
       .mode('overwrite')
       .saveAsTable(tbl('raw'))
    )
    written = spark.read.table(tbl('raw')).count()
    print(f"sales_raw rows written: {written:,} (expect 10,000,000)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Small Files
# MAGIC
# MAGIC `sales_raw` should hold about 2,000 files: 200 write partitions x 10 files of 5,000 rows. Every comparison in the next two notebooks depends on that baseline.
# MAGIC
# MAGIC If the count is far lower, Databricks compacted the table in the background (Unity Catalog managed tables get background auto compaction, and predictive optimization where it is enabled), and the baseline is gone. Re-run this notebook: the generation cell regenerates `sales_raw` whenever it holds fewer than 1,800 files or not exactly 10,000,000 rows.

# COMMAND ----------

raw_files = spark.sql(f"DESCRIBE DETAIL {tbl('raw')}").first()["numFiles"]
print(f"sales_raw files: {raw_files:,} (expect about 2,000; anything from 1,800 up means the small files are intact)")

# COMMAND ----------

# MAGIC %md
# MAGIC Next: open `01_Data_Layout` to compare partitioning, Z-Order and Liquid Clustering on this table.
