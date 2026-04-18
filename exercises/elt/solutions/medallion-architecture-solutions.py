# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Medallion Architecture - Solutions
# MAGIC **Topic**: ELT | **Exercises**: 8
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "medallion_architecture"
BASE_SCHEMA = "elt"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Raw Append to Bronze Preserving All Fields
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Bronze is the raw layer - no filtering, no deduplication, no transformation
# MAGIC 2. Use CREATE OR REPLACE TABLE ... AS SELECT to write a Delta table
# MAGIC 3. Select ALL columns from the source table - every field lands in bronze as-is
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Deduplicating or filtering nulls (bronze preserves everything)
# MAGIC - Forgetting some columns (all 7 must be present)
# MAGIC - Using INSERT INTO instead of CREATE OR REPLACE TABLE (not idempotent)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex1
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_orders_ex1 AS
    SELECT
        order_id,
        customer_id,
        product_id,
        amount,
        status,
        order_date,
        updated_at
    FROM {CATALOG}.{SCHEMA}.raw_orders_ex1
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Bronze with Ingestion Metadata Columns
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `current_timestamp()` in a SELECT to generate the ingestion timestamp
# MAGIC 2. Use a string literal for the source file name: `'raw_orders_batch_001'`
# MAGIC 3. Alias the new columns as `_ingested_at` and `_source_file`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using Python's `datetime.now()` instead of Spark's `current_timestamp()` (generates one value vs per-row)
# MAGIC - Forgetting to alias the new columns with underscore prefix
# MAGIC - Filtering rows (bronze preserves all data including nulls and zeroes)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex2
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.bronze_orders_ex2 AS
    SELECT
        order_id,
        customer_id,
        product_id,
        amount,
        status,
        order_date,
        updated_at,
        current_timestamp() AS _ingested_at,
        'raw_orders_batch_001' AS _source_file
    FROM {CATALOG}.{SCHEMA}.raw_orders_ex2
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Bronze-to-Silver Dedup Using ROW_NUMBER
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) to rank duplicates
# MAGIC 2. Filter WHERE rn = 1 to keep only the latest version per order_id
# MAGIC 3. Do NOT filter NULL customer_ids - this exercise is dedup only, not cleaning
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Ordering by _ingested_at ASC instead of DESC (keeps oldest, not latest)
# MAGIC - Filtering out NULL customer_ids (exercise only asks for dedup)
# MAGIC - Using DISTINCT instead of ROW_NUMBER (DISTINCT does not let you pick WHICH duplicate)
# MAGIC - Using GROUP BY with MIN/MAX (loses non-aggregated columns)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex3
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_orders_ex3 AS
    SELECT
        order_id,
        customer_id,
        product_id,
        amount,
        status,
        order_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) AS rn
        FROM {CATALOG}.{SCHEMA}.bronze_orders_ex3
    )
    WHERE rn = 1
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Silver Data Cleaning - Nulls, Types, Standardization
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Filter with WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
# MAGIC 2. CAST(amount AS DOUBLE) converts the STRING column to numeric type
# MAGIC 3. UPPER(TRIM(status)) standardizes status: first remove whitespace, then uppercase
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Applying TRIM but forgetting UPPER (or vice versa) - both are needed
# MAGIC - Filtering nulls with != instead of IS NOT NULL (SQL null semantics)
# MAGIC - Forgetting to cast order_date from STRING to DATE
# MAGIC - Using LOWER instead of UPPER (spec requires uppercase)
# MAGIC - Casting amount after filtering (order doesn't matter here, but be explicit)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex4
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_orders_ex4 AS
    SELECT
        order_id,
        customer_id,
        product_id,
        CAST(amount AS DOUBLE) AS amount,
        UPPER(TRIM(status)) AS status,
        CAST(order_date AS DATE) AS order_date
    FROM {CATALOG}.{SCHEMA}.bronze_orders_ex4
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Silver-to-Gold Aggregation - Daily/Regional Summaries
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. GROUP BY order_date, region gives one row per date+region combination
# MAGIC 2. Use ROUND(AVG(amount), 2) to avoid floating point precision issues
# MAGIC 3. COUNT(*) for order_count, SUM(amount) for total_revenue
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting ROUND on avg_order_amount (floating point values fail assertions)
# MAGIC - Using COUNT(DISTINCT order_id) instead of COUNT(*) (some order_ids could theoretically repeat)
# MAGIC - Grouping by only one dimension (missing either order_date or region)
# MAGIC - Not including all three output columns (order_count, total_revenue, avg_order_amount)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex5
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_daily_summary_ex5 AS
    SELECT
        order_date,
        region,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        ROUND(AVG(amount), 2) AS avg_order_amount
    FROM {CATALOG}.{SCHEMA}.silver_orders_ex5
    GROUP BY order_date, region
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Incremental Bronze-to-Silver with MERGE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. First copy existing silver to a working table with CREATE OR REPLACE TABLE ... AS SELECT
# MAGIC 2. Dedup the new batch using a CTE or subquery with ROW_NUMBER before the MERGE source
# MAGIC 3. The MERGE WHEN MATCHED condition should check that source updated_at > target updated_at
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to dedup the new batch before MERGE (ORD-607 appears 3 times, causes duplicate inserts)
# MAGIC - Missing the "AND source.updated_at > target.updated_at" guard on WHEN MATCHED
# MAGIC - MERGEing directly into silver_orders_ex6 instead of a copy (corrupts the setup data)
# MAGIC - Not setting processed_at for updated rows (only setting it for inserts)
# MAGIC - Using the wrong dedup column (_ingested_at for dedup, updated_at for MERGE condition)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex6
# Step 1: Copy existing silver to working table
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_orders_ex6_merged AS
    SELECT * FROM {CATALOG}.{SCHEMA}.silver_orders_ex6
""")

# Step 2: Dedup new batch + MERGE into silver
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.silver_orders_ex6_merged AS target
    USING (
        SELECT order_id, customer_id, amount, status, updated_at
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) AS rn
            FROM {CATALOG}.{SCHEMA}.bronze_batch_ex6
        )
        WHERE rn = 1
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND source.updated_at > target.updated_at THEN
        UPDATE SET
            target.customer_id = source.customer_id,
            target.amount = source.amount,
            target.status = source.status,
            target.updated_at = source.updated_at,
            target.processed_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id, amount, status, updated_at, processed_at)
        VALUES (source.order_id, source.customer_id, source.amount, source.status, source.updated_at, current_timestamp())
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Gold Incremental Refresh with MERGE
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. First copy existing gold to a working table
# MAGIC 2. Aggregate silver_new_ex7 by (order_date, region) to get batch-level totals
# MAGIC 3. MERGE matching on BOTH order_date AND region (composite key)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Matching on only order_date or only region (need both for the composite key)
# MAGIC - Trying to ADD to existing gold values instead of replacing (spec says update with new batch values)
# MAGIC - Forgetting ROUND on avg_order_amount (floating point mismatch)
# MAGIC - MERGEing directly into gold_summary_ex7 instead of a copy
# MAGIC - Not aggregating silver before MERGE (merging raw silver rows into gold aggregates)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex7
# Step 1: Copy existing gold to working table
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_summary_ex7_merged AS
    SELECT * FROM {CATALOG}.{SCHEMA}.gold_summary_ex7
""")

# Step 2: Aggregate new silver + MERGE into gold
spark.sql(f"""
    MERGE INTO {CATALOG}.{SCHEMA}.gold_summary_ex7_merged AS target
    USING (
        SELECT
            order_date,
            region,
            CAST(COUNT(*) AS INT) AS order_count,
            SUM(amount) AS total_revenue,
            ROUND(AVG(amount), 2) AS avg_order_amount
        FROM {CATALOG}.{SCHEMA}.silver_new_ex7
        GROUP BY order_date, region
    ) AS source
    ON target.order_date = source.order_date AND target.region = source.region
    WHEN MATCHED THEN
        UPDATE SET
            target.order_count = source.order_count,
            target.total_revenue = source.total_revenue,
            target.avg_order_amount = source.avg_order_amount
    WHEN NOT MATCHED THEN
        INSERT (order_date, region, order_count, total_revenue, avg_order_amount)
        VALUES (source.order_date, source.region, source.order_count, source.total_revenue, source.avg_order_amount)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Data Quality Report Across Tiers
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run six separate checks and UNION ALL the results into one table
# MAGIC 2. For orphan check: LEFT ANTI JOIN qc_silver_ex8 with qc_customers_ex8 on customer_id
# MAGIC 3. Cast to DOUBLE before dividing to avoid integer division (e.g., 10 / 12 = 0 in integer math)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Integer division: (12 - 3) / 12 = 0 in SQL integer math. Cast numerator to DOUBLE first.
# MAGIC - Forgetting to UNION ALL all six checks into one table
# MAGIC - Mixing up total_rows between tiers (bronze=12, silver=10, gold=8)
# MAGIC - Using BETWEEN for date range (correct here, but be careful with date vs timestamp)
# MAGIC - Using LEFT JOIN + IS NULL instead of LEFT ANTI JOIN for orphans (both work, but anti is cleaner)

# COMMAND ----------

# EXERCISE_KEY: medallion_ex8
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.quality_report_ex8 AS

    -- Bronze check 1: null order_ids
    SELECT
        'bronze' AS tier,
        'null_order_id' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8 WHERE order_id IS NULL) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8 WHERE order_id IS NULL))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS pass_rate

    UNION ALL

    -- Bronze check 2: out-of-range order_date
    SELECT
        'bronze' AS tier,
        'invalid_date_range' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8
         WHERE order_date < CAST('2026-01-01' AS DATE)
            OR order_date > CAST('2027-12-31' AS DATE)) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8
                WHERE order_date < CAST('2026-01-01' AS DATE)
                   OR order_date > CAST('2027-12-31' AS DATE)))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS pass_rate

    UNION ALL

    -- Bronze check 3: negative amounts
    SELECT
        'bronze' AS tier,
        'negative_amount' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8 WHERE amount < 0) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8 WHERE amount < 0))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_bronze_ex8) AS pass_rate

    UNION ALL

    -- Silver check: orphan customer_ids
    SELECT
        'silver' AS tier,
        'orphan_customer_id' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_silver_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_silver_ex8 s
         LEFT ANTI JOIN {CATALOG}.{SCHEMA}.qc_customers_ex8 c ON s.customer_id = c.customer_id) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_silver_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_silver_ex8 s
                LEFT ANTI JOIN {CATALOG}.{SCHEMA}.qc_customers_ex8 c ON s.customer_id = c.customer_id))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_silver_ex8) AS pass_rate

    UNION ALL

    -- Gold check 1: zero order_count
    SELECT
        'gold' AS tier,
        'zero_order_count' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8 WHERE order_count = 0) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8 WHERE order_count = 0))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8) AS pass_rate

    UNION ALL

    -- Gold check 2: negative revenue
    SELECT
        'gold' AS tier,
        'negative_revenue' AS check_name,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8) AS INT) AS total_rows,
        CAST((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8 WHERE total_revenue < 0) AS INT) AS failed_rows,
        CAST(
            ((SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8)
             - (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8 WHERE total_revenue < 0))
            AS DOUBLE
        ) / (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.qc_gold_ex8) AS pass_rate
""")
