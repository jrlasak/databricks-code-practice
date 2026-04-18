# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Spark SQL Joins & Aggregations - Solutions
# MAGIC **Topic**: ELT | **Exercises**: 9
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "spark_sql_joins"
BASE_SCHEMA = "elt"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join Orders with Customers
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use INNER JOIN (or just JOIN) to match rows that exist in both tables
# MAGIC 2. Write the query result directly as a Delta table using CREATE OR REPLACE TABLE ... AS SELECT
# MAGIC 3. Use table aliases (o, c) to disambiguate column names
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using LEFT JOIN instead of INNER JOIN (would include orders with NULL customer_id)
# MAGIC - Forgetting to alias columns that exist in both tables (ambiguous column error)
# MAGIC - Not using the three-level namespace (catalog.schema.table)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex1
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex1_output AS
    SELECT
        o.order_id,
        o.customer_id,
        o.amount,
        o.status,
        o.order_date,
        c.name,
        c.region,
        c.tier
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON o.customer_id = c.customer_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Left Join with COALESCE for Null Handling
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. LEFT JOIN preserves ALL rows from the left table (orders), even when no match exists
# MAGIC 2. Use COALESCE(column, 'default') to replace NULLs with a default value
# MAGIC 3. Orders with NULL customer_id will have NULL for all customer columns after the join
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using INNER JOIN (drops orders with no matching customer)
# MAGIC - Applying COALESCE to customer_id instead of name (customer_id can legitimately be NULL)
# MAGIC - Forgetting that COALESCE needs the replacement value as a string literal, not a column

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex2
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex2_output AS
    SELECT
        o.order_id,
        o.customer_id,
        o.amount,
        o.status,
        o.order_date,
        COALESCE(c.name, 'Unknown') AS name,
        c.region
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    LEFT JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON o.customer_id = c.customer_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Anti-Join to Find Orphan Order Items
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. LEFT ANTI JOIN returns rows from the left table that have NO match in the right table
# MAGIC 2. This is a Spark SQL extension - standard SQL would use NOT EXISTS or NOT IN subqueries
# MAGIC 3. Think of it as "give me everything in A that is NOT in B"
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using LEFT JOIN + WHERE IS NULL (works but misses the point - anti-join is cleaner and faster)
# MAGIC - Using NOT IN with a subquery (breaks if the subquery returns NULLs)
# MAGIC - Joining on the wrong column (must be order_id, not product_id)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex3
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex3_output AS
    SELECT oi.*
    FROM {CATALOG}.{BASE_SCHEMA}.order_items oi
    LEFT ANTI JOIN {CATALOG}.{BASE_SCHEMA}.orders o
        ON oi.order_id = o.order_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Semi-Join to Find Customers Who Have Orders
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. LEFT SEMI JOIN returns rows from the left table that have at least one match in the right table
# MAGIC 2. Unlike INNER JOIN, semi-join never duplicates rows and returns only left-table columns
# MAGIC 3. Think of it as an EXISTS check expressed as a join
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using INNER JOIN (duplicates customers who have multiple orders)
# MAGIC - Using SELECT DISTINCT after INNER JOIN (works but semi-join is cleaner and faster)
# MAGIC - Including order columns in the SELECT (semi-join only returns left-table columns)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex4
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex4_output AS
    SELECT c.*
    FROM {CATALOG}.{BASE_SCHEMA}.customers c
    LEFT SEMI JOIN {CATALOG}.{BASE_SCHEMA}.orders o
        ON c.customer_id = o.customer_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Self-Join to Find Same-Day Orders by Same Customer
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Self-join means joining a table with itself - use two different aliases (o1, o2)
# MAGIC 2. Join on customer_id AND order_date to find same-day orders by the same customer
# MAGIC 3. Use o1.order_id < o2.order_id to avoid pairing an order with itself and to deduplicate pairs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using o1.order_id != o2.order_id instead of < (produces each pair twice: A-B and B-A)
# MAGIC - Forgetting to filter out NULL customer_ids (NULL = NULL is false in SQL, but be explicit)
# MAGIC - Joining only on customer_id without order_date (finds all orders by same customer, not just same-day)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex5
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex5_output AS
    SELECT
        o1.customer_id,
        o1.order_date,
        o1.order_id AS order_id_1,
        o2.order_id AS order_id_2,
        o1.amount AS amount_1,
        o2.amount AS amount_2
    FROM {CATALOG}.{BASE_SCHEMA}.orders o1
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.orders o2
        ON o1.customer_id = o2.customer_id
       AND o1.order_date = o2.order_date
       AND o1.order_id < o2.order_id
    WHERE o1.customer_id IS NOT NULL
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Table Join Chain (Orders + Customers + Products + Order Items)
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Chain JOINs sequentially: orders -> customers -> order_items -> products
# MAGIC 2. QUALIFY filters window function results without a subquery (Databricks SQL extension)
# MAGIC 3. ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1 keeps the first row per group
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Joining products on orders.product_id instead of order_items.product_id (order_items has the actual line items)
# MAGIC - Forgetting QUALIFY and using a subquery with WHERE rn = 1 (works but QUALIFY is cleaner)
# MAGIC - Not aliasing both name columns (customers.name and products.name would collide)
# MAGIC - Using PARTITION BY without ORDER BY in ROW_NUMBER (non-deterministic results)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex6
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex6_output AS
    SELECT
        o.order_id,
        c.name AS customer_name,
        c.region,
        oi.quantity,
        oi.unit_price,
        p.name AS product_name,
        p.category,
        o.status
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON o.customer_id = c.customer_id
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.order_items oi
        ON o.order_id = oi.order_id
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.products p
        ON oi.product_id = p.product_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY oi.quantity DESC) = 1
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: GROUP BY with HAVING to Find High-Value Customers
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. GROUP BY aggregates rows - use it with SUM, COUNT, AVG to get per-customer totals
# MAGIC 2. HAVING filters groups after aggregation (WHERE filters rows before aggregation)
# MAGIC 3. Join the aggregated result with customers to get name and region
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using WHERE instead of HAVING for the total_spent filter (WHERE runs before GROUP BY)
# MAGIC - Forgetting to exclude NULL customer_ids (they create a single group of unattributed orders)
# MAGIC - Joining before aggregating without re-grouping (produces wrong counts if customers have multiple orders)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex7
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex7_output AS
    SELECT
        agg.customer_id,
        c.name,
        c.region,
        agg.order_count,
        agg.total_spent,
        agg.avg_order_value
    FROM (
        SELECT
            customer_id,
            COUNT(*) AS order_count,
            SUM(amount) AS total_spent,
            AVG(amount) AS avg_order_value
        FROM {CATALOG}.{BASE_SCHEMA}.orders
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        HAVING SUM(amount) > 500
    ) agg
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON agg.customer_id = c.customer_id
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: ROLLUP for Hierarchical Subtotals by Region and Tier
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. GROUP BY ROLLUP(a, b) produces: (a, b), (a, NULL), (NULL, NULL) - hierarchical subtotals
# MAGIC 2. NULL in a grouping column means "all values" (it is a subtotal/total row)
# MAGIC 3. ROLLUP respects column order - ROLLUP(region, tier) gives per-region subtotals but NOT per-tier subtotals
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using GROUP BY region, tier (no subtotals - just regular grouping)
# MAGIC - Confusing ROLLUP with CUBE (ROLLUP is hierarchical left-to-right, CUBE is all combinations)
# MAGIC - Not filtering to completed orders before aggregating (inflates revenue with pending/cancelled)
# MAGIC - Forgetting that ROLLUP(region, tier) does NOT produce per-tier-only rows

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex8
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex8_rollup AS
    SELECT
        c.region,
        c.tier,
        COUNT(*) AS order_count,
        SUM(o.amount) AS total_revenue,
        AVG(o.amount) AS avg_order_value
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON o.customer_id = c.customer_id
    WHERE o.status = 'completed'
    GROUP BY ROLLUP(c.region, c.tier)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: CUBE for All-Combination Subtotals Across Region and Status
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. GROUP BY CUBE(a, b) produces: (a, b), (a, NULL), (NULL, b), (NULL, NULL) - all combinations
# MAGIC 2. CUBE adds per-column subtotals that ROLLUP does not (e.g., per-status totals across all regions)
# MAGIC 3. The grand total row has NULL for all grouping columns and matches the unfiltered aggregate
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing CUBE and ROLLUP (CUBE produces per-status rows that ROLLUP would not)
# MAGIC - Using WHERE instead of HAVING for post-aggregation filtering
# MAGIC - Assuming CUBE output is ordered (it is not - use ORDER BY if you need sorted output)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex9
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.joins_ex9_cube AS
    SELECT
        c.region,
        o.status,
        COUNT(*) AS order_count,
        SUM(o.amount) AS total_revenue,
        AVG(o.amount) AS avg_order_value
    FROM {CATALOG}.{BASE_SCHEMA}.orders o
    INNER JOIN {CATALOG}.{BASE_SCHEMA}.customers c
        ON o.customer_id = c.customer_id
    GROUP BY CUBE(c.region, o.status)
""")
