# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Window Functions - Solutions
# MAGIC **Topic**: ELT | **Exercises**: 8
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "window_functions"
BASE_SCHEMA = "elt"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: ROW_NUMBER Dedup - Keep Latest Order per Customer
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. ROW_NUMBER() assigns sequential integers within each partition
# MAGIC 2. Databricks supports the QUALIFY clause to filter window function results inline
# MAGIC 3. Filter out NULL customer_id before or after the window function
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using RANK() instead of ROW_NUMBER() when there are ties (RANK gives the same number to ties, ROW_NUMBER does not)
# MAGIC - Forgetting to exclude NULL customer_id (would create an extra "null group" row)
# MAGIC - Leaving the row_number column in the output (should be dropped)
# MAGIC - Ordering by updated_at ASC instead of DESC (keeps oldest, not newest)

# COMMAND ----------

# EXERCISE_KEY: window_ex1
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex1_deduped AS
    SELECT order_id, customer_id, product_id, amount, status, order_date, updated_at
    FROM {CATALOG}.{SCHEMA}.window_ex1_orders
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: RANK vs DENSE_RANK - Rank Customers by Order Count
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. First aggregate to get order_count per customer, then apply window functions
# MAGIC 2. RANK() leaves gaps after ties (1, 2, 2, 4), DENSE_RANK() does not (1, 2, 2, 3)
# MAGIC 3. You can use a CTE or subquery to aggregate first, then rank in the outer query
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to apply RANK to individual order rows instead of aggregating first
# MAGIC - Ordering ASC instead of DESC (ranks lowest count as 1 instead of highest)
# MAGIC - Using only one of RANK/DENSE_RANK when the exercise requires both
# MAGIC - Confusing RANK with ROW_NUMBER (ROW_NUMBER always assigns unique values, RANK allows ties)

# COMMAND ----------

# EXERCISE_KEY: window_ex2
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex2_ranked AS
    WITH customer_counts AS (
        SELECT
            customer_id,
            CAST(COUNT(*) AS BIGINT) AS order_count
        FROM {CATALOG}.{SCHEMA}.window_ex2_orders
        GROUP BY customer_id
    )
    SELECT
        customer_id,
        order_count,
        CAST(RANK() OVER (ORDER BY order_count DESC) AS BIGINT) AS rank_val,
        CAST(DENSE_RANK() OVER (ORDER BY order_count DESC) AS BIGINT) AS dense_rank_val
    FROM customer_counts
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: LAG for Period-Over-Period - Order Amount Change
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. LAG(column, 1) returns the value from the previous row in the window
# MAGIC 2. Partition by customer so each customer's orders are compared independently
# MAGIC 3. The first order per customer will have NULL for LAG (no previous row)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using LEAD instead of LAG (LEAD looks forward, LAG looks backward)
# MAGIC - Forgetting to ORDER BY order_date (window functions need deterministic ordering)
# MAGIC - Trying to replace NULL amount_change with 0 (the spec says NULL for first order)
# MAGIC - Not handling the $0 amount edge case (amount_change = 0 - 80 = -80, not an error)

# COMMAND ----------

# EXERCISE_KEY: window_ex3
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex3_changes AS
    SELECT
        order_id,
        customer_id,
        amount,
        order_date,
        LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_amount,
        amount - LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS amount_change
    FROM {CATALOG}.{SCHEMA}.window_ex3_orders
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Running Totals with Explicit ROWS BETWEEN Frame
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. SUM() with ORDER BY already defaults to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC 2. The exercise requires you to write the frame spec explicitly (not rely on the default)
# MAGIC 3. ROWS vs RANGE: ROWS counts physical rows, RANGE considers logical values (ROWS is safer for running totals)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Omitting the frame spec (it works due to defaults, but the exercise tests explicit framing)
# MAGIC - Using RANGE instead of ROWS (RANGE would group ties differently, though not an issue with unique dates)
# MAGIC - Using UNBOUNDED FOLLOWING instead of CURRENT ROW (gives the partition total for every row)
# MAGIC - Forgetting PARTITION BY (calculates running total across ALL customers, not per customer)

# COMMAND ----------

# EXERCISE_KEY: window_ex4
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex4_running_totals AS
    SELECT
        order_id,
        customer_id,
        amount,
        order_date,
        SUM(amount) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total
    FROM {CATALOG}.{SCHEMA}.window_ex4_orders
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: NTILE Quartile Bucketing by Total Spend
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. NTILE(4) divides the ordered result set into 4 roughly equal groups
# MAGIC 2. With 8 rows and NTILE(4), each group gets exactly 2 rows
# MAGIC 3. Order by total_spend ascending so quartile 1 = lowest spenders
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Ordering DESC instead of ASC (inverts quartile meaning: Q1 would be highest spenders)
# MAGIC - Using PERCENT_RANK or CUME_DIST (these return 0-1 values, not bucket numbers)
# MAGIC - Adding PARTITION BY when the bucketing should be across all customers globally
# MAGIC - Using NTILE(100) for percentiles when NTILE(4) for quartiles was requested

# COMMAND ----------

# EXERCISE_KEY: window_ex5
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex5_quartiles AS
    SELECT
        customer_id,
        total_spend,
        NTILE(4) OVER (ORDER BY total_spend ASC) AS spend_quartile
    FROM {CATALOG}.{SCHEMA}.window_ex5_customer_spend
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: FIRST_VALUE/LAST_VALUE - First and Most Recent Order
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. FIRST_VALUE returns the first value in the window frame - with ORDER BY order_date, that is the earliest order
# MAGIC 2. LAST_VALUE requires an explicit frame ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC 3. Without the explicit frame, LAST_VALUE only sees up to the current row (not the actual last)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting the frame spec for LAST_VALUE (default frame ends at CURRENT ROW, so LAST_VALUE = current row)
# MAGIC - Using MAX/MIN instead of FIRST_VALUE/LAST_VALUE (these would work for amounts but not for order_id correlation)
# MAGIC - Confusing the frame for FIRST_VALUE vs LAST_VALUE (FIRST_VALUE works with default frame; LAST_VALUE does not)
# MAGIC - Not applying the same frame to both LAST_VALUE calls (inconsistent results)

# COMMAND ----------

# EXERCISE_KEY: window_ex6
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex6_first_last AS
    SELECT
        order_id,
        customer_id,
        amount,
        order_date,
        FIRST_VALUE(order_id) OVER (
            PARTITION BY customer_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_order_id,
        FIRST_VALUE(amount) OVER (
            PARTITION BY customer_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_order_amount,
        LAST_VALUE(order_id) OVER (
            PARTITION BY customer_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_order_id,
        LAST_VALUE(amount) OVER (
            PARTITION BY customer_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_order_amount
    FROM {CATALOG}.{SCHEMA}.window_ex6_orders
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: QUALIFY for Top-N Per Group Without Subquery
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. QUALIFY is a Databricks SQL extension that filters window function results inline (like HAVING for GROUP BY)
# MAGIC 2. Use ROW_NUMBER partitioned by customer_id, ordered by amount DESC
# MAGIC 3. QUALIFY ROW_NUMBER() ... <= 2 keeps the top 2 per group without needing a subquery or CTE
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Wrapping in a CTE/subquery instead of using QUALIFY (works, but misses the point of the exercise)
# MAGIC - Using RANK instead of ROW_NUMBER (RANK would keep more than 2 rows if there are ties)
# MAGIC - Leaving the row_number column in the output (SELECT should list only original columns)
# MAGIC - Ordering amount ASC instead of DESC (gets bottom 2 instead of top 2)

# COMMAND ----------

# EXERCISE_KEY: window_ex7
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex7_top2 AS
    SELECT order_id, customer_id, amount, order_date
    FROM {CATALOG}.{SCHEMA}.window_ex7_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) <= 2
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Sessionization - Assign Session IDs Based on 30-Min Gaps
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Step 1: Use LAG to get the previous event timestamp per user
# MAGIC 2. Step 2: Flag rows where the gap exceeds 30 minutes (or there is no previous event)
# MAGIC 3. Step 3: Cumulative SUM of the flags gives you session IDs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using minutes incorrectly with unix_timestamp: `(unix_timestamp(event_ts) - unix_timestamp(prev_ts)) / 60 > 30`
# MAGIC - Forgetting that the FIRST event per user should start session 1 (no previous event = new session)
# MAGIC - Using DENSE_RANK or ROW_NUMBER instead of cumulative SUM (these don't account for gap logic)
# MAGIC - Calculating the gap across users (must PARTITION BY user_id)
# MAGIC - Off-by-one: a gap of exactly 30 minutes should NOT start a new session (must be >30)

# COMMAND ----------

# EXERCISE_KEY: window_ex8
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.window_ex8_sessions AS
    WITH with_prev AS (
        SELECT *,
            LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_event_ts
        FROM {CATALOG}.{SCHEMA}.window_ex8_events
    ),
    with_flag AS (
        SELECT *,
            CASE
                WHEN prev_event_ts IS NULL THEN 1
                WHEN (unix_timestamp(event_ts) - unix_timestamp(prev_event_ts)) / 60 > 30 THEN 1
                ELSE 0
            END AS new_session_flag
        FROM with_prev
    )
    SELECT
        event_id,
        user_id,
        event_type,
        event_ts,
        payload,
        CAST(SUM(new_session_flag) OVER (PARTITION BY user_id ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) AS session_id
    FROM with_flag
""")
