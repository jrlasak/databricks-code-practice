# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Window Functions
# MAGIC **Topic**: ELT | **Exercises**: 8 | **Total Time**: ~80 min
# MAGIC
# MAGIC Practice Spark SQL window functions: ROW_NUMBER dedup with QUALIFY, RANK vs DENSE_RANK
# MAGIC tie handling, LAG for period-over-period, running totals with explicit frame specs,
# MAGIC NTILE quartile bucketing, FIRST_VALUE/LAST_VALUE, QUALIFY for top-N, and sessionization.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/window-functions-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from setup):
# MAGIC - `db_code.window_functions.window_ex1_orders` - orders with multiple per customer (dedup)
# MAGIC - `db_code.window_functions.window_ex2_orders` - orders per customer (RANK vs DENSE_RANK)
# MAGIC - `db_code.window_functions.window_ex3_orders` - orders per customer (LAG comparison)
# MAGIC - `db_code.window_functions.window_ex4_orders` - orders for running totals
# MAGIC - `db_code.window_functions.window_ex5_customer_spend` - aggregated customer spend (NTILE)
# MAGIC - `db_code.window_functions.window_ex6_orders` - orders for FIRST_VALUE/LAST_VALUE
# MAGIC - `db_code.window_functions.window_ex7_orders` - orders for QUALIFY top-N
# MAGIC - `db_code.window_functions.window_ex8_events` - clickstream events (sessionization)

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/window-functions-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Exercise tables are in `db_code.window_functions`.
# MAGIC Each exercise reads from its own source table and writes results to a separate output table.
# MAGIC - Ex 1 (easy): `window_ex1_orders` - 12 rows, 7 customers (some with multiple orders, 1 NULL customer_id)
# MAGIC - Ex 2 (easy): `window_ex2_orders` - 20 rows, 6 customers with varying order counts (ties exist)
# MAGIC - Ex 3 (medium): `window_ex3_orders` - 12 rows, 3 customers with 4 orders each
# MAGIC - Ex 4 (medium): `window_ex4_orders` - 10 rows, 3 customers with 3-4 orders each
# MAGIC - Ex 5 (medium): `window_ex5_customer_spend` - 8 customers with total spend
# MAGIC - Ex 6 (medium): `window_ex6_orders` - 15 rows, 4 customers with 2-5 orders each
# MAGIC - Ex 7 (medium): `window_ex7_orders` - 12 rows, 3 customers with 4 orders each
# MAGIC - Ex 8 (hard): `window_ex8_events` - 15 clickstream events across 2 users

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: ROW_NUMBER Dedup - Keep Latest Order per Customer
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Deduplicate orders keeping only the most recent order per customer.
# MAGIC Use ROW_NUMBER with QUALIFY to filter inline.
# MAGIC
# MAGIC **Source** (`window_ex1_orders`): 12 rows across 7 customers. CUST-001 has 3 orders,
# MAGIC CUST-002 has 2 orders, CUST-003 has 2 orders. One row has NULL customer_id.
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex1_deduped`. 7 rows (one per customer,
# MAGIC excluding the NULL customer_id row).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use ROW_NUMBER() partitioned by `customer_id`, ordered by `updated_at` DESC
# MAGIC 2. Use QUALIFY to keep only the latest order per customer (row_number = 1)
# MAGIC 3. Exclude rows where `customer_id` IS NULL
# MAGIC 4. Write all original columns (no row_number column in output) to a Delta table
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Input has 12 rows, output must have exactly 7
# MAGIC - CUST-001's latest order is ORD-011 (updated_at 2026-03-05)
# MAGIC - CUST-003's latest order is ORD-010 (updated_at 2026-02-28)

# COMMAND ----------

# EXERCISE_KEY: window_ex1
# TODO: Deduplicate orders keeping latest per customer using ROW_NUMBER + QUALIFY

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex1_deduped")

assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
assert result.filter("customer_id IS NULL").count() == 0, "NULL customer_id rows should be excluded"
assert result.filter("customer_id = 'CUST-001'").count() == 1, "CUST-001 should have exactly 1 row"
assert result.filter("customer_id = 'CUST-001'").select("order_id").collect()[0][0] == "ORD-011", \
    "CUST-001 latest order should be ORD-011"
assert result.filter("customer_id = 'CUST-002'").select("order_id").collect()[0][0] == "ORD-007", \
    "CUST-002 latest order should be ORD-007"
assert result.filter("customer_id = 'CUST-003'").select("order_id").collect()[0][0] == "ORD-010", \
    "CUST-003 latest order should be ORD-010"
assert "row_number" not in [c.lower() for c in result.columns] and "rn" not in [c.lower() for c in result.columns], \
    "Output should not contain the row_number column"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: RANK vs DENSE_RANK - Rank Customers by Order Count
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Rank customers by their number of orders. Show the difference between RANK and
# MAGIC DENSE_RANK when there are ties in order count.
# MAGIC
# MAGIC **Source** (`window_ex2_orders`): 20 rows across 6 customers.
# MAGIC Order counts: CUST-001=5, CUST-002=4, CUST-003=4, CUST-004=3, CUST-005=3, CUST-006=1.
# MAGIC Ties at 4 orders (CUST-002, CUST-003) and 3 orders (CUST-004, CUST-005).
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex2_ranked`. 6 rows with columns:
# MAGIC `customer_id`, `order_count`, `rank_val`, `dense_rank_val`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Aggregate orders by customer_id to get order_count
# MAGIC 2. Apply RANK() ordered by order_count DESC as `rank_val`
# MAGIC 3. Apply DENSE_RANK() ordered by order_count DESC as `dense_rank_val`
# MAGIC 4. Write to Delta table
# MAGIC
# MAGIC **Constraints**:
# MAGIC - 6 rows in output (one per customer)
# MAGIC - CUST-001 (5 orders): rank_val=1, dense_rank_val=1
# MAGIC - CUST-002 and CUST-003 (4 orders each): rank_val=2, dense_rank_val=2
# MAGIC - CUST-004 and CUST-005 (3 orders each): rank_val=4 (not 3!), dense_rank_val=3
# MAGIC - CUST-006 (1 order): rank_val=6, dense_rank_val=4

# COMMAND ----------

# EXERCISE_KEY: window_ex2
# TODO: Rank customers by order count showing RANK vs DENSE_RANK difference

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex2_ranked")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "order_count" in result.columns, "Missing order_count column"
assert "rank_val" in result.columns, "Missing rank_val column"
assert "dense_rank_val" in result.columns, "Missing dense_rank_val column"

# CUST-001: 5 orders, rank 1, dense_rank 1
c1 = result.filter("customer_id = 'CUST-001'").collect()[0]
assert c1.order_count == 5, f"CUST-001 order_count should be 5, got {c1.order_count}"
assert c1.rank_val == 1, f"CUST-001 rank_val should be 1, got {c1.rank_val}"
assert c1.dense_rank_val == 1, f"CUST-001 dense_rank_val should be 1, got {c1.dense_rank_val}"

# CUST-004: 3 orders, rank 4 (gap!), dense_rank 3 (no gap)
c4 = result.filter("customer_id = 'CUST-004'").collect()[0]
assert c4.order_count == 3, f"CUST-004 order_count should be 3, got {c4.order_count}"
assert c4.rank_val == 4, f"CUST-004 rank_val should be 4 (gap after tied 2nd), got {c4.rank_val}"
assert c4.dense_rank_val == 3, f"CUST-004 dense_rank_val should be 3 (no gap), got {c4.dense_rank_val}"

# CUST-006: 1 order, rank 6, dense_rank 4
c6 = result.filter("customer_id = 'CUST-006'").collect()[0]
assert c6.rank_val == 6, f"CUST-006 rank_val should be 6, got {c6.rank_val}"
assert c6.dense_rank_val == 4, f"CUST-006 dense_rank_val should be 4, got {c6.dense_rank_val}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: LAG for Period-Over-Period - Order Amount Change
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Calculate the order-over-order amount change per customer using LAG.
# MAGIC For each order, compute the difference from the previous order's amount (by date).
# MAGIC
# MAGIC **Source** (`window_ex3_orders`): 12 rows. 3 customers (CUST-001, CUST-002, CUST-003)
# MAGIC with 4 orders each, ordered by `order_date`.
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex3_changes`. 12 rows with columns:
# MAGIC `order_id`, `customer_id`, `amount`, `order_date`, `prev_amount`, `amount_change`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use LAG(amount) partitioned by `customer_id`, ordered by `order_date`
# MAGIC 2. Add `prev_amount` column (the previous order's amount, NULL for first order)
# MAGIC 3. Add `amount_change` column = `amount - prev_amount` (NULL for first order)
# MAGIC 4. Write to Delta table with all 6 columns
# MAGIC
# MAGIC **Constraints**:
# MAGIC - First order per customer has NULL prev_amount and NULL amount_change
# MAGIC - CUST-002's second order has amount=0.00, so amount_change = 0.00 - 80.00 = -80.00

# COMMAND ----------

# EXERCISE_KEY: window_ex3
# TODO: Calculate order-over-order amount change per customer using LAG

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex3_changes")

assert result.count() == 12, f"Expected 12 rows, got {result.count()}"
assert "prev_amount" in result.columns, "Missing prev_amount column"
assert "amount_change" in result.columns, "Missing amount_change column"

# First order per customer should have NULL prev_amount
first_orders = result.filter("prev_amount IS NULL")
assert first_orders.count() == 3, f"Expected 3 rows with NULL prev_amount (first per customer), got {first_orders.count()}"

# CUST-002 second order: amount=0, prev=80, change=-80
cust2_second = result.filter("order_id = 'ORD-306'").collect()[0]
assert cust2_second.prev_amount == 80.00, f"ORD-306 prev_amount should be 80.00, got {cust2_second.prev_amount}"
assert cust2_second.amount_change == -80.00, f"ORD-306 amount_change should be -80.00, got {cust2_second.amount_change}"

# CUST-001 last order: amount=200, prev=120, change=80
cust1_last = result.filter("order_id = 'ORD-304'").collect()[0]
assert cust1_last.amount_change == 80.00, f"ORD-304 amount_change should be 80.00, got {cust1_last.amount_change}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Running Totals with Explicit ROWS BETWEEN Frame
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Calculate a cumulative order amount per customer using an explicit window frame.
# MAGIC The running total should include all rows from the start of the partition up to the current row.
# MAGIC
# MAGIC **Source** (`window_ex4_orders`): 10 rows. CUST-001 (3 orders), CUST-002 (4 orders),
# MAGIC CUST-003 (3 orders).
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex4_running_totals`. 10 rows with columns:
# MAGIC `order_id`, `customer_id`, `amount`, `order_date`, `running_total`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use SUM(amount) with PARTITION BY `customer_id`, ORDER BY `order_date`
# MAGIC 2. Use explicit frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC 3. Add `running_total` column with the cumulative sum
# MAGIC 4. Write to Delta table
# MAGIC
# MAGIC **Constraints**:
# MAGIC - CUST-001 running totals: 100.00, 250.00, 450.00
# MAGIC - CUST-002 final running total: 80 + 120 + 60 + 90 = 350.00

# COMMAND ----------

# EXERCISE_KEY: window_ex4
# TODO: Calculate cumulative order amount per customer with explicit frame specification

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex4_running_totals")

assert result.count() == 10, f"Expected 10 rows, got {result.count()}"
assert "running_total" in result.columns, "Missing running_total column"

# CUST-001 first order: running total = 100.00
cust1_first = result.filter("order_id = 'ORD-401'").select("running_total").collect()[0][0]
assert cust1_first == 100.00, f"CUST-001 first running_total should be 100.00, got {cust1_first}"

# CUST-001 last order: running total = 100 + 150 + 200 = 450.00
cust1_last = result.filter("order_id = 'ORD-403'").select("running_total").collect()[0][0]
assert cust1_last == 450.00, f"CUST-001 final running_total should be 450.00, got {cust1_last}"

# CUST-002 final running total: 80 + 120 + 60 + 90 = 350.00
cust2_last = result.filter("order_id = 'ORD-407'").select("running_total").collect()[0][0]
assert cust2_last == 350.00, f"CUST-002 final running_total should be 350.00, got {cust2_last}"

# CUST-003 mid-point: 200 + 175 = 375.00
cust3_mid = result.filter("order_id = 'ORD-409'").select("running_total").collect()[0][0]
assert cust3_mid == 375.00, f"CUST-003 second running_total should be 375.00, got {cust3_mid}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: NTILE Quartile Bucketing by Total Spend
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Bucket customers into quartiles by total spend using NTILE(4).
# MAGIC Quartile 1 = lowest spenders, Quartile 4 = highest spenders.
# MAGIC
# MAGIC **Source** (`window_ex5_customer_spend`): 8 customers with pre-aggregated `total_spend`.
# MAGIC Ordered lowest to highest: 75, 150, 450, 950, 1200, 2800, 3500, 5000.
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex5_quartiles`. 8 rows with columns:
# MAGIC `customer_id`, `total_spend`, `spend_quartile`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use NTILE(4) ordered by `total_spend` ascending
# MAGIC 2. Add `spend_quartile` column (1 = lowest, 4 = highest)
# MAGIC 3. Write to Delta table
# MAGIC
# MAGIC **Constraints**:
# MAGIC - CUST-008 (spend=75) should be in quartile 1
# MAGIC - CUST-007 (spend=5000) should be in quartile 4
# MAGIC - 8 customers / 4 quartiles = 2 customers per quartile

# COMMAND ----------

# EXERCISE_KEY: window_ex5
# TODO: Bucket customers into quartiles by total spend using NTILE

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex5_quartiles")

assert result.count() == 8, f"Expected 8 rows, got {result.count()}"
assert "spend_quartile" in result.columns, "Missing spend_quartile column"

# Lowest spender should be quartile 1
q1 = result.filter("customer_id = 'CUST-008'").select("spend_quartile").collect()[0][0]
assert q1 == 1, f"CUST-008 (lowest spend) should be quartile 1, got {q1}"

# Highest spender should be quartile 4
q4 = result.filter("customer_id = 'CUST-007'").select("spend_quartile").collect()[0][0]
assert q4 == 4, f"CUST-007 (highest spend) should be quartile 4, got {q4}"

# Each quartile should have exactly 2 customers (8 / 4 = 2)
from pyspark.sql import functions as F
quartile_counts = result.groupBy("spend_quartile").count().collect()
for row in quartile_counts:
    assert row["count"] == 2, f"Quartile {row['spend_quartile']} should have 2 customers, got {row['count']}"

# CUST-006 (spend=150) should be quartile 1
q1b = result.filter("customer_id = 'CUST-006'").select("spend_quartile").collect()[0][0]
assert q1b == 1, f"CUST-006 (spend=150) should be quartile 1, got {q1b}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: FIRST_VALUE/LAST_VALUE - First and Most Recent Order
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Find each customer's first and most recent order details in a single query using
# MAGIC FIRST_VALUE and LAST_VALUE window functions.
# MAGIC
# MAGIC **Source** (`window_ex6_orders`): 15 rows across 4 customers.
# MAGIC CUST-001: 5 orders, CUST-002: 4 orders, CUST-003: 4 orders, CUST-004: 2 orders.
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex6_first_last`. 15 rows with columns:
# MAGIC `order_id`, `customer_id`, `amount`, `order_date`, `first_order_id`, `first_order_amount`,
# MAGIC `last_order_id`, `last_order_amount`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use FIRST_VALUE(order_id) and FIRST_VALUE(amount) partitioned by `customer_id`, ordered by `order_date`
# MAGIC 2. Use LAST_VALUE(order_id) and LAST_VALUE(amount) with explicit frame ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
# MAGIC 3. Add `first_order_id`, `first_order_amount`, `last_order_id`, `last_order_amount` columns
# MAGIC 4. Write to Delta table with all 8 columns
# MAGIC
# MAGIC **Constraints**:
# MAGIC - CUST-001 first: ORD-601 (100.00), last: ORD-605 (50.00)
# MAGIC - CUST-002 first: ORD-606 (80.00), last: ORD-609 (140.00)
# MAGIC - CUST-004 first: ORD-614 (500.00), last: ORD-615 (220.00)
# MAGIC - Every row for the same customer has identical first/last values

# COMMAND ----------

# EXERCISE_KEY: window_ex6
# TODO: Find first and most recent order per customer using FIRST_VALUE and LAST_VALUE

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex6_first_last")

assert result.count() == 15, f"Expected 15 rows, got {result.count()}"
for col in ["first_order_id", "first_order_amount", "last_order_id", "last_order_amount"]:
    assert col in result.columns, f"Missing {col} column"

# CUST-001: first=ORD-601 (100.00), last=ORD-605 (50.00) - consistent across all 5 rows
c1_rows = result.filter("customer_id = 'CUST-001'").collect()
for row in c1_rows:
    assert row.first_order_id == "ORD-601", f"CUST-001 first_order_id should be ORD-601, got {row.first_order_id}"
    assert row.first_order_amount == 100.00, f"CUST-001 first_order_amount should be 100.00, got {row.first_order_amount}"
    assert row.last_order_id == "ORD-605", f"CUST-001 last_order_id should be ORD-605, got {row.last_order_id}"
    assert row.last_order_amount == 50.00, f"CUST-001 last_order_amount should be 50.00, got {row.last_order_amount}"

# CUST-002: first=ORD-606 (80.00), last=ORD-609 (140.00)
c2 = result.filter("customer_id = 'CUST-002' AND order_id = 'ORD-607'").collect()[0]
assert c2.first_order_id == "ORD-606", f"CUST-002 first_order_id should be ORD-606, got {c2.first_order_id}"
assert c2.last_order_amount == 140.00, f"CUST-002 last_order_amount should be 140.00, got {c2.last_order_amount}"

# CUST-004: only 2 orders, first=ORD-614 (500.00), last=ORD-615 (220.00)
c4 = result.filter("customer_id = 'CUST-004' AND order_id = 'ORD-614'").collect()[0]
assert c4.first_order_id == "ORD-614", f"CUST-004 first_order_id should be ORD-614, got {c4.first_order_id}"
assert c4.last_order_id == "ORD-615", f"CUST-004 last_order_id should be ORD-615, got {c4.last_order_id}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: QUALIFY for Top-N Per Group Without Subquery
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Get the top 2 highest-value orders per customer using the QUALIFY clause (Databricks SQL
# MAGIC extension). No subqueries or CTEs needed.
# MAGIC
# MAGIC **Source** (`window_ex7_orders`): 12 rows across 3 customers (4 orders each).
# MAGIC - CUST-001 amounts: 100, 250, 175, 300
# MAGIC - CUST-002 amounts: 80, 0, 190, 140
# MAGIC - CUST-003 amounts: 400, 125, 275, 350
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex7_top2`. 6 rows (2 per customer)
# MAGIC with all original columns: `order_id`, `customer_id`, `amount`, `order_date`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use ROW_NUMBER() partitioned by `customer_id`, ordered by `amount` DESC
# MAGIC 2. Use QUALIFY to keep only the top 2 per customer (row_number <= 2)
# MAGIC 3. Do NOT include the row_number column in the output
# MAGIC 4. Write to Delta table
# MAGIC
# MAGIC **Constraints**:
# MAGIC - CUST-001 top 2: ORD-704 (300.00), ORD-702 (250.00)
# MAGIC - CUST-002 top 2: ORD-707 (190.00), ORD-708 (140.00)
# MAGIC - CUST-003 top 2: ORD-709 (400.00), ORD-712 (350.00)
# MAGIC - Output has exactly 6 rows, no row_number/rn column

# COMMAND ----------

# EXERCISE_KEY: window_ex7
# TODO: Get top 2 highest-value orders per customer using QUALIFY

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex7_top2")

assert result.count() == 6, f"Expected 6 rows, got {result.count()}"
assert "row_number" not in [c.lower() for c in result.columns] and "rn" not in [c.lower() for c in result.columns], \
    "Output should not contain the row_number column"

# CUST-001 top 2: ORD-704 (300), ORD-702 (250)
c1_orders = set(r.order_id for r in result.filter("customer_id = 'CUST-001'").collect())
assert c1_orders == {"ORD-704", "ORD-702"}, f"CUST-001 top 2 should be ORD-704 and ORD-702, got {c1_orders}"

# CUST-002 top 2: ORD-707 (190), ORD-708 (140) - $0 order excluded
c2_orders = set(r.order_id for r in result.filter("customer_id = 'CUST-002'").collect())
assert c2_orders == {"ORD-707", "ORD-708"}, f"CUST-002 top 2 should be ORD-707 and ORD-708, got {c2_orders}"

# CUST-003 top 2: ORD-709 (400), ORD-712 (350)
c3_orders = set(r.order_id for r in result.filter("customer_id = 'CUST-003'").collect())
assert c3_orders == {"ORD-709", "ORD-712"}, f"CUST-003 top 2 should be ORD-709 and ORD-712, got {c3_orders}"

# Each customer has exactly 2 rows
for cust in ["CUST-001", "CUST-002", "CUST-003"]:
    cnt = result.filter(f"customer_id = '{cust}'").count()
    assert cnt == 2, f"{cust} should have 2 rows, got {cnt}"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Sessionization - Assign Session IDs Based on 30-Min Gaps
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Assign session IDs to clickstream events based on a 30-minute inactivity gap.
# MAGIC If more than 30 minutes pass between consecutive events for the same user,
# MAGIC the later event starts a new session.
# MAGIC
# MAGIC **Source** (`window_ex8_events`): 15 events across 2 users.
# MAGIC - USER-001: 9 events forming 3 sessions (gaps at 45 min and 60 min)
# MAGIC - USER-002: 6 events forming 2 sessions (gap at 35 min)
# MAGIC
# MAGIC **Target**: Write to `db_code.window_functions.window_ex8_sessions`. 15 rows with columns:
# MAGIC `event_id`, `user_id`, `event_type`, `event_ts`, `payload`, `session_id`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use LAG(event_ts) partitioned by `user_id`, ordered by `event_ts` to find the previous event time
# MAGIC 2. Flag a new session when the gap from previous event exceeds 30 minutes (or it is the first event)
# MAGIC 3. Use a cumulative SUM of the new-session flag to assign incrementing `session_id` per user
# MAGIC 4. Session IDs start at 1 per user and increment for each new session
# MAGIC 5. Write to Delta table with all original columns plus `session_id`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - USER-001 should have 3 sessions (session_id 1, 2, 3)
# MAGIC - USER-002 should have 2 sessions (session_id 1, 2)
# MAGIC - Total distinct (user_id, session_id) pairs = 5

# COMMAND ----------

# EXERCISE_KEY: window_ex8
# TODO: Assign session IDs based on 30-minute inactivity gap

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.window_ex8_sessions")

assert result.count() == 15, f"Expected 15 rows, got {result.count()}"
assert "session_id" in result.columns, "Missing session_id column"

# USER-001 should have 3 sessions
u1_sessions = result.filter("user_id = 'USER-001'").select("session_id").distinct().count()
assert u1_sessions == 3, f"USER-001 should have 3 sessions, got {u1_sessions}"

# USER-002 should have 2 sessions
u2_sessions = result.filter("user_id = 'USER-002'").select("session_id").distinct().count()
assert u2_sessions == 2, f"USER-002 should have 2 sessions, got {u2_sessions}"

# EVT-001 (first event, USER-001) should be session 1
evt1_session = result.filter("event_id = 'EVT-001'").select("session_id").collect()[0][0]
assert evt1_session == 1, f"EVT-001 should be session 1, got {evt1_session}"

# EVT-005 (after 45-min gap) should be session 2
evt5_session = result.filter("event_id = 'EVT-005'").select("session_id").collect()[0][0]
assert evt5_session == 2, f"EVT-005 should be session 2 (45-min gap), got {evt5_session}"

# EVT-008 (after 60-min gap) should be session 3
evt8_session = result.filter("event_id = 'EVT-008'").select("session_id").collect()[0][0]
assert evt8_session == 3, f"EVT-008 should be session 3 (60-min gap), got {evt8_session}"

# EVT-014 (USER-002, after 35-min gap) should be session 2
evt14_session = result.filter("event_id = 'EVT-014'").select("session_id").collect()[0][0]
assert evt14_session == 2, f"EVT-014 should be session 2 for USER-002, got {evt14_session}"

# Total distinct (user_id, session_id) pairs = 5
distinct_sessions = result.select("user_id", "session_id").distinct().count()
assert distinct_sessions == 5, f"Expected 5 distinct (user_id, session_id) pairs, got {distinct_sessions}"

print("Exercise 8 passed!")
