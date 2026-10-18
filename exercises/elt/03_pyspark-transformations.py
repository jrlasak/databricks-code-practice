# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark DataFrame Transformations
# MAGIC Practice core PySpark DataFrame API operations: select, filter, withColumn, when/otherwise,
# MAGIC pivot, unpivot, UDFs, native function rewrites, and chained transform pipelines. Every
# MAGIC exercise reads from Delta tables in Unity Catalog and writes results back to Delta.
# MAGIC
# MAGIC **Topic**: ELT | **Exercises**: 8 | **Total Time**: ~85 min
# MAGIC
# MAGIC Exercises 1-7 each write their result as a Delta table in overwrite mode, to the table on their **Target** line. Exercise 8 overwrites only part of its target with `replaceWhere`.
# MAGIC
# MAGIC **Shared schema** (`db_code.elt.orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | e.g. ORD-001. ORD-042 and ORD-067 appear twice, with different `updated_at` |
# MAGIC | customer_id | STRING | Nullable. Some rows have NULL customer_id |
# MAGIC | product_id | STRING | References products table |
# MAGIC | amount | DOUBLE | Order total in USD. Includes $0 amounts |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | |
# MAGIC | updated_at | TIMESTAMP | Last modification time |
# MAGIC
# MAGIC **Shared schema** (`db_code.elt.customers`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | STRING | Primary key |
# MAGIC | name | STRING | Customer name (some duplicates) |
# MAGIC | email | STRING | Email (some nulls) |
# MAGIC | region | STRING | US-East, US-West, EU-West, EU-East, APAC |
# MAGIC | tier | STRING | bronze, silver, gold, platinum, inactive |
# MAGIC | signup_date | DATE | Registration date |
# MAGIC
# MAGIC **Shared schema** (`db_code.elt.events`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | e.g. EVT-001. Has duplicates |
# MAGIC | user_id | STRING | |
# MAGIC | event_type | STRING | page_view, click, purchase, signup, logout, error, search |
# MAGIC | event_ts | TIMESTAMP | |
# MAGIC | payload | STRING | JSON string. Some rows have NULL payload |
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/pyspark-transformations-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Prerequisites**: none. The next two cells run `00_Setup` and this topic's setup for you.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/pyspark-transformations-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Setup created per-exercise source tables in `db_code.pyspark_transformations`. Each exercise
# MAGIC reads from its own source table(s) and writes to its own output table. Exercises are
# MAGIC independent - you can skip to any exercise.
# MAGIC
# MAGIC The setup also defines the Python variables `CATALOG` (`db_code`), `SCHEMA` (`pyspark_transformations`) and `BASE_SCHEMA` (`elt`) for your code. TODO cells are Python; run SQL with `spark.sql(...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Basic select/filter/withColumn
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Filter orders to only completed orders, select a subset of columns, and add a tax column.
# MAGIC
# MAGIC **Source** (`orders_ex1`): All orders from the base table with all statuses (completed,
# MAGIC pending, shipped, cancelled). Includes rows with $0 amounts and NULL customer_id.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.completed_orders_ex1`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex1`
# MAGIC 2. Filter to only rows where `status` = "completed"
# MAGIC 3. Select columns: `order_id`, `customer_id`, `amount`, `order_date`
# MAGIC 4. Add a new column `amount_with_tax` = `amount` * 1.08 (8% tax), rounded to 2 decimal places
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Keep rows even if `customer_id` is NULL or `amount` is 0
# MAGIC - The tax calculation must handle $0 amounts (result should be 0.0)
# MAGIC
# MAGIC **Expected output**: 43 rows. `ORD-001` has `amount_with_tax` = 118.26; `ORD-022` and `ORD-058` have 0.0.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex1
# TODO: Filter to completed orders, add amount_with_tax column, write to Delta
# Your code here

# COMMAND ----------

result_ex1 = spark.table(f"{CATALOG}.{SCHEMA}.completed_orders_ex1")

# Row count: only completed orders
completed_count = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex1").filter("status = 'completed'").count()
assert result_ex1.count() == completed_count, f"Expected {completed_count} rows, got {result_ex1.count()}"

# Schema check: must have amount_with_tax column
assert "amount_with_tax" in result_ex1.columns, "Missing column: amount_with_tax"
assert set(result_ex1.columns) == {"order_id", "customer_id", "amount", "order_date", "amount_with_tax"}, f"Unexpected columns: {result_ex1.columns}"

# Value check: tax calculation
row = result_ex1.filter("amount > 0").first()
expected_tax = round(row.amount * 1.08, 2)
assert row.amount_with_tax == expected_tax, f"Tax calculation wrong: expected {expected_tax}, got {row.amount_with_tax}"

# Edge case: $0 amounts should have $0 tax
zero_amounts = result_ex1.filter("amount = 0")
if zero_amounts.count() > 0:
    assert zero_amounts.first().amount_with_tax == 0.0, "Tax on $0 amount should be 0.0"

print("Exercise 1 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Column renaming and type casting
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Rename camelCase columns to snake_case and cast string dates to DateType.
# MAGIC
# MAGIC **Source** (`customers_ex2`): Customer table with camelCase column names and signup date
# MAGIC stored as a STRING. Columns: `customerId`, `customerName`, `emailAddress`, `customerRegion`,
# MAGIC `customerTier`, `signupDate` (STRING format "yyyy-MM-dd").
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.customers_clean_ex2`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.customers_ex2`
# MAGIC 2. Rename columns to snake_case:
# MAGIC    - `customerId` -> `customer_id`
# MAGIC    - `customerName` -> `customer_name`
# MAGIC    - `emailAddress` -> `email_address`
# MAGIC    - `customerRegion` -> `region`
# MAGIC    - `customerTier` -> `tier`
# MAGIC    - `signupDate` -> `signup_date`
# MAGIC 3. Cast `signup_date` from STRING to DateType
# MAGIC
# MAGIC **Constraints**:
# MAGIC - All 6 columns must be present with the exact snake_case names
# MAGIC - `signup_date` must be DateType in the output, not StringType
# MAGIC - Preserve all rows (some emails are NULL - keep them)
# MAGIC
# MAGIC **Expected output**: 50 rows, 3 with a NULL `email_address`. `CUST-001` is `Alice Smith` with `signup_date` 2024-01-15 (DATE).

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex2
# TODO: Rename columns to snake_case, cast signupDate to DateType, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql.types import DateType

result_ex2 = spark.table(f"{CATALOG}.{SCHEMA}.customers_clean_ex2")
source_ex2 = spark.table(f"{CATALOG}.{SCHEMA}.customers_ex2")

# Row count: all rows preserved
assert result_ex2.count() == source_ex2.count(), f"Expected {source_ex2.count()} rows, got {result_ex2.count()}"

# Schema check: exact column names in snake_case
expected_cols = {"customer_id", "customer_name", "email_address", "region", "tier", "signup_date"}
assert set(result_ex2.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex2.columns)}"

# No camelCase columns should remain
for col_name in result_ex2.columns:
    assert col_name == col_name.lower().replace(" ", "_"), f"Column {col_name} is not snake_case"

# Type check: signup_date must be DateType
signup_field = result_ex2.schema["signup_date"]
assert isinstance(signup_field.dataType, DateType), f"signup_date should be DateType, got {signup_field.dataType}"

# Value check: first customer should have correct name
first_source = source_ex2.first()
first_result = result_ex2.filter(f"customer_id = '{first_source.customerId}'").first()
assert first_result.customer_name == first_source.customerName, f"Name mismatch: {first_result.customer_name} vs {first_source.customerName}"

# NULL emails preserved
null_emails_source = source_ex2.filter("emailAddress IS NULL").count()
null_emails_result = result_ex2.filter("email_address IS NULL").count()
assert null_emails_result == null_emails_source, f"NULL email count changed: {null_emails_source} -> {null_emails_result}"

print("Exercise 2 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Complex when/otherwise chains - multi-condition order tier
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Categorize orders into tiers based on multiple conditions using chained `when`/`otherwise`
# MAGIC expressions. The tier depends on BOTH status and amount.
# MAGIC
# MAGIC **Source** (`orders_ex3`): 108 orders with columns `order_id`, `customer_id`, `amount`, `status`, `order_date`. Amounts include $0, and two orders have a NULL `amount` (ORD-901, ORD-902). Statuses: completed, pending, shipped, cancelled.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.tiered_orders_ex3`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex3`
# MAGIC 2. Add column `order_tier` based on these rules (evaluated in order):
# MAGIC    - If `status` = "cancelled": tier = "inactive"
# MAGIC    - If `amount` >= 200: tier = "premium"
# MAGIC    - If `amount` >= 50 and `amount` < 200: tier = "standard"
# MAGIC    - If `amount` > 0 and `amount` < 50: tier = "budget"
# MAGIC    - If `amount` = 0 or `amount` IS NULL: tier = "free"
# MAGIC 3. Add column `priority_score` as an integer:
# MAGIC    - "premium" = 4, "standard" = 3, "budget" = 2, "free" = 1, "inactive" = 0
# MAGIC 4. Keep all five source columns and add the two new ones
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Every row must have a non-null `order_tier` and `priority_score`
# MAGIC
# MAGIC **Expected output**: 108 rows. `ORD-004` (cancelled, 320.00) is `inactive`/0, `ORD-011` (450.00) is `premium`/4, `ORD-005` (89.99) is `standard`/3, `ORD-003` (45.00) is `budget`/2, and `ORD-022` (0.00) and `ORD-901` (NULL) are `free`/1.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex3
# TODO: Add order_tier and priority_score columns using when/otherwise, write to Delta
# Your code here

# COMMAND ----------

result_ex3 = spark.table(f"{CATALOG}.{SCHEMA}.tiered_orders_ex3")
source_ex3 = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex3")

# Row count: all rows preserved
assert result_ex3.count() == source_ex3.count(), f"Expected {source_ex3.count()} rows, got {result_ex3.count()}"

# Schema check
assert "order_tier" in result_ex3.columns, "Missing column: order_tier"
assert "priority_score" in result_ex3.columns, "Missing column: priority_score"

# No nulls in order_tier or priority_score
null_tiers = result_ex3.filter("order_tier IS NULL").count()
assert null_tiers == 0, f"Found {null_tiers} rows with NULL order_tier"
null_scores = result_ex3.filter("priority_score IS NULL").count()
assert null_scores == 0, f"Found {null_scores} rows with NULL priority_score"

# Cancelled orders must be inactive
inactive_check = result_ex3.filter("status = 'cancelled' AND order_tier != 'inactive'").count()
assert inactive_check == 0, f"{inactive_check} cancelled orders not marked as inactive"

# Premium check: non-inactive orders >= $200
premium_check = result_ex3.filter("status != 'cancelled' AND amount >= 200")
if premium_check.count() > 0:
    bad_premium = premium_check.filter("order_tier != 'premium'").count()
    assert bad_premium == 0, f"{bad_premium} orders >= $200 not marked as premium"

# Standard check: non-inactive orders from $50 up to $200
standard_check = result_ex3.filter("status != 'cancelled' AND amount >= 50 AND amount < 200 AND order_tier != 'standard'").count()
assert standard_check == 0, f"{standard_check} orders between $50 and $200 not marked as standard"

# Priority score mapping
score_premium = result_ex3.filter("order_tier = 'premium' AND priority_score != 4").count()
assert score_premium == 0, "Premium orders should have priority_score = 4"
score_inactive = result_ex3.filter("order_tier = 'inactive' AND priority_score != 0").count()
assert score_inactive == 0, "Inactive orders should have priority_score = 0"
score_budget = result_ex3.filter("order_tier = 'budget' AND priority_score != 2").count()
assert score_budget == 0, "Budget orders should have priority_score = 2"
score_standard = result_ex3.filter("order_tier = 'standard' AND priority_score != 3").count()
assert score_standard == 0, "Standard orders should have priority_score = 3"
score_free = result_ex3.filter("order_tier = 'free' AND priority_score != 1").count()
assert score_free == 0, "Free orders should have priority_score = 1"

# Free check: $0 or NULL amounts that are not cancelled
free_val = result_ex3.filter("status != 'cancelled' AND (amount = 0 OR amount IS NULL)")
if free_val.count() > 0:
    bad_free = free_val.filter("order_tier != 'free'").count()
    assert bad_free == 0, f"{bad_free} zero/null-amount orders not marked as free"

print("Exercise 3 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Pivot - order counts by status per customer
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Pivot order data to create one row per customer with order counts per status as columns.
# MAGIC
# MAGIC **Source** (`orders_ex4`): columns `order_id`, `customer_id`, `amount`, `status`; rows with a NULL `customer_id` are already removed. Statuses: completed, pending, shipped, cancelled.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.customer_status_pivot_ex4`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex4`
# MAGIC 2. Pivot on `status` column to create one column per status value
# MAGIC 3. For each `customer_id`, calculate the COUNT of orders per status
# MAGIC 4. Specify pivot values explicitly: `["completed", "pending", "shipped", "cancelled"]`
# MAGIC 5. Replace any NULL pivot values with 0 (customers with no orders in a status get 0)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - One row per `customer_id` in the output
# MAGIC - Output columns: `customer_id`, `completed`, `pending`, `shipped`, `cancelled`
# MAGIC - All count columns must be integers, no nulls
# MAGIC
# MAGIC **Expected output**: 50 rows. `CUST-050` has `completed` 1, `pending` 1, `shipped` 1, `cancelled` 0.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex4
# TODO: Pivot orders by status to get per-customer order counts, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex4 = spark.table(f"{CATALOG}.{SCHEMA}.customer_status_pivot_ex4")
source_ex4 = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex4")

# Row count: one row per unique customer_id
expected_customers = source_ex4.select("customer_id").distinct().count()
assert result_ex4.count() == expected_customers, f"Expected {expected_customers} customers, got {result_ex4.count()}"

# Schema check: must have customer_id + status columns
assert "customer_id" in result_ex4.columns, "Missing column: customer_id"
status_cols = ["completed", "pending", "shipped", "cancelled"]
for sc in status_cols:
    assert sc in result_ex4.columns, f"Missing pivot column: {sc}"

# Check that total across status columns equals total orders for a sample customer
sample_cust = source_ex4.groupBy("customer_id").count().orderBy(F.desc("count"), "customer_id").first().customer_id
sample_row = result_ex4.filter(f"customer_id = '{sample_cust}'").first()
sample_total_from_source = source_ex4.filter(f"customer_id = '{sample_cust}'").count()

sample_total_from_pivot = sum(getattr(sample_row, c) or 0 for c in status_cols)
assert sample_total_from_pivot == sample_total_from_source, (
    f"Customer {sample_cust}: pivot total {sample_total_from_pivot} != source count {sample_total_from_source}"
)

# No nulls in pivot columns (should be 0)
for col_name in status_cols:
    null_count = result_ex4.filter(f"`{col_name}` IS NULL").count()
    assert null_count == 0, f"Column {col_name} has {null_count} nulls (should be 0)"

print("Exercise 4 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Unpivot with stack() - wide metrics to long format
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Convert a wide metrics table to long format with `stack()`, so each metric becomes its own row.
# MAGIC
# MAGIC **Source** (`customer_metrics_ex5`): One row per customer with columns: `customer_id`,
# MAGIC `name`, `completed_orders` (BIGINT), `pending_orders` (BIGINT), `cancelled_orders` (BIGINT),
# MAGIC `total_revenue` (DOUBLE).
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.customer_metrics_long_ex5`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.customer_metrics_ex5`
# MAGIC 2. Unpivot the three order count columns (`completed_orders`, `pending_orders`,
# MAGIC    `cancelled_orders`) into two columns: `metric_name` (STRING) and `metric_value` (BIGINT)
# MAGIC 3. Keep `customer_id` and `name` as identifier columns (do NOT unpivot them)
# MAGIC 4. Do NOT include `total_revenue` in the unpivot (it has a different type)
# MAGIC 5. Filter out rows where `metric_value` = 0 (only keep non-zero metrics)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Output columns: `customer_id`, `name`, `metric_name`, `metric_value`
# MAGIC - `metric_name` values should be the original column names: "completed_orders",
# MAGIC   "pending_orders", "cancelled_orders"
# MAGIC
# MAGIC **Approach**: `stack()` is a SQL generator function: call it inside `selectExpr` and name its two output columns. Check the solutions notebook hints if you need the argument order.
# MAGIC
# MAGIC **Expected output**: 67 rows: 36 `completed_orders`, 22 `pending_orders` and 9 `cancelled_orders`. `CUST-001` has two rows: `completed_orders` 2 and `pending_orders` 1.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex5
# TODO: Unpivot wide metrics to long format using stack(), filter zeros, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex5 = spark.table(f"{CATALOG}.{SCHEMA}.customer_metrics_long_ex5")
source_ex5 = spark.table(f"{CATALOG}.{SCHEMA}.customer_metrics_ex5")

# Schema check: exact output columns
expected_cols = {"customer_id", "name", "metric_name", "metric_value"}
assert set(result_ex5.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex5.columns)}"

# No zero metric_values
zero_count = result_ex5.filter("metric_value = 0").count()
assert zero_count == 0, f"Found {zero_count} rows with metric_value = 0 (should be filtered out)"

# No nulls in metric_value
null_mv = result_ex5.filter("metric_value IS NULL").count()
assert null_mv == 0, f"Found {null_mv} rows with NULL metric_value"

# Valid metric_name values only
valid_metrics = {"completed_orders", "pending_orders", "cancelled_orders"}
actual_metrics = set(row.metric_name for row in result_ex5.select("metric_name").distinct().collect())
assert actual_metrics.issubset(valid_metrics), f"Invalid metric_name values: {actual_metrics - valid_metrics}"

# Cross-check: sum of completed_orders in long format should match source
source_completed_sum = source_ex5.agg(F.sum("completed_orders")).collect()[0][0]
result_completed_sum = result_ex5.filter("metric_name = 'completed_orders'").agg(F.sum("metric_value")).collect()[0][0]
assert result_completed_sum == source_completed_sum, (
    f"completed_orders sum mismatch: source={source_completed_sum}, result={result_completed_sum}"
)

# total_revenue should NOT appear as a metric
assert "total_revenue" not in actual_metrics, "total_revenue should not be unpivoted"

print("Exercise 5 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Python UDF for custom logic with null handling
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Write a Python UDF that classifies each event payload, apply it to `events_ex6`, and write the result to Delta. The UDF must return "no_data" for NULL payloads instead of failing.
# MAGIC
# MAGIC **Source** (`events_ex6`): 156 events with JSON payload strings. Examples: `{"page": "/home"}`, `{"session_duration": 1200}`, `{"item_id": "P-100", "price": 29.99}`, `{"price": 15.00, "duration": 30}`. Five payloads are NULL and one is an empty string.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.classified_events_ex6`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.events_ex6`
# MAGIC 2. Create a Python UDF that takes a `payload` string and returns a classification:
# MAGIC    - If payload is NULL or empty string: return "no_data"
# MAGIC    - If payload contains "price": return "transaction"
# MAGIC    - If payload contains "duration": return "engagement"
# MAGIC    - Otherwise: return "other"
# MAGIC 3. Apply the UDF to create a new column `payload_class`
# MAGIC 4. Select columns: `event_id`, `user_id`, `event_type`, `event_ts`, `payload_class`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The UDF must handle NULL payloads without errors
# MAGIC - Every row must have a non-null `payload_class`
# MAGIC - Use `@udf` decorator or `udf()` function with explicit return type (StringType)
# MAGIC
# MAGIC **Expected output**: 156 rows: 6 `no_data`, 2 `transaction` (EVT-901, EVT-903), 21 `engagement` and 127 `other`.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex6
# TODO: Create a UDF to classify payloads, apply it, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex6 = spark.table(f"{CATALOG}.{SCHEMA}.classified_events_ex6")
source_ex6 = spark.table(f"{CATALOG}.{SCHEMA}.events_ex6")

# Row count: all events preserved
assert result_ex6.count() == source_ex6.count(), f"Expected {source_ex6.count()} rows, got {result_ex6.count()}"

# Schema check
assert "payload_class" in result_ex6.columns, "Missing column: payload_class"
assert set(result_ex6.columns) == {"event_id", "user_id", "event_type", "event_ts", "payload_class"}, f"Unexpected columns: {result_ex6.columns}"

# No nulls in payload_class
null_class = result_ex6.filter("payload_class IS NULL").count()
assert null_class == 0, f"Found {null_class} rows with NULL payload_class"

# Valid values only
valid_classes = {"no_data", "transaction", "engagement", "other"}
actual_classes = set(row.payload_class for row in result_ex6.select("payload_class").distinct().collect())
assert actual_classes.issubset(valid_classes), f"Invalid payload_class values: {actual_classes - valid_classes}"

# NULL and empty payloads -> "no_data"
null_payloads = source_ex6.filter("payload IS NULL").count()
empty_payloads = source_ex6.filter("payload = ''").count()
no_data_count = result_ex6.filter("payload_class = 'no_data'").count()
assert no_data_count == null_payloads + empty_payloads, f"no_data count {no_data_count} != null ({null_payloads}) + empty ({empty_payloads})"

# "duration" without "price" -> "engagement"
duration_source = source_ex6.filter("payload LIKE '%duration%' AND payload NOT LIKE '%price%'").count()
engagement_result = result_ex6.filter("payload_class = 'engagement'").count()
assert engagement_result == duration_source, f"Expected {duration_source} engagement rows, got {engagement_result}"

# Cross-check: rows with "price" in payload should be "transaction"
price_in_source = source_ex6.filter("payload LIKE '%price%'").count()
transaction_in_result = result_ex6.filter("payload_class = 'transaction'").count()
assert transaction_in_result == price_in_source, f"Expected {price_in_source} transaction rows, got {transaction_in_result}"

print("Exercise 6 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 7: Replace UDF with native Spark functions
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC The UDF from Exercise 6 works, but every row crosses from the JVM to a Python worker and back. Rewrite the
# MAGIC same classification logic using ONLY native Spark functions, which run inside the engine with no Python round trip.
# MAGIC
# MAGIC **Source** (`events_ex7`): The same 156 events as Exercise 6, including the NULL and empty payloads.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.classified_events_native_ex7`.
# MAGIC
# MAGIC **The rules to replace** (the Exercise 6 UDF, checked in this order): a NULL or empty payload is "no_data"; a payload containing "price" is "transaction"; a payload containing "duration" is "engagement"; anything else is "other".
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.events_ex7`
# MAGIC 2. Replicate the EXACT same classification logic using only native Spark functions
# MAGIC    (no Python UDFs, no `@udf`, no `F.udf`)
# MAGIC 3. Create column `payload_class` with the same values: "no_data", "transaction",
# MAGIC    "engagement", "other"
# MAGIC 4. Select columns: `event_id`, `user_id`, `event_type`, `event_ts`, `payload_class`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Zero Python UDFs. Only `F.when()`, `F.col()`, `.contains()`, `.isNull()`, etc.
# MAGIC - Every row must get the same `payload_class` the UDF returns, including "no_data" for NULL and empty payloads
# MAGIC
# MAGIC **Expected output**: 156 rows: 6 `no_data`, 2 `transaction`, 21 `engagement` and 127 `other`, the same as Exercise 6.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex7
# TODO: Rewrite payload classification using only native Spark functions, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex7 = spark.table(f"{CATALOG}.{SCHEMA}.classified_events_native_ex7")
source_ex7 = spark.table(f"{CATALOG}.{SCHEMA}.events_ex7")

# Row count: all events preserved
assert result_ex7.count() == source_ex7.count(), f"Expected {source_ex7.count()} rows, got {result_ex7.count()}"

# Schema check
assert set(result_ex7.columns) == {"event_id", "user_id", "event_type", "event_ts", "payload_class"}, f"Unexpected columns: {result_ex7.columns}"

# No nulls in payload_class
null_class = result_ex7.filter("payload_class IS NULL").count()
assert null_class == 0, f"Found {null_class} rows with NULL payload_class"

# Valid values only
valid_classes = {"no_data", "transaction", "engagement", "other"}
actual_classes = set(row.payload_class for row in result_ex7.select("payload_class").distinct().collect())
assert actual_classes.issubset(valid_classes), f"Invalid payload_class values: {actual_classes - valid_classes}"

# Cross-check against UDF logic: NULL and empty payloads -> "no_data"
null_payloads = source_ex7.filter("payload IS NULL").count()
no_data = result_ex7.filter("payload_class = 'no_data'").count()
empty_payloads = source_ex7.filter("payload = ''").count()
assert no_data == null_payloads + empty_payloads, f"no_data count {no_data} != null ({null_payloads}) + empty ({empty_payloads})"

# Cross-check: "price" -> "transaction"
price_source = source_ex7.filter("payload LIKE '%price%'").count()
transaction_result = result_ex7.filter("payload_class = 'transaction'").count()
assert transaction_result == price_source, f"Expected {price_source} transaction rows, got {transaction_result}"

# Cross-check: "duration" (without "price") -> "engagement"
duration_source = source_ex7.filter("payload LIKE '%duration%' AND payload NOT LIKE '%price%'").count()
engagement_result = result_ex7.filter("payload_class = 'engagement'").count()
assert engagement_result == duration_source, f"Expected {duration_source} engagement rows, got {engagement_result}"

print("Exercise 7 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 8: Chained transform pipeline with replaceWhere
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Build a multi-step pipeline: aggregate order items into a struct column, join them onto orders, bucket the amounts, and write with `replaceWhere` so only the completed rows are overwritten.
# MAGIC
# MAGIC - **Source** (`orders_ex8`): Orders pre-joined with customer info (`customer_name`, `region`). 106 rows.
# MAGIC - **Source** (`order_items_ex8`): Line items per order (`order_id`, `product_id`, `quantity`, `unit_price`). 205 rows. Some `order_id` values have no matching order.
# MAGIC - **Target** (`enriched_orders_ex8`): Already holds the cancelled orders. After your write it must hold both those cancelled rows and your completed rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `orders_ex8` and `order_items_ex8` from `db_code.pyspark_transformations`
# MAGIC 2. Aggregate `order_items_ex8` per `order_id`: compute `total_items` (sum of quantity)
# MAGIC    and `total_value` (sum of quantity * unit_price)
# MAGIC 3. Join the aggregated items onto `orders_ex8` (left join on order_id)
# MAGIC 4. Create a struct column `items_summary` with fields: `total_items` (BIGINT),
# MAGIC    `total_value` (DOUBLE)
# MAGIC 5. Add column `amount_bucket`: "high" if `amount` >= 100, "medium" if `amount` >= 25,
# MAGIC    "low" otherwise
# MAGIC 6. Select final columns: `order_id`, `customer_id`, `customer_name`, `region`, `amount`,
# MAGIC    `status`, `amount_bucket`, `items_summary`, `order_date`, `updated_at`
# MAGIC 7. Filter to only `status = 'completed'`
# MAGIC 8. Write to `db_code.pyspark_transformations.enriched_orders_ex8` using `replaceWhere`
# MAGIC    with condition `"status = 'completed'"`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Orders with no matching items: `items_summary.total_items` = 0,
# MAGIC   `items_summary.total_value` = 0.0
# MAGIC - Use DataFrame API (not SQL) for transformations
# MAGIC
# MAGIC **Expected output**: 53 rows after your write: your 43 completed rows plus the 10 cancelled rows from setup. `ORD-001` is `high` with `items_summary` = {6, 528.0}; `ORD-096` has no line items, so its `items_summary` is {0, 0.0}.

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex8
# TODO: Multi-step pipeline - aggregate items, join, create struct, write with replaceWhere
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex8 = spark.table(f"{CATALOG}.{SCHEMA}.enriched_orders_ex8")

# The table must hold completed rows (your write) and cancelled rows (setup)
statuses_present = set(row.status for row in result_ex8.select("status").distinct().collect())
assert "completed" in statuses_present, "No completed orders found - did you write with replaceWhere?"
assert "cancelled" in statuses_present, "Cancelled orders from setup are missing - replaceWhere should preserve them"

# Count completed rows matches source
source_completed = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex8").filter("status = 'completed'").count()
result_completed = result_ex8.filter("status = 'completed'").count()
assert result_completed == source_completed, f"Expected {source_completed} completed rows, got {result_completed}"

# Schema check: items_summary must be a struct
from pyspark.sql.types import StructType
items_field = result_ex8.schema["items_summary"]
assert isinstance(items_field.dataType, StructType), f"items_summary should be a struct, got {items_field.dataType}"
struct_fields = {f.name for f in items_field.dataType.fields}
assert "total_items" in struct_fields, "items_summary missing field: total_items"
assert "total_value" in struct_fields, "items_summary missing field: total_value"

# amount_bucket check
assert "amount_bucket" in result_ex8.columns, "Missing column: amount_bucket"
high_check = result_ex8.filter("status = 'completed' AND amount >= 100 AND amount_bucket != 'high'").count()
assert high_check == 0, f"{high_check} rows with amount >= 100 not bucketed as 'high'"
medium_check = result_ex8.filter("status = 'completed' AND amount >= 25 AND amount < 100 AND amount_bucket != 'medium'").count()
assert medium_check == 0, f"{medium_check} rows with amount 25-99 not bucketed as 'medium'"
low_check = result_ex8.filter("status = 'completed' AND amount < 25 AND amount_bucket != 'low'").count()
assert low_check == 0, f"{low_check} rows with amount under 25 not bucketed as 'low'"

# Items summary: orders with matching items should have non-zero totals
items_source = spark.table(f"{CATALOG}.{SCHEMA}.order_items_ex8")
orders_with_items = set(row.order_id for row in items_source.select("order_id").distinct().collect())
sample_with_items = result_ex8.filter("status = 'completed'").collect()
found_nonzero = False
for row in sample_with_items:
    if row.order_id in orders_with_items:
        if row.items_summary.total_items > 0:
            found_nonzero = True
            break
if len(orders_with_items) > 0:
    assert found_nonzero, "Expected some completed orders to have non-zero items_summary"

# No nulls in items_summary for completed rows
null_struct = result_ex8.filter("status = 'completed' AND items_summary IS NULL").count()
assert null_struct == 0, f"Found {null_struct} completed rows with NULL items_summary"

# Orders with no line items get a zero summary, not NULL fields
no_items = result_ex8.filter("order_id = 'ORD-096'").first()
assert no_items is not None, "ORD-096 (completed) is missing from the output"
assert no_items.items_summary.total_items == 0 and no_items.items_summary.total_value == 0.0, \
    f"ORD-096 has no line items: expected items_summary (0, 0.0), got {no_items.items_summary}"

print("Exercise 8 passed!")
