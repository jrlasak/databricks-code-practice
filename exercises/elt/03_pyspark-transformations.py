# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark DataFrame Transformations
# MAGIC Practice core PySpark DataFrame API operations: select, filter, withColumn, when/otherwise,
# MAGIC pivot, unpivot, UDFs, native function rewrites, and chained transform pipelines. Every
# MAGIC exercise reads from Delta tables in Unity Catalog and writes results back to Delta.
# MAGIC
# MAGIC **8 exercises** | Easy to Hard | ~95 min total
# MAGIC
# MAGIC **Shared schema** (`db_code.elt.orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | e.g. ORD-001. Has duplicates (same order_id, different updated_at) |
# MAGIC | customer_id | STRING | Nullable. Some rows have NULL customer_id |
# MAGIC | product_id | STRING | References products table |
# MAGIC | amount | DOUBLE | Order total in USD. Includes $0 amounts |
# MAGIC | status | STRING | completed, pending, cancelled, returned |
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
# MAGIC | tier | STRING | bronze, silver, gold, platinum |
# MAGIC | signup_date | DATE | Registration date |
# MAGIC
# MAGIC **Shared schema** (`db_code.elt.events`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | e.g. EVT-001. Has duplicates |
# MAGIC | user_id | STRING | |
# MAGIC | event_type | STRING | page_view, click, purchase, signup |
# MAGIC | event_ts | TIMESTAMP | |
# MAGIC | payload | STRING | JSON string. Some rows have NULL payload |
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/pyspark-transformations-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Prerequisites**: Run `00_Setup.py` once, then run the setup cell below.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/pyspark-transformations-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Setup created per-exercise source tables in `db_code.pyspark_transformations`. Each exercise
# MAGIC reads from its own source table(s) and writes to its own output table. Exercises are
# MAGIC independent - you can skip to any exercise.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Basic select/filter/withColumn
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Filter orders to only completed orders, select a subset of columns, and add a tax column.
# MAGIC
# MAGIC **Source** (`orders_ex1`): All orders from the base table with all statuses (completed,
# MAGIC pending, cancelled, returned). Includes rows with $0 amounts and NULL customer_id.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.completed_orders_ex1`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex1`
# MAGIC 2. Filter to only rows where `status` = "completed"
# MAGIC 3. Select columns: `order_id`, `customer_id`, `amount`, `order_date`
# MAGIC 4. Add a new column `amount_with_tax` = `amount` * 1.08 (8% tax), rounded to 2 decimal places
# MAGIC 5. Write as a Delta table to `db_code.pyspark_transformations.completed_orders_ex1` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Keep rows even if `customer_id` is NULL or `amount` is 0
# MAGIC - The tax calculation must handle $0 amounts (result should be 0.0)

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

# No non-completed orders should be present
if "status" in result_ex1.columns:
    assert result_ex1.filter("status != 'completed'").count() == 0, "Non-completed orders found in output"

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
# MAGIC 4. Write to `db_code.pyspark_transformations.customers_clean_ex2` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - All 6 columns must be present with the exact snake_case names
# MAGIC - `signup_date` must be DateType in the output, not StringType
# MAGIC - Preserve all rows (some emails are NULL - keep them)

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
# MAGIC **Source** (`orders_ex3`): Orders with varying amounts (including $0 and NULL) and statuses
# MAGIC (completed, pending, cancelled, returned).
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.tiered_orders_ex3`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex3`
# MAGIC 2. Add column `order_tier` based on these rules (evaluated in order):
# MAGIC    - If `status` = "cancelled" or "returned": tier = "inactive"
# MAGIC    - If `amount` >= 200: tier = "premium"
# MAGIC    - If `amount` >= 50 and `amount` < 200: tier = "standard"
# MAGIC    - If `amount` > 0 and `amount` < 50: tier = "budget"
# MAGIC    - If `amount` = 0 or `amount` IS NULL: tier = "free"
# MAGIC 3. Add column `priority_score` as an integer:
# MAGIC    - "premium" = 4, "standard" = 3, "budget" = 2, "free" = 1, "inactive" = 0
# MAGIC 4. Write all original columns plus the two new columns to
# MAGIC    `db_code.pyspark_transformations.tiered_orders_ex3` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Cancelled/returned orders are ALWAYS "inactive" regardless of amount
# MAGIC - NULL amounts should map to "free"
# MAGIC - Every row must have a non-null `order_tier` and `priority_score`

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

# Cancelled/returned orders must be inactive
inactive_check = result_ex3.filter("status IN ('cancelled', 'returned') AND order_tier != 'inactive'").count()
assert inactive_check == 0, f"{inactive_check} cancelled/returned orders not marked as inactive"

# Premium check: non-inactive orders >= $200
premium_check = result_ex3.filter("status NOT IN ('cancelled', 'returned') AND amount >= 200")
if premium_check.count() > 0:
    bad_premium = premium_check.filter("order_tier != 'premium'").count()
    assert bad_premium == 0, f"{bad_premium} orders >= $200 not marked as premium"

# Priority score mapping
score_premium = result_ex3.filter("order_tier = 'premium' AND priority_score != 4").count()
assert score_premium == 0, "Premium orders should have priority_score = 4"
score_inactive = result_ex3.filter("order_tier = 'inactive' AND priority_score != 0").count()
assert score_inactive == 0, "Inactive orders should have priority_score = 0"
score_budget = result_ex3.filter("order_tier = 'budget' AND priority_score != 2").count()
assert score_budget == 0, "Budget orders should have priority_score = 2"

# Free check: $0 or NULL amounts that are not cancelled/returned
free_val = result_ex3.filter("status NOT IN ('cancelled', 'returned') AND (amount = 0 OR amount IS NULL)")
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
# MAGIC **Source** (`orders_ex4`): Orders with non-null customer_ids and multiple statuses per
# MAGIC customer. Statuses: completed, pending, cancelled, returned.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.customer_status_pivot_ex4`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.orders_ex4`
# MAGIC 2. Pivot on `status` column to create one column per status value
# MAGIC 3. For each customer_id, calculate the COUNT of orders per status
# MAGIC 4. Specify pivot values explicitly: `["completed", "pending", "cancelled", "returned"]`
# MAGIC 5. Replace any NULL pivot values with 0 (customers with no orders in a status get 0)
# MAGIC 6. Write to `db_code.pyspark_transformations.customer_status_pivot_ex4` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - One row per customer_id in the output
# MAGIC - Output columns: `customer_id`, `completed`, `pending`, `cancelled`, `returned`
# MAGIC - All count columns must be integers, no nulls

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
status_cols = ["completed", "pending", "cancelled", "returned"]
for sc in status_cols:
    assert sc in result_ex4.columns, f"Missing pivot column: {sc}"

# Check that total across status columns equals total orders for a sample customer
sample_cust = source_ex4.groupBy("customer_id").count().orderBy(F.desc("count")).first().customer_id
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
# MAGIC Convert a wide metrics table back to long format using `stack()`. The source table has one
# MAGIC row per customer with separate columns for each metric. Transform it so each metric becomes
# MAGIC its own row.
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
# MAGIC 6. Write to `db_code.pyspark_transformations.customer_metrics_long_ex5` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Output columns: `customer_id`, `name`, `metric_name`, `metric_value`
# MAGIC - `metric_name` values should be the original column names: "completed_orders",
# MAGIC   "pending_orders", "cancelled_orders"
# MAGIC - Only rows with `metric_value` > 0 in the output
# MAGIC
# MAGIC **Approach**: Use `selectExpr` with `stack(N, val1, val2, ...)` to unpivot columns.
# MAGIC `stack(3, 'completed_orders', completed_orders, ...)` creates one row per value.

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
# MAGIC Create a Python UDF to extract and classify event payloads, apply it to the events table,
# MAGIC and write results to Delta. The UDF must safely handle NULL payloads.
# MAGIC
# MAGIC **Source** (`events_ex6`): Events with JSON payload strings. Some payloads are NULL.
# MAGIC Payload format: `{"page": "/home", "duration": 45}` or `{"item_id": "P-100", "price": 29.99}`.
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
# MAGIC 5. Write to `db_code.pyspark_transformations.classified_events_ex6` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The UDF must handle NULL payloads without errors
# MAGIC - Every row must have a non-null `payload_class`
# MAGIC - Use `@udf` decorator or `udf()` function with explicit return type (StringType)

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

# NULL payload check: rows with null payload must be "no_data"
null_payload_rows = source_ex6.filter("payload IS NULL").count()
if null_payload_rows > 0:
    no_data_count = result_ex6.filter("payload_class = 'no_data'").count()
    assert no_data_count >= null_payload_rows, f"Expected at least {null_payload_rows} 'no_data' rows for NULL payloads, got {no_data_count}"

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
# MAGIC The UDF from Exercise 6 works but serializes data between JVM and Python (slow). Rewrite the
# MAGIC same classification logic using ONLY native Spark functions for 10x+ performance.
# MAGIC
# MAGIC **Source** (`events_ex7`): Same events table as Exercise 6 with JSON payloads and NULLs.
# MAGIC
# MAGIC **Target**: Write to `db_code.pyspark_transformations.classified_events_native_ex7`.
# MAGIC
# MAGIC **The UDF to replace** (from Exercise 6):
# MAGIC ```python
# MAGIC @udf(StringType())
# MAGIC def classify_payload(payload):
# MAGIC     if payload is None or payload == "":
# MAGIC         return "no_data"
# MAGIC     if "price" in payload:
# MAGIC         return "transaction"
# MAGIC     if "duration" in payload:
# MAGIC         return "engagement"
# MAGIC     return "other"
# MAGIC ```
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.pyspark_transformations.events_ex7`
# MAGIC 2. Replicate the EXACT same classification logic using only native Spark functions
# MAGIC    (no Python UDFs, no `@udf`, no `F.udf`)
# MAGIC 3. Create column `payload_class` with the same values: "no_data", "transaction",
# MAGIC    "engagement", "other"
# MAGIC 4. Select columns: `event_id`, `user_id`, `event_type`, `event_ts`, `payload_class`
# MAGIC 5. Write to `db_code.pyspark_transformations.classified_events_native_ex7` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Zero Python UDFs. Only `F.when()`, `F.col()`, `.contains()`, `.isNull()`, etc.
# MAGIC - Output must be byte-for-byte identical to what the UDF would produce
# MAGIC - NULL and empty string payloads both map to "no_data"

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

# Cross-check against UDF logic: NULL payloads -> "no_data"
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
# MAGIC Build a multi-step transformation pipeline that reads orders, enriches them with item
# MAGIC summaries as a struct column, applies business logic, and writes using `replaceWhere` for
# MAGIC targeted partition overwrite. The target table already has cancelled orders from setup -
# MAGIC your write must preserve them.
# MAGIC
# MAGIC **Source** (`orders_ex8`): Orders pre-joined with customer info (customer_name, region). ~100 rows.
# MAGIC **Source** (`order_items_ex8`): Line items per order (order_id, product_id, quantity,
# MAGIC unit_price). ~200 rows. Some orphan order_ids.
# MAGIC **Target** (`enriched_orders_ex8`): Pre-populated with cancelled orders from setup.
# MAGIC After your write, it should contain BOTH cancelled (from setup) AND completed (from your write).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `orders_ex8` and `order_items_ex8` from `db_code.pyspark_transformations`
# MAGIC 2. Aggregate `order_items_ex8` per `order_id`: compute `total_items` (sum of quantity)
# MAGIC    and `total_value` (sum of quantity * unit_price)
# MAGIC 3. Join the aggregated items onto `orders_ex8` (left join on order_id)
# MAGIC 4. Create a struct column `items_summary` with fields: `total_items` (LONG),
# MAGIC    `total_value` (DOUBLE)
# MAGIC 5. Add column `amount_bucket`: "high" if amount >= 100, "medium" if amount >= 25,
# MAGIC    "low" otherwise
# MAGIC 6. Select final columns: `order_id`, `customer_id`, `customer_name`, `region`, `amount`,
# MAGIC    `status`, `amount_bucket`, `items_summary`, `order_date`, `updated_at`
# MAGIC 7. Filter to only `status = 'completed'`
# MAGIC 8. Write to `db_code.pyspark_transformations.enriched_orders_ex8` using `replaceWhere`
# MAGIC    with condition `"status = 'completed'"` (overwrite only the completed partition)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Orders with no matching items: `items_summary.total_items` = 0,
# MAGIC   `items_summary.total_value` = 0.0
# MAGIC - After your write, the table must contain both cancelled (from setup) AND completed rows
# MAGIC - Use DataFrame API (not SQL) for transformations

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex8
# TODO: Multi-step pipeline - aggregate items, join, create struct, write with replaceWhere
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex8 = spark.table(f"{CATALOG}.{SCHEMA}.enriched_orders_ex8")

# The table should have both completed (from user write) and cancelled (from setup)
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

print("Exercise 8 passed!")
