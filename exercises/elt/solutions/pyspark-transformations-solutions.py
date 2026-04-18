# Databricks notebook source
# MAGIC %md
# MAGIC # Solutions: PySpark DataFrame Transformations
# MAGIC Hints, reference solutions, and common mistakes for each exercise.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "pyspark_transformations"
BASE_SCHEMA = "elt"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Basic select/filter/withColumn
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Use `.filter()` or `.where()` to keep only completed orders
# MAGIC 2. `F.round()` takes a column expression and the number of decimal places
# MAGIC 3. Chain `.filter()`, `.select()`, `.withColumn()` in any order, then `.write.mode("overwrite").saveAsTable()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to round the tax calculation (leads to floating-point precision issues)
# MAGIC - Filtering out $0 amounts or NULL customer_ids when the spec says to keep them
# MAGIC - Using `.write.format("delta")` instead of `.saveAsTable()` with three-level namespace

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex1
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex1")

result = (
    df
    .filter(F.col("status") == "completed")
    .select("order_id", "customer_id", "amount", "order_date")
    .withColumn("amount_with_tax", F.round(F.col("amount") * 1.08, 2))
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.completed_orders_ex1")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Column renaming and type casting
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Use `.withColumnRenamed("old", "new")` or `.select(F.col("old").alias("new"), ...)` for renaming
# MAGIC 2. For casting, use `.withColumn("col", F.col("col").cast(DateType()))` or `.cast("date")`
# MAGIC 3. You can chain multiple `.withColumnRenamed()` calls, or do all renames in a single `.select()`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `.toDF()` with positional names - fragile if column order changes
# MAGIC - Forgetting to cast signupDate to DateType (leaving it as STRING)
# MAGIC - Using `F.to_date()` without a format string when the source format is ambiguous (safe here since "yyyy-MM-dd" is the default)

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex2
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

df = spark.table(f"{CATALOG}.{SCHEMA}.customers_ex2")

result = (
    df
    .withColumnRenamed("customerId", "customer_id")
    .withColumnRenamed("customerName", "customer_name")
    .withColumnRenamed("emailAddress", "email_address")
    .withColumnRenamed("customerRegion", "region")
    .withColumnRenamed("customerTier", "tier")
    .withColumn("signup_date", F.col("signupDate").cast(DateType()))
    .drop("signupDate")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customers_clean_ex2")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: Complex when/otherwise chains - multi-condition order tier
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Order matters in `when` chains - evaluate cancelled/returned FIRST before checking amounts
# MAGIC 2. Use `F.when(...).when(...).otherwise(...)` chaining, not nested if/else
# MAGIC 3. For `priority_score`, you can chain another `when/otherwise` on the `order_tier` column
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Checking amount before status: a cancelled $500 order should be "inactive", not "premium"
# MAGIC - Using `== None` instead of `.isNull()` for NULL amount checks
# MAGIC - Forgetting that `F.when` conditions are evaluated top-to-bottom and first match wins
# MAGIC - Not handling NULL amounts: `F.col("amount") < 50` returns NULL (not True) when amount is NULL

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex3
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex3")

result = df.withColumn(
    "order_tier",
    F.when(F.col("status").isin("cancelled", "returned"), "inactive")
    .when(F.col("amount") >= 200, "premium")
    .when((F.col("amount") >= 50) & (F.col("amount") < 200), "standard")
    .when((F.col("amount") > 0) & (F.col("amount") < 50), "budget")
    .otherwise("free")
).withColumn(
    "priority_score",
    F.when(F.col("order_tier") == "premium", 4)
    .when(F.col("order_tier") == "standard", 3)
    .when(F.col("order_tier") == "budget", 2)
    .when(F.col("order_tier") == "free", 1)
    .otherwise(0)
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.tiered_orders_ex3")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Pivot - order counts by status per customer
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Use `.groupBy("customer_id").pivot("status").agg(F.count("order_id"))`
# MAGIC 2. After pivot, NULL means zero orders for that status - use `.fillna(0)` to replace
# MAGIC 3. Specify the pivot values explicitly for deterministic column ordering: `.pivot("status", ["completed", "pending", "cancelled", "returned"])`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Not specifying pivot values explicitly, which makes column order non-deterministic across runs
# MAGIC - Using `F.sum` instead of `F.count` (the exercise asks for counts, not totals)
# MAGIC - Forgetting `.fillna(0)` - pivot produces NULLs for missing combinations

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex4
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex4")

result = (
    df
    .groupBy("customer_id")
    .pivot("status", ["completed", "pending", "cancelled", "returned"])
    .agg(F.count("order_id"))
    .fillna(0)
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_status_pivot_ex4")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Unpivot with stack() - wide metrics to long format
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. `stack(N, 'name1', col1, 'name2', col2, ...)` creates N rows per input row, each with (name, value) pair
# MAGIC 2. Use `.selectExpr("customer_id", "name", "stack(3, 'completed_orders', completed_orders, ...) as (metric_name, metric_value)")`
# MAGIC 3. After stack, filter out rows where `metric_value = 0`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `unpivot()` (Spark 3.4+) instead of `stack()` - both work, but `stack()` works on all Databricks versions
# MAGIC - Forgetting to quote the column name strings inside stack: `'completed_orders'` (literal string) vs `completed_orders` (column reference)
# MAGIC - Including `total_revenue` in the stack - it has a different type (DOUBLE vs BIGINT) and the exercise says to exclude it

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex5
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.customer_metrics_ex5")

result = (
    df
    .selectExpr(
        "customer_id",
        "name",
        "stack(3, 'completed_orders', completed_orders, 'pending_orders', pending_orders, 'cancelled_orders', cancelled_orders) as (metric_name, metric_value)"
    )
    .filter("metric_value > 0")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customer_metrics_long_ex5")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Python UDF for custom logic with null handling
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Define a regular Python function first, then wrap it with `@udf(StringType())` or `udf(func, StringType())`
# MAGIC 2. Inside the function, check `if payload is None` before doing string operations
# MAGIC 3. Use Python `in` operator for substring checks: `"price" in payload`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Not handling `None` in the Python function - PySpark passes Python `None` for SQL NULL
# MAGIC - Forgetting to specify the return type (`StringType()`) in the UDF declaration
# MAGIC - Using `F.col("payload").contains("price")` instead of the UDF - that is the native approach (better for performance but not what this exercise asks for)
# MAGIC - Performance note: Python UDFs serialize data between JVM and Python. In production, prefer native Spark functions (Exercise 7 covers this)

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex6
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(StringType())
def classify_payload(payload):
    if payload is None or payload == "":
        return "no_data"
    if "price" in payload:
        return "transaction"
    if "duration" in payload:
        return "engagement"
    return "other"

df = spark.table(f"{CATALOG}.{SCHEMA}.events_ex6")

result = (
    df
    .withColumn("payload_class", classify_payload(F.col("payload")))
    .select("event_id", "user_id", "event_type", "event_ts", "payload_class")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.classified_events_ex6")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 7: Replace UDF with native Spark functions
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Use `F.when()` chains - same logic as the UDF but expressed as column expressions
# MAGIC 2. Check NULL/empty first: `F.when(F.col("payload").isNull() | (F.col("payload") == ""), "no_data")`
# MAGIC 3. Use `.contains("price")` for substring matching: `F.col("payload").contains("price")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to handle empty strings (the UDF checks `payload == ""`, so native must too)
# MAGIC - Wrong order: checking "duration" before "price" when a payload could contain both (though unlikely, order should match the UDF)
# MAGIC - Using `F.regexp_extract` when `.contains()` is sufficient (overcomplicating it)

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex7
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.events_ex7")

result = (
    df
    .withColumn(
        "payload_class",
        F.when(F.col("payload").isNull() | (F.col("payload") == ""), "no_data")
        .when(F.col("payload").contains("price"), "transaction")
        .when(F.col("payload").contains("duration"), "engagement")
        .otherwise("other")
    )
    .select("event_id", "user_id", "event_type", "event_ts", "payload_class")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.classified_events_native_ex7")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 8: Chained transform pipeline with replaceWhere
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. First aggregate order_items: `.groupBy("order_id").agg(F.sum("quantity").alias("total_items"), F.sum(F.col("quantity") * F.col("unit_price")).alias("total_value"))`
# MAGIC 2. After the left join, orders without items will have NULL aggregates - use `F.coalesce()` or `.fillna()` to replace with 0
# MAGIC 3. Create the struct with `F.struct(F.col("total_items").cast("long"), F.col("total_value").cast("double")).alias("items_summary")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using inner join instead of left join - orders without items would be dropped
# MAGIC - Not handling NULL total_items/total_value from the left join before creating the struct
# MAGIC - Forgetting `replaceWhere` syntax: `.option("replaceWhere", "status = 'completed'")` on the writer
# MAGIC - Writing ALL statuses instead of filtering to completed first - replaceWhere requires the written data to match the condition
# MAGIC - Not casting struct fields to the correct types (LONG for total_items, DOUBLE for total_value)

# COMMAND ----------

# EXERCISE_KEY: pyspark_ex8
from pyspark.sql import functions as F

orders = spark.table(f"{CATALOG}.{SCHEMA}.orders_ex8")
items = spark.table(f"{CATALOG}.{SCHEMA}.order_items_ex8")

# Step 1: Aggregate items per order
items_agg = (
    items
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_items"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("total_value")
    )
)

# Step 2: Join and handle nulls from left join
enriched = (
    orders
    .join(items_agg, "order_id", "left")
    .withColumn("total_items", F.coalesce(F.col("total_items"), F.lit(0)))
    .withColumn("total_value", F.coalesce(F.col("total_value"), F.lit(0.0)))
)

# Step 3: Create struct column and amount_bucket
result = (
    enriched
    .withColumn(
        "items_summary",
        F.struct(
            F.col("total_items").cast("long").alias("total_items"),
            F.col("total_value").cast("double").alias("total_value")
        )
    )
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") >= 100, "high")
        .when(F.col("amount") >= 25, "medium")
        .otherwise("low")
    )
    .select(
        "order_id", "customer_id", "customer_name", "region",
        "amount", "status", "amount_bucket", "items_summary",
        "order_date", "updated_at"
    )
    .filter(F.col("status") == "completed")
)

# Step 4: Write with replaceWhere - only overwrite completed rows
(
    result
    .write
    .mode("overwrite")
    .option("replaceWhere", "status = 'completed'")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.enriched_orders_ex8")
)
