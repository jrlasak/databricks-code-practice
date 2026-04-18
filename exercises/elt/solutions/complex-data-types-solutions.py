# Databricks notebook source
# MAGIC %md
# MAGIC # Solutions: Complex Data Types
# MAGIC Hints, reference solutions, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "complex_data_types"
BASE_SCHEMA = "elt"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Access struct fields with dot notation
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Struct fields are accessed with dot notation: `col("address.city")` or `df.address.city`
# MAGIC 2. You can use `.select()` with `col("address.city").alias("city")` to rename
# MAGIC 3. Use `.write.mode("overwrite").saveAsTable()` for the final write
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `address["city"]` (Python dict syntax) instead of `address.city` (dot notation)
# MAGIC - Filtering out rows with NULL struct fields when the spec says to keep them
# MAGIC - Forgetting `.alias()` when extracting struct fields - without it, the column name is `address.city` (with dot)
# MAGIC - Assuming a NULL struct means the row should be excluded - `NULL.city` returns NULL, not an error

# COMMAND ----------

# EXERCISE_KEY: complex_ex1
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.customers_struct_ex1")

result = df.select(
    "customer_id",
    "name",
    "tier",
    F.col("address.city").alias("city"),
    F.col("address.state").alias("state")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.flat_customers_ex1")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Explode array column into rows
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. `explode_outer` converts each array element into its own row AND preserves NULL arrays
# MAGIC 2. Use `.alias("tag")` to name the exploded column
# MAGIC 3. The parent columns (order_id, customer_id, amount) are automatically duplicated for each exploded row
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `explode` instead of `explode_outer` - `explode` silently drops rows where the array is NULL
# MAGIC - Forgetting to alias the exploded column, which defaults to "col"
# MAGIC - Using `F.flatten` instead of `F.explode` - flatten merges nested arrays, explode converts array elements to rows
# MAGIC - Expecting empty arrays to produce a NULL row - `explode_outer` on empty array produces 0 rows (only NULL arrays produce a NULL row)

# COMMAND ----------

# EXERCISE_KEY: complex_ex2
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.orders_tags_ex2")

result = df.select(
    "order_id",
    "customer_id",
    "amount",
    F.explode_outer("tags").alias("tag")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.exploded_tags_ex2")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: from_json - parse JSON string column into struct
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Define the schema as a DDL string: `"page STRING, duration_sec INT, referrer STRING"`
# MAGIC 2. Use `F.from_json(F.col("payload"), schema)` to parse the string into a struct
# MAGIC 3. After parsing, access fields with dot notation: `col("parsed.page")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `schema_of_json` on a NULL row, which returns NULL schema and causes all rows to parse as NULL
# MAGIC - Defining schema with wrong types (e.g., STRING for duration_sec instead of INT)
# MAGIC - Using `get_json_object` instead of `from_json` - `get_json_object` returns strings, `from_json` returns typed structs
# MAGIC - Not handling NULL payloads: `from_json(NULL)` returns NULL struct, and accessing `.page` on NULL struct returns NULL (correct behavior, no special handling needed)
# MAGIC - Worrying about malformed JSON: `from_json` on invalid JSON returns NULL struct (same as NULL input)

# COMMAND ----------

# EXERCISE_KEY: complex_ex3
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.events_json_ex3")

schema = "page STRING, duration_sec INT, referrer STRING"

result = (
    df
    .withColumn("parsed", F.from_json(F.col("payload"), schema))
    .select(
        "event_id",
        "user_id",
        "event_type",
        "event_ts",
        F.col("parsed.page").alias("page"),
        F.col("parsed.duration_sec").alias("duration_sec")
    )
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.parsed_events_ex3")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Map operations - map_keys, map_values, element_at
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. `F.map_keys("attributes")` returns an ARRAY of all keys in the map
# MAGIC 2. `F.element_at("attributes", "color")` looks up a key and returns the value (NULL if missing)
# MAGIC 3. `F.size(F.map_keys("attributes"))` counts the number of key-value pairs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `attributes["color"]` (Python bracket syntax) instead of `element_at`
# MAGIC - Confusing `map_keys` (returns array of keys) with `map_values` (returns array of values)
# MAGIC - Assuming `element_at` throws on missing keys - it returns NULL
# MAGIC - Forgetting that `size()` on an empty map returns 0, not NULL
# MAGIC - Using `F.col("attributes.color")` - dot notation does not work on maps, only on structs

# COMMAND ----------

# EXERCISE_KEY: complex_ex4
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.products_map_ex4")

result = df.select(
    "product_id",
    "name",
    F.map_keys("attributes").alias("attribute_names"),
    F.element_at("attributes", "color").alias("color"),
    F.size(F.map_keys("attributes")).alias("num_attributes")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.map_results_ex4")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: collect_list/collect_set - aggregate into arrays
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. `F.collect_list("product_id")` keeps duplicates; `F.collect_set("product_id")` removes them
# MAGIC 2. Both are aggregate functions, so use them inside `.agg()` after `.groupBy("order_id")`
# MAGIC 3. `F.sum("quantity")` computes total quantity per group
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing `collect_list` and `collect_set` - list keeps duplicates, set removes them
# MAGIC - Using `F.array_distinct(F.collect_list(...))` instead of `collect_set` - works but unnecessarily complex
# MAGIC - Forgetting that `collect_list` order is non-deterministic within a group (don't assert exact order)
# MAGIC - Using `count` instead of `sum("quantity")` for total quantity - `count` counts rows, `sum` totals the quantity values

# COMMAND ----------

# EXERCISE_KEY: complex_ex5
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.order_items_ex5")

result = (
    df
    .groupBy("order_id")
    .agg(
        F.collect_list("product_id").alias("all_products"),
        F.collect_set("product_id").alias("unique_products"),
        F.sum("quantity").alias("total_quantity")
    )
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.order_products_ex5")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Higher-order functions - transform() and filter() on arrays
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. `F.transform("temperatures", lambda x: ...)` applies a function to each element without exploding
# MAGIC 2. `F.filter("temperatures", lambda x: x > 80)` keeps only elements matching the predicate
# MAGIC 3. Cast the Celsius result to INT: `((x - 32) * 5 / 9).cast("int")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `explode` + `groupBy` + `collect_list` instead of `transform` - works but misses the point of higher-order functions
# MAGIC - Forgetting to cast the Celsius result to INT (the formula produces a float)
# MAGIC - Expecting `size()` on NULL array to return 0 - it returns -1 in Spark
# MAGIC - Using SQL syntax `transform(col, x -> ...)` in PySpark - use Python lambda syntax `F.transform(col, lambda x: ...)`
# MAGIC - Applying `filter` to the Celsius-converted array instead of the original Fahrenheit array

# COMMAND ----------

# EXERCISE_KEY: complex_ex6
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.sensor_readings_ex6")

result = df.select(
    "sensor_id",
    "reading_date",
    F.transform("temperatures", lambda x: ((x - 32) * 5 / 9).cast("int")).alias("temps_celsius"),
    F.filter("temperatures", lambda x: x > 80).alias("high_temps"),
).withColumn(
    "num_high_temps", F.size("high_temps")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.transformed_arrays_ex6")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 7: Flatten deeply nested JSON - multi-level structs with arrays of structs
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Start by extracting `body.user.id` and `body.user.name` as flat columns
# MAGIC 2. Use `explode_outer` (not `explode`) for `body.orders` - this preserves rows where orders is NULL
# MAGIC 3. After the first explode, access the exploded struct's fields: `col("order.order_id")`, `col("order.items")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `explode` instead of `explode_outer` - `explode` drops rows with NULL arrays (R-004 would vanish silently)
# MAGIC - Forgetting to filter out rows where both `sku` AND `qty` are NULL after the second explode
# MAGIC - Not aliasing the exploded columns, causing name collisions (two columns both named "col")
# MAGIC - Trying to explode both levels in one step - you must explode `orders` first, then explode `items` from the result
# MAGIC - Using `F.col("body.orders.items")` before exploding orders - you cannot reach into an array's struct fields without exploding first

# COMMAND ----------

# EXERCISE_KEY: complex_ex7
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.api_responses_ex7")

# Step 1: Extract user fields and explode orders
orders_exploded = (
    df
    .select(
        "response_id",
        F.col("body.user.id").alias("user_id"),
        F.col("body.user.name").alias("user_name"),
        F.explode_outer("body.orders").alias("order")
    )
)

# Step 2: Extract order fields and explode items
items_exploded = (
    orders_exploded
    .select(
        "response_id",
        "user_id",
        "user_name",
        F.col("order.order_id").alias("order_id"),
        F.col("order.total").alias("order_total"),
        F.explode_outer("order.items").alias("item")
    )
)

# Step 3: Extract item fields and filter NULL items
result = (
    items_exploded
    .select(
        "response_id",
        "user_id",
        "user_name",
        "order_id",
        "order_total",
        F.col("item.sku").alias("sku"),
        F.col("item.qty").alias("qty")
    )
    .filter(F.col("sku").isNotNull() | F.col("qty").isNotNull())
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.flat_items_ex7")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 8: Build complex types from flat data - struct and array columns
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC **Hints** (try before peeking):
# MAGIC 1. Use `F.struct()` to create a struct column: `F.struct("col1", "col2").alias("my_struct")`
# MAGIC 2. Use `F.first()` inside `.agg()` to get the customer info (same for all rows in a group)
# MAGIC 3. Use `F.collect_list(F.struct("product_name", "quantity", "unit_price"))` to build an array of structs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `named_struct` (SQL function) in PySpark - use `F.struct()` instead
# MAGIC - Forgetting that `F.struct()` in PySpark takes column names as strings or Column objects
# MAGIC - Using `F.first("customer_name")` separately for each field then wrapping in `F.struct()` -
# MAGIC   this works but you can also `F.first(F.struct("customer_name", "customer_email", "customer_tier"))`
# MAGIC - Not handling the NULL email in ORD-003 - `first()` preserves NULL values, which is correct
# MAGIC - Using `collect_set` instead of `collect_list` for items - `collect_set` would deduplicate identical items

# COMMAND ----------

# EXERCISE_KEY: complex_ex8
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.flat_order_lines_ex8")

result = (
    df
    .groupBy("order_id")
    .agg(
        F.struct(
            F.first("customer_name").alias("name"),
            F.first("customer_email").alias("email"),
            F.first("customer_tier").alias("tier")
        ).alias("customer"),
        F.collect_list(
            F.struct(
                F.col("product_name"),
                F.col("quantity"),
                F.col("unit_price")
            )
        ).alias("items"),
        F.count("*").alias("item_count")
    )
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.structured_orders_ex8")
