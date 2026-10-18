# Databricks notebook source
# MAGIC %md
# MAGIC # Solutions: Complex Data Types
# MAGIC Hints, reference solutions, and common mistakes for each exercise.
# MAGIC Try each exercise before you read its solution.

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
# MAGIC **Hints**:
# MAGIC 1. Struct fields are accessed with dot notation: `col("address.city")` or `df.address.city`
# MAGIC 2. You can use `.select()` with `col("address.city").alias("city")` to rename
# MAGIC 3. Use `.write.mode("overwrite").saveAsTable()` for the final write
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Filtering out rows with NULL struct fields: C-009, C-010 and C-011 must stay
# MAGIC - Forgetting `.alias()` when you reach the field as `df.address.city`: that column comes out named `address.city`, so name it `city` explicitly

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
# MAGIC **Hints**:
# MAGIC 1. `explode_outer` turns each array element into its own row, and when the array is NULL or empty it keeps the row with a NULL element
# MAGIC 2. Use `.alias("tag")` to name the exploded column
# MAGIC 3. The parent columns (order_id, customer_id, amount) are automatically duplicated for each exploded row
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `explode` instead of `explode_outer` - `explode` silently drops rows whose array is NULL or empty (ORD-007 and ORD-009 would vanish)
# MAGIC - Forgetting to alias the exploded column, which defaults to "col"
# MAGIC - Using `F.flatten` instead of `F.explode` - flatten merges nested arrays, explode converts array elements to rows

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
# MAGIC **Hints**:
# MAGIC 1. Define the schema as a DDL string: `"page STRING, duration_sec INT, referrer STRING"`
# MAGIC 2. Use `F.from_json(F.col("payload"), schema)` to parse the string into a struct
# MAGIC 3. After parsing, access fields with dot notation: `col("parsed.page")`
# MAGIC 4. NULL payloads (EVT-108, EVT-112) need no special handling: `from_json` returns NULL, so `parsed.page` is NULL too.
# MAGIC 5. Malformed JSON (EVT-113) needs none either: `from_json` returns a struct with every field NULL, so `page` and `duration_sec` come out NULL.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Passing the `payload` column to `schema_of_json`: it only accepts a literal JSON string, not a column, and from a sample it infers `duration_sec` as BIGINT, not INT. Write the DDL schema yourself.
# MAGIC - Defining schema with wrong types (e.g., STRING for duration_sec instead of INT)
# MAGIC - Using `get_json_object` instead of `from_json` - `get_json_object` returns strings, `from_json` returns typed structs

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
# MAGIC ## Exercise 4: Map operations - map_keys, element_at, size
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. A MAP column has its own functions: one returns its keys as an array, one looks up a single key.
# MAGIC 2. `F.map_keys()` returns the keys; `F.element_at(map, key)` returns the value, or NULL when the key is missing.
# MAGIC 3. `F.size()` counts the entries of an array or a map, which gives `num_attributes`.
# MAGIC 4. `F.col("attributes")["color"]` does the same lookup and also returns NULL for a missing key
# MAGIC 5. `F.col("attributes.color")` also works on a map with STRING keys, but it reads like a struct field; `element_at` makes the map lookup explicit
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing `map_keys` (returns array of keys) with `map_values` (returns array of values)
# MAGIC - Assuming `element_at` throws on missing keys - it returns NULL
# MAGIC - Forgetting that `size()` on an empty map returns 0, not NULL

# COMMAND ----------

# EXERCISE_KEY: complex_ex4
from pyspark.sql import functions as F

df = spark.table(f"{CATALOG}.{SCHEMA}.products_map_ex4")

result = df.select(
    "product_id",
    "name",
    F.map_keys("attributes").alias("attribute_names"),
    F.element_at("attributes", "color").alias("color"),
    F.size("attributes").alias("num_attributes")
)

result.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.map_results_ex4")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: collect_list/collect_set - aggregate into arrays
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. One `.groupBy("order_id")` with three aggregates inside a single `.agg()`.
# MAGIC 2. `F.collect_list` keeps every value including repeats; `F.collect_set` keeps each distinct value once.
# MAGIC 3. `F.sum("quantity")` gives `total_quantity`.
# MAGIC 4. `F.array_distinct(F.collect_list(...))` gives the same set; `collect_set` is the shorter way.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing `collect_list` and `collect_set` - list keeps duplicates, set removes them
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
# MAGIC **Hints**:
# MAGIC 1. `F.transform("temperatures", lambda x: ...)` applies a function to each element without exploding
# MAGIC 2. `F.filter("temperatures", lambda x: x > 80)` keeps only elements matching the predicate
# MAGIC 3. Cast the Celsius result to INT: `((x - 32) * 5 / 9).cast("int")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `explode` + `groupBy` + `collect_list` instead of `transform`: the constraints rule out `explode`, and the regroup costs a shuffle that `transform` never needs
# MAGIC - Forgetting to cast the Celsius result to INT (the formula produces a float)
# MAGIC - Expecting `size()` on a NULL array to return 0: with ANSI mode on (the default on serverless and Databricks Runtime 17.0+) it returns NULL, which the validate cell checks for S-007; with ANSI off it returns -1
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
# MAGIC **Hints**:
# MAGIC 1. Start by extracting `body.user.id` and `body.user.name` as flat columns
# MAGIC 2. `explode_outer` on `body.orders` keeps R-004 as a row with a NULL order until step 7 drops it; plain `explode` drops it straight away, with the same final 11 rows
# MAGIC 3. Alias the exploded order (for example `.alias("order")`), then read its fields with `F.col("order.order_id")` and `F.col("order.items")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to filter out rows where both `sku` AND `qty` are NULL after the second explode
# MAGIC - Not aliasing the exploded columns, causing name collisions (two columns both named "col")
# MAGIC - Trying to explode both levels in one step - you must explode `orders` first, then explode `items` from the result
# MAGIC - Using `F.col("body.orders.items")` before exploding orders: it returns an ARRAY<ARRAY<STRUCT>>, one inner array per order, so explode `orders` first

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
# MAGIC **Hints**:
# MAGIC 1. Use `F.struct()` to create a struct column: `F.struct("col1", "col2").alias("my_struct")`
# MAGIC 2. Use `F.first()` inside `.agg()` to get the customer info (same for all rows in a group)
# MAGIC 3. Use `F.collect_list(F.struct("product_name", "quantity", "unit_price"))` to build an array of structs
# MAGIC 4. `F.named_struct(F.lit("name"), F.first("customer_name"), ...)` builds the same struct with the field names inline
# MAGIC 5. `first()` keeps ORD-003's NULL email, so `customer.email` is NULL with no extra code.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Wrapping the raw columns in one struct, `F.first(F.struct("customer_name", "customer_email", "customer_tier"))`, without renaming: the fields keep the names `customer_name`, `customer_email`, `customer_tier`, so `customer.name` does not exist. Alias each field to `name`, `email`, `tier`.
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
