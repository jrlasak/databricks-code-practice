# Databricks notebook source
# MAGIC %md
# MAGIC # Complex Data Types
# MAGIC **Topic**: ELT | **Exercises**: 8 | **Total Time**: ~85 min
# MAGIC
# MAGIC Practice working with complex Spark SQL types: structs, arrays, maps, `from_json`, `explode`,
# MAGIC `collect_list`, higher-order functions (`transform`, `filter`), and nested JSON flattening.
# MAGIC Every exercise reads from Delta tables in Unity Catalog and writes results to Delta.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/complex-data-types-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (created by `setup/complex-data-types-setup.py`):
# MAGIC - `db_code.complex_data_types.customers_struct_ex1` - customers with struct address column (11 rows)
# MAGIC - `db_code.complex_data_types.orders_tags_ex2` - orders with array of tags (9 rows)
# MAGIC - `db_code.complex_data_types.events_json_ex3` - events with JSON string payloads (13 rows)
# MAGIC - `db_code.complex_data_types.products_map_ex4` - products with MAP attributes column (8 rows)
# MAGIC - `db_code.complex_data_types.order_items_ex5` - line items for grouping/aggregation (15 rows)
# MAGIC - `db_code.complex_data_types.sensor_readings_ex6` - sensors with array of temperatures (7 rows)
# MAGIC - `db_code.complex_data_types.api_responses_ex7` - deeply nested structs with arrays of structs (6 rows)
# MAGIC - `db_code.complex_data_types.flat_order_lines_ex8` - flat order+item rows to restructure (9 rows)
# MAGIC
# MAGIC **Prerequisites**: Run `00_Setup.py` once, then run the setup cell below.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/complex-data-types-setup

# COMMAND ----------

# MAGIC %md
# MAGIC Setup created per-exercise source tables in `db_code.complex_data_types`. Each exercise reads
# MAGIC from its own source table and writes to its own output table. Exercises are independent - you
# MAGIC can skip to any exercise.
# MAGIC
# MAGIC | Exercise | Source Table | Output Table | Difficulty |
# MAGIC |----------|-------------|--------------|------------|
# MAGIC | 1 | `customers_struct_ex1` (11 rows) | `flat_customers_ex1` | Easy |
# MAGIC | 2 | `orders_tags_ex2` (9 rows) | `exploded_tags_ex2` | Easy |
# MAGIC | 3 | `events_json_ex3` (13 rows) | `parsed_events_ex3` | Medium |
# MAGIC | 4 | `products_map_ex4` (8 rows) | `map_results_ex4` | Medium |
# MAGIC | 5 | `order_items_ex5` (15 rows) | `order_products_ex5` | Medium |
# MAGIC | 6 | `sensor_readings_ex6` (7 rows) | `transformed_arrays_ex6` | Medium |
# MAGIC | 7 | `api_responses_ex7` (6 rows) | `flat_items_ex7` | Hard |
# MAGIC | 8 | `flat_order_lines_ex8` (9 rows) | `structured_orders_ex8` | Hard |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Access struct fields with dot notation
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Extract fields from a struct column using dot notation and write a flat result to Delta.
# MAGIC
# MAGIC **Source** (`customers_struct_ex1`): 11 customers with a struct column `address` containing
# MAGIC `street`, `city`, `state`, `zip`. Edge cases: C-009 has NULL street, C-010 has NULL city,
# MAGIC C-011 has an entirely NULL address struct.
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.flat_customers_ex1`. Expected 11 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.customers_struct_ex1`
# MAGIC 2. Extract `address.city` and `address.state` as top-level columns named `city` and `state`
# MAGIC 3. Select final columns: `customer_id`, `name`, `tier`, `city`, `state`
# MAGIC 4. Write as a Delta table to `db_code.complex_data_types.flat_customers_ex1` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Keep ALL rows including those with NULL struct fields or entirely NULL address
# MAGIC - The output must have exactly 5 columns

# COMMAND ----------

# EXERCISE_KEY: complex_ex1
# TODO: Extract struct fields to flat columns, write to Delta
# Your code here

# COMMAND ----------

result_ex1 = spark.table(f"{CATALOG}.{SCHEMA}.flat_customers_ex1")

# Row count: all 11 customers
assert result_ex1.count() == 11, f"Expected 11 rows, got {result_ex1.count()}"

# Schema check: exactly 5 flat columns
expected_cols = {"customer_id", "name", "tier", "city", "state"}
assert set(result_ex1.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex1.columns)}"

# Value check: known city
alice = result_ex1.filter("customer_id = 'C-001'").first()
assert alice.city == "Seattle", f"C-001 city should be Seattle, got {alice.city}"
assert alice.state == "WA", f"C-001 state should be WA, got {alice.state}"

# NULL struct field preserved (C-010 has NULL city)
c010 = result_ex1.filter("customer_id = 'C-010'").first()
assert c010.city is None, f"C-010 should have NULL city, got {c010.city}"
assert c010.state == "CA", f"C-010 state should be CA, got {c010.state}"

# Entirely NULL struct (C-011)
c011 = result_ex1.filter("customer_id = 'C-011'").first()
assert c011.city is None, f"C-011 should have NULL city, got {c011.city}"
assert c011.state is None, f"C-011 should have NULL state, got {c011.state}"

print("Exercise 1 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Explode array column into rows
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Explode an array column into individual rows, keeping the parent columns intact.
# MAGIC Handle empty arrays and NULL arrays properly.
# MAGIC
# MAGIC **Source** (`orders_tags_ex2`): 9 orders, each with an `ARRAY<STRING>` column `tags`.
# MAGIC ORD-007 has an empty array (0 tags). ORD-009 has NULL tags.
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.exploded_tags_ex2`. Expected 18 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.orders_tags_ex2`
# MAGIC 2. Explode the `tags` array so each tag gets its own row
# MAGIC 3. Use `explode_outer` so rows with NULL or empty arrays produce a row with NULL tag
# MAGIC 4. Name the exploded column `tag` (singular)
# MAGIC 5. Select final columns: `order_id`, `customer_id`, `amount`, `tag`
# MAGIC 6. Write as a Delta table to `db_code.complex_data_types.exploded_tags_ex2` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - ORD-005 has 4 tags and should produce 4 rows
# MAGIC - ORD-009 (NULL tags) should produce 1 row with NULL tag
# MAGIC - ORD-007 (empty array) produces 1 row with NULL tag via `explode_outer`

# COMMAND ----------

# EXERCISE_KEY: complex_ex2
# TODO: Explode the tags array into rows, write to Delta
# Your code here

# COMMAND ----------

result_ex2 = spark.table(f"{CATALOG}.{SCHEMA}.exploded_tags_ex2")

# Row count: explode_outer produces 1 row for both empty arrays and NULL arrays
# ORD-001=3, ORD-002=1, ORD-003=2, ORD-004=1, ORD-005=4, ORD-006=2,
# ORD-007(empty)=1, ORD-008=3, ORD-009(NULL)=1 -> total = 18
assert result_ex2.count() == 18, f"Expected 18 rows, got {result_ex2.count()}"

# Schema check
expected_cols = {"order_id", "customer_id", "amount", "tag"}
assert set(result_ex2.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex2.columns)}"

# ORD-005 should have 4 rows
ord005_count = result_ex2.filter("order_id = 'ORD-005'").count()
assert ord005_count == 4, f"ORD-005 should have 4 rows, got {ord005_count}"

# ORD-009 (NULL tags) should produce 1 row with NULL tag
ord009 = result_ex2.filter("order_id = 'ORD-009'")
assert ord009.count() == 1, f"ORD-009 should have 1 row, got {ord009.count()}"
assert ord009.first().tag is None, f"ORD-009 tag should be NULL, got {ord009.first().tag}"

# ORD-007 (empty array) should produce 1 row with NULL tag via explode_outer
ord007 = result_ex2.filter("order_id = 'ORD-007'")
assert ord007.count() == 1, f"ORD-007 (empty array) should have 1 row, got {ord007.count()}"
assert ord007.first().tag is None, f"ORD-007 tag should be NULL, got {ord007.first().tag}"

# ORD-002 should have exactly 1 row
ord002_count = result_ex2.filter("order_id = 'ORD-002'").count()
assert ord002_count == 1, f"ORD-002 should have 1 row, got {ord002_count}"

print("Exercise 2 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: from_json - parse JSON string column into struct
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Parse a STRING column containing JSON into a typed struct using `from_json`, then extract
# MAGIC nested fields as top-level columns.
# MAGIC
# MAGIC **Source** (`events_json_ex3`): 13 events. The `payload` column is a STRING containing JSON
# MAGIC like `{"page": "/home", "duration_sec": 45, "referrer": "google"}`. 2 rows have NULL payload,
# MAGIC 1 row has malformed JSON (non-JSON string).
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.parsed_events_ex3`. Expected 13 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.events_json_ex3`
# MAGIC 2. Define the JSON schema: `page` (STRING), `duration_sec` (INT), `referrer` (STRING)
# MAGIC 3. Use `from_json` to parse the `payload` string column into a struct column named `parsed`
# MAGIC 4. Extract `parsed.page` as `page` and `parsed.duration_sec` as `duration_sec`
# MAGIC 5. Select final columns: `event_id`, `user_id`, `event_type`, `event_ts`, `page`, `duration_sec`
# MAGIC 6. Write as a Delta table to `db_code.complex_data_types.parsed_events_ex3` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Rows with NULL payload should produce NULL `page` and NULL `duration_sec`
# MAGIC - Malformed JSON (EVT-113) should also produce NULL fields (from_json returns NULL for bad input)
# MAGIC - Do not filter out any rows

# COMMAND ----------

# EXERCISE_KEY: complex_ex3
# TODO: Parse JSON string payload using from_json, extract fields, write to Delta
# Your code here

# COMMAND ----------

result_ex3 = spark.table(f"{CATALOG}.{SCHEMA}.parsed_events_ex3")

# Row count: all 13 events preserved
assert result_ex3.count() == 13, f"Expected 13 rows, got {result_ex3.count()}"

# Schema check
expected_cols = {"event_id", "user_id", "event_type", "event_ts", "page", "duration_sec"}
assert set(result_ex3.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex3.columns)}"

# Value check: EVT-101 should have page=/home, duration=45
evt101 = result_ex3.filter("event_id = 'EVT-101'").first()
assert evt101.page == "/home", f"EVT-101 page should be /home, got {evt101.page}"
assert evt101.duration_sec == 45, f"EVT-101 duration_sec should be 45, got {evt101.duration_sec}"

# NULL payload produces NULL fields
evt108 = result_ex3.filter("event_id = 'EVT-108'").first()
assert evt108.page is None, f"EVT-108 should have NULL page, got {evt108.page}"
assert evt108.duration_sec is None, f"EVT-108 should have NULL duration_sec, got {evt108.duration_sec}"

# Malformed JSON produces NULL fields
evt113 = result_ex3.filter("event_id = 'EVT-113'").first()
assert evt113.page is None, f"EVT-113 (malformed) should have NULL page, got {evt113.page}"
assert evt113.duration_sec is None, f"EVT-113 (malformed) should have NULL duration_sec, got {evt113.duration_sec}"

# Check another known value
evt106 = result_ex3.filter("event_id = 'EVT-106'").first()
assert evt106.page == "/register", f"EVT-106 page should be /register, got {evt106.page}"

print("Exercise 3 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Map operations - map_keys, map_values, element_at
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Work with a MAP column: extract all keys, all values, look up specific entries
# MAGIC by key, and count the number of attributes per product.
# MAGIC
# MAGIC **Source** (`products_map_ex4`): 8 products with a `MAP<STRING, STRING>` column `attributes`.
# MAGIC P-007 has an empty map. Each product has different attribute keys (color, weight_kg, warranty_years, etc.).
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.map_results_ex4`. Expected 8 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.products_map_ex4`
# MAGIC 2. Use `map_keys` to extract all attribute names as an array column named `attribute_names`
# MAGIC 3. Use `element_at` to extract the `color` value as a STRING column named `color` (NULL if key not present)
# MAGIC 4. Use `size(map_keys(...))` to count attributes as an INT column named `num_attributes`
# MAGIC 5. Select final columns: `product_id`, `name`, `attribute_names`, `color`, `num_attributes`
# MAGIC 6. Write as a Delta table to `db_code.complex_data_types.map_results_ex4` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - P-007 (empty map) should have 0 `num_attributes` and NULL `color`
# MAGIC - P-004 has no `color` key, so `color` should be NULL
# MAGIC - `element_at` on a missing key returns NULL (no error)

# COMMAND ----------

# EXERCISE_KEY: complex_ex4
# TODO: Extract map keys, look up specific key, count entries, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex4 = spark.table(f"{CATALOG}.{SCHEMA}.map_results_ex4")

# Row count: all 8 products
assert result_ex4.count() == 8, f"Expected 8 rows, got {result_ex4.count()}"

# Schema check
expected_cols = {"product_id", "name", "attribute_names", "color", "num_attributes"}
assert set(result_ex4.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex4.columns)}"

# P-001 has color=silver and 3 attributes
p001 = result_ex4.filter("product_id = 'P-001'").first()
assert p001.color == "silver", f"P-001 color should be silver, got {p001.color}"
assert p001.num_attributes == 3, f"P-001 should have 3 attributes, got {p001.num_attributes}"

# P-007 (empty map) should have 0 attributes and NULL color
p007 = result_ex4.filter("product_id = 'P-007'").first()
assert p007.num_attributes == 0, f"P-007 should have 0 attributes, got {p007.num_attributes}"
assert p007.color is None, f"P-007 color should be NULL, got {p007.color}"

# P-004 has no color key
p004 = result_ex4.filter("product_id = 'P-004'").first()
assert p004.color is None, f"P-004 color should be NULL (no color key), got {p004.color}"
assert p004.num_attributes == 3, f"P-004 should have 3 attributes, got {p004.num_attributes}"

# P-008 has 4 attributes
p008 = result_ex4.filter("product_id = 'P-008'").first()
assert p008.num_attributes == 4, f"P-008 should have 4 attributes, got {p008.num_attributes}"
assert p008.color == "black", f"P-008 color should be black, got {p008.color}"

print("Exercise 4 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: collect_list/collect_set - aggregate into arrays
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Group order line items by order_id and aggregate product_ids into arrays using
# MAGIC `collect_list` and `collect_set`, plus sum the quantities.
# MAGIC
# MAGIC **Source** (`order_items_ex5`): 15 line items across 7 orders. Some orders have the same
# MAGIC product_id appearing multiple times (e.g., ORD-002 has P-100 twice, ORD-007 has P-500 twice).
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.order_products_ex5`. Expected 7 rows (one per order).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.order_items_ex5`
# MAGIC 2. Group by `order_id`
# MAGIC 3. Use `collect_list` on `product_id` to create column `all_products` (keeps duplicates)
# MAGIC 4. Use `collect_set` on `product_id` to create column `unique_products` (removes duplicates)
# MAGIC 5. Add column `total_quantity` as the sum of `quantity` for each order
# MAGIC 6. Select final columns: `order_id`, `all_products`, `unique_products`, `total_quantity`
# MAGIC 7. Write as a Delta table to `db_code.complex_data_types.order_products_ex5` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - `all_products` must include duplicates (ORD-002 should list P-100 twice)
# MAGIC - `unique_products` must deduplicate (ORD-002 should list P-100 once)
# MAGIC - One output row per order_id

# COMMAND ----------

# EXERCISE_KEY: complex_ex5
# TODO: Group by order, collect_list and collect_set on product_id, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex5 = spark.table(f"{CATALOG}.{SCHEMA}.order_products_ex5")

# Row count: 7 unique orders
assert result_ex5.count() == 7, f"Expected 7 rows, got {result_ex5.count()}"

# Schema check
expected_cols = {"order_id", "all_products", "unique_products", "total_quantity"}
assert set(result_ex5.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex5.columns)}"

# ORD-002 has P-100 twice and P-300 once: all_products should have 3 items, unique_products 2
ord002 = result_ex5.filter("order_id = 'ORD-002'").first()
assert len(ord002.all_products) == 3, f"ORD-002 all_products should have 3 items, got {len(ord002.all_products)}"
assert len(ord002.unique_products) == 2, f"ORD-002 unique_products should have 2 items, got {len(ord002.unique_products)}"

# ORD-007 has P-500 appearing in 2 line items: all_products=2, unique_products=1
ord007 = result_ex5.filter("order_id = 'ORD-007'").first()
assert len(ord007.all_products) == 2, f"ORD-007 all_products should have 2 items, got {len(ord007.all_products)}"
assert len(ord007.unique_products) == 1, f"ORD-007 unique_products should have 1 item, got {len(ord007.unique_products)}"

# total_quantity check for ORD-005: 5+1+2+1 = 9
ord005 = result_ex5.filter("order_id = 'ORD-005'").first()
assert ord005.total_quantity == 9, f"ORD-005 total_quantity should be 9, got {ord005.total_quantity}"

# ORD-002 total_quantity: 1+3+1 = 5
assert ord002.total_quantity == 5, f"ORD-002 total_quantity should be 5, got {ord002.total_quantity}"

print("Exercise 5 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Higher-order functions - transform() and filter() on arrays
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Use higher-order functions `transform()` and `filter()` to modify and select array elements
# MAGIC WITHOUT exploding the arrays.
# MAGIC
# MAGIC **Source** (`sensor_readings_ex6`): 7 sensors with `temperatures` (ARRAY<INT>) and `labels`
# MAGIC (ARRAY<STRING>). S-006 has empty arrays. S-007 has NULL arrays.
# MAGIC Temperatures are in Fahrenheit.
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.transformed_arrays_ex6`. Expected 7 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.sensor_readings_ex6`
# MAGIC 2. Use `transform()` on `temperatures` to convert each value from Fahrenheit to Celsius:
# MAGIC    formula is `(temp - 32) * 5 / 9`, cast each result to INT. Name the column `temps_celsius`
# MAGIC 3. Use `filter()` on `temperatures` to keep only values above 80 (Fahrenheit).
# MAGIC    Name the column `high_temps`
# MAGIC 4. Add column `num_high_temps` as the `size()` of `high_temps`
# MAGIC 5. Select final columns: `sensor_id`, `reading_date`, `temps_celsius`, `high_temps`, `num_high_temps`
# MAGIC 6. Write as a Delta table to `db_code.complex_data_types.transformed_arrays_ex6` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - S-006 (empty arrays) should produce empty arrays for `temps_celsius` and `high_temps`, 0 for `num_high_temps`
# MAGIC - S-007 (NULL arrays) should produce NULL for all derived columns (temps_celsius, high_temps, num_high_temps)
# MAGIC - Do not use `explode` - the point is to use higher-order functions

# COMMAND ----------

# EXERCISE_KEY: complex_ex6
# TODO: Use transform() and filter() on array columns, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex6 = spark.table(f"{CATALOG}.{SCHEMA}.transformed_arrays_ex6")

# Row count: all 7 sensors
assert result_ex6.count() == 7, f"Expected 7 rows, got {result_ex6.count()}"

# Schema check
expected_cols = {"sensor_id", "reading_date", "temps_celsius", "high_temps", "num_high_temps"}
assert set(result_ex6.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex6.columns)}"

# S-002 has temps [85,90,88,92,87] - all above 80, so high_temps should have 5 elements
s002 = result_ex6.filter("sensor_id = 'S-002'").first()
assert s002.num_high_temps == 5, f"S-002 should have 5 high temps, got {s002.num_high_temps}"
# Celsius conversion: (85-32)*5/9=29, (90-32)*5/9=32, etc.
assert s002.temps_celsius[0] == 29, f"S-002 first celsius should be 29, got {s002.temps_celsius[0]}"
assert s002.temps_celsius[1] == 32, f"S-002 second celsius should be 32, got {s002.temps_celsius[1]}"

# S-003 has temps [32,28,35,30,27] - all below 80, so high_temps should be empty
s003 = result_ex6.filter("sensor_id = 'S-003'").first()
assert s003.num_high_temps == 0, f"S-003 should have 0 high temps, got {s003.num_high_temps}"
assert len(s003.high_temps) == 0, f"S-003 high_temps should be empty, got {s003.high_temps}"

# S-004 has [70,105,72,110,68] - 2 above 80: [105,110]
s004 = result_ex6.filter("sensor_id = 'S-004'").first()
assert s004.num_high_temps == 2, f"S-004 should have 2 high temps, got {s004.num_high_temps}"
assert sorted(s004.high_temps) == [105, 110], f"S-004 high temps should be [105,110], got {sorted(s004.high_temps)}"

# S-006 (empty arrays): 0 high temps
s006 = result_ex6.filter("sensor_id = 'S-006'").first()
assert s006.num_high_temps == 0, f"S-006 should have 0 high temps, got {s006.num_high_temps}"

# S-007 (NULL arrays): all derived columns should be NULL
s007 = result_ex6.filter("sensor_id = 'S-007'").first()
assert s007.num_high_temps is None, f"S-007 num_high_temps should be NULL for NULL array, got {s007.num_high_temps}"
assert s007.temps_celsius is None, f"S-007 temps_celsius should be NULL, got {s007.temps_celsius}"

print("Exercise 6 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 7: Flatten deeply nested JSON - multi-level structs with arrays of structs
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Flatten a deeply nested struct column containing arrays of structs. The source has
# MAGIC 3 nesting levels: response > body.user + body.orders[] > orders[].items[].
# MAGIC Flatten to one row per item, handling NULLs at every level.
# MAGIC
# MAGIC **Source** (`api_responses_ex7`): 6 API responses. Column `body` is:
# MAGIC `STRUCT<user: STRUCT<id, name, email>, orders: ARRAY<STRUCT<order_id, total, items: ARRAY<STRUCT<sku, qty>>>>>`
# MAGIC
# MAGIC Edge cases:
# MAGIC - R-004: `body.orders` is NULL (user exists but no orders)
# MAGIC - R-005: The only item in items array is NULL
# MAGIC - R-006: `body.user` is NULL (orders exist but no user info, 2 items)
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.flat_items_ex7`. Expected 11 rows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.api_responses_ex7`
# MAGIC 2. Extract `body.user.id` as `user_id`, `body.user.name` as `user_name`
# MAGIC 3. Explode `body.orders` to get one row per order (use `explode_outer` to keep rows where orders is NULL)
# MAGIC 4. Extract the exploded order's `order_id` and `total` as `order_total`
# MAGIC 5. Explode the order's `items` array to get one row per item (use `explode_outer` to keep rows where items is NULL)
# MAGIC 6. Extract item `sku` and `qty` from the exploded item struct
# MAGIC 7. Filter out rows where BOTH `sku` AND `qty` are NULL (artifact of NULL items in array, and NULL orders row)
# MAGIC 8. Select final columns: `response_id`, `user_id`, `user_name`, `order_id`, `order_total`, `sku`, `qty`
# MAGIC 9. Write as a Delta table to `db_code.complex_data_types.flat_items_ex7` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - R-004 (NULL orders) should produce zero rows in the output (dropped after filtering)
# MAGIC - R-005's NULL item should be dropped (both sku and qty are NULL)
# MAGIC - R-006 (NULL user) should still produce rows with NULL user_id and user_name
# MAGIC - The output should have exactly 11 data rows

# COMMAND ----------

# EXERCISE_KEY: complex_ex7
# TODO: Flatten nested structs and arrays, handle NULLs at each level, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex7 = spark.table(f"{CATALOG}.{SCHEMA}.flat_items_ex7")

# Row count: 11 items total after filtering NULLs
assert result_ex7.count() == 11, f"Expected 11 rows, got {result_ex7.count()}"

# Schema check
expected_cols = {"response_id", "user_id", "user_name", "order_id", "order_total", "sku", "qty"}
assert set(result_ex7.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex7.columns)}"

# R-001 should have 3 items: ORD-001 has 2 items + ORD-002 has 1 item
r001_count = result_ex7.filter("response_id = 'R-001'").count()
assert r001_count == 3, f"R-001 should have 3 rows, got {r001_count}"

# R-004 (NULL orders) should have 0 rows
r004_count = result_ex7.filter("response_id = 'R-004'").count()
assert r004_count == 0, f"R-004 should have 0 rows (NULL orders), got {r004_count}"

# R-006 (NULL user) should still have rows with NULL user_id
r006 = result_ex7.filter("response_id = 'R-006'")
assert r006.count() == 2, f"R-006 should have 2 rows, got {r006.count()}"
r006_row = r006.filter("sku = 'SKU-F6'").first()
assert r006_row.user_id is None, f"R-006 user_id should be NULL, got {r006_row.user_id}"
assert r006_row.sku == "SKU-F6", f"R-006 sku should be SKU-F6, got {r006_row.sku}"

# R-005 should have 0 rows (the only item is NULL, gets filtered)
r005_count = result_ex7.filter("response_id = 'R-005'").count()
assert r005_count == 0, f"R-005 should have 0 rows (NULL item filtered), got {r005_count}"

# Value check: R-002, ORD-003 should have SKU-C3 with qty=3
r002_c3 = result_ex7.filter("response_id = 'R-002' AND sku = 'SKU-C3'").first()
assert r002_c3 is not None, "R-002 should have SKU-C3"
assert r002_c3.qty == 3, f"R-002 SKU-C3 qty should be 3, got {r002_c3.qty}"
assert r002_c3.order_total == 220.0, f"R-002 order_total should be 220.0, got {r002_c3.order_total}"

# R-003 should have 3 items: ORD-004 has 1 + ORD-005 has 2
r003_count = result_ex7.filter("response_id = 'R-003'").count()
assert r003_count == 3, f"R-003 should have 3 rows, got {r003_count}"

print("Exercise 7 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 8: Build complex types from flat data - struct and array columns
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Build struct columns and array-of-structs columns from flat grouped data. This is the
# MAGIC reverse of flattening - restructure denormalized rows into nested complex types.
# MAGIC
# MAGIC **Source** (`flat_order_lines_ex8`): 9 flat rows across 5 orders. Each row has order info,
# MAGIC customer info, and one line item. Customer columns repeat across rows for the same order.
# MAGIC ORD-003 has NULL customer_email.
# MAGIC
# MAGIC **Target**: Write to `db_code.complex_data_types.structured_orders_ex8`. Expected 5 rows (one per order).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read from `db_code.complex_data_types.flat_order_lines_ex8`
# MAGIC 2. Group by `order_id`
# MAGIC 3. Build a struct column `customer` containing `name`, `email`, `tier` from the first row of each group
# MAGIC    (use `first()` since customer columns are the same across rows in the same order).
# MAGIC    Name the struct column `customer`
# MAGIC 4. Build an array column `items` using `collect_list` of structs, where each struct has
# MAGIC    `product_name`, `quantity`, `unit_price`. Name the column `items`
# MAGIC 5. Add column `item_count` as the count of line items per order
# MAGIC 6. Select final columns: `order_id`, `customer`, `items`, `item_count`
# MAGIC 7. Write as a Delta table to `db_code.complex_data_types.structured_orders_ex8` (overwrite mode)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - ORD-001 has 3 line items, so `items` array should have 3 elements
# MAGIC - ORD-005 has 1 line item, so `items` array should have 1 element
# MAGIC - `customer.email` for ORD-003 should be NULL
# MAGIC - One output row per order_id

# COMMAND ----------

# EXERCISE_KEY: complex_ex8
# TODO: Build struct and array columns from flat data, write to Delta
# Your code here

# COMMAND ----------

from pyspark.sql import functions as F

result_ex8 = spark.table(f"{CATALOG}.{SCHEMA}.structured_orders_ex8")

# Row count: 5 unique orders
assert result_ex8.count() == 5, f"Expected 5 rows, got {result_ex8.count()}"

# Schema check
expected_cols = {"order_id", "customer", "items", "item_count"}
assert set(result_ex8.columns) == expected_cols, f"Expected columns {expected_cols}, got {set(result_ex8.columns)}"

# ORD-001 has 3 line items
ord001 = result_ex8.filter("order_id = 'ORD-001'").first()
assert ord001.item_count == 3, f"ORD-001 item_count should be 3, got {ord001.item_count}"
assert len(ord001.items) == 3, f"ORD-001 items array should have 3 elements, got {len(ord001.items)}"

# Check customer struct for ORD-001
assert ord001.customer.name == "Alice Kim", f"ORD-001 customer.name should be Alice Kim, got {ord001.customer.name}"
assert ord001.customer.email == "alice@example.com", f"ORD-001 customer.email should be alice@example.com, got {ord001.customer.email}"
assert ord001.customer.tier == "gold", f"ORD-001 customer.tier should be gold, got {ord001.customer.tier}"

# ORD-003 has NULL customer email
ord003 = result_ex8.filter("order_id = 'ORD-003'").first()
assert ord003.customer.email is None, f"ORD-003 customer.email should be NULL, got {ord003.customer.email}"
assert ord003.customer.name == "Carol Diaz", f"ORD-003 customer.name should be Carol Diaz, got {ord003.customer.name}"
assert ord003.item_count == 1, f"ORD-003 item_count should be 1, got {ord003.item_count}"

# ORD-005 has 1 item
ord005 = result_ex8.filter("order_id = 'ORD-005'").first()
assert ord005.item_count == 1, f"ORD-005 item_count should be 1, got {ord005.item_count}"
assert len(ord005.items) == 1, f"ORD-005 items array should have 1 element, got {len(ord005.items)}"

# Check items struct content for ORD-002
ord002 = result_ex8.filter("order_id = 'ORD-002'").first()
assert ord002.item_count == 2, f"ORD-002 item_count should be 2, got {ord002.item_count}"
product_names = sorted([item.product_name for item in ord002.items])
assert product_names == ["Keyboard", "Monitor 27\""], f"ORD-002 products should be Keyboard and Monitor 27\", got {product_names}"

print("Exercise 8 passed!")
