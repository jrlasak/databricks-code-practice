# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Complex Data Types
# MAGIC Creates per-exercise source tables with complex type data (structs, arrays, maps, JSON strings,
# MAGIC nested structs with arrays of structs) in `db_code.complex_data_types`.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "complex_data_types"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Clean up any previous exercise output tables (idempotent)
for tbl in [
    "flat_customers_ex1",
    "exploded_tags_ex2",
    "parsed_events_ex3",
    "map_results_ex4",
    "order_products_ex5",
    "transformed_arrays_ex6",
    "flat_items_ex7",
    "structured_orders_ex8",
]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Access struct fields
# MAGIC Source: customers with a struct column for address. Includes NULL struct rows.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.customers_struct_ex1 (
  customer_id STRING,
  name STRING,
  address STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>,
  tier STRING
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.customers_struct_ex1 VALUES
  ('C-001', 'Alice Kim', named_struct('street', '123 Main St', 'city', 'Seattle', 'state', 'WA', 'zip', '98101'), 'gold'),
  ('C-002', 'Bob Patel', named_struct('street', '456 Oak Ave', 'city', 'Portland', 'state', 'OR', 'zip', '97201'), 'silver'),
  ('C-003', 'Carol Diaz', named_struct('street', '789 Pine Rd', 'city', 'San Francisco', 'state', 'CA', 'zip', '94102'), 'gold'),
  ('C-004', 'Dan Lee', named_struct('street', '321 Elm Dr', 'city', 'Denver', 'state', 'CO', 'zip', '80201'), 'bronze'),
  ('C-005', 'Eve Torres', named_struct('street', '654 Maple Ln', 'city', 'Seattle', 'state', 'WA', 'zip', '98102'), 'silver'),
  ('C-006', 'Frank Wu', named_struct('street', '987 Cedar Ct', 'city', 'Austin', 'state', 'TX', 'zip', '73301'), 'gold'),
  ('C-007', 'Grace Ng', named_struct('street', '147 Birch Way', 'city', 'Portland', 'state', 'OR', 'zip', '97202'), 'bronze'),
  ('C-008', 'Hank Miller', named_struct('street', '258 Spruce Pl', 'city', 'Denver', 'state', 'CO', 'zip', '80202'), 'silver'),
  ('C-009', 'Iris Chen', named_struct('street', CAST(NULL AS STRING), 'city', 'Seattle', 'state', 'WA', 'zip', '98103'), 'gold'),
  ('C-010', 'Jay Brown', named_struct('street', '369 Walnut Bl', 'city', CAST(NULL AS STRING), 'state', 'CA', 'zip', '90001'), 'bronze'),
  ('C-011', 'Kim Patel', CAST(NULL AS STRUCT<street: STRING, city: STRING, state: STRING, zip: STRING>), 'silver')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Explode arrays
# MAGIC Source: orders with an array of tags. Includes empty arrays and NULLs.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.orders_tags_ex2 (
  order_id STRING,
  customer_id STRING,
  amount DOUBLE,
  tags ARRAY<STRING>
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.orders_tags_ex2 VALUES
  ('ORD-001', 'C-001', CAST(150.00 AS DOUBLE), array('rush', 'gift-wrap', 'premium')),
  ('ORD-002', 'C-002', CAST(45.50 AS DOUBLE), array('standard')),
  ('ORD-003', 'C-003', CAST(220.00 AS DOUBLE), array('rush', 'fragile')),
  ('ORD-004', 'C-001', CAST(89.99 AS DOUBLE), array('gift-wrap')),
  ('ORD-005', 'C-004', CAST(310.00 AS DOUBLE), array('rush', 'premium', 'insured', 'fragile')),
  ('ORD-006', 'C-005', CAST(12.00 AS DOUBLE), array('standard', 'economy')),
  ('ORD-007', 'C-003', CAST(0.00 AS DOUBLE), CAST(array() AS ARRAY<STRING>)),
  ('ORD-008', 'C-006', CAST(175.25 AS DOUBLE), array('rush', 'gift-wrap', 'premium')),
  ('ORD-009', 'C-007', CAST(65.00 AS DOUBLE), CAST(NULL AS ARRAY<STRING>))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: from_json parsing
# MAGIC Source: events with JSON string payloads to parse into structs. Includes NULL and malformed payloads.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.events_json_ex3 (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  event_ts TIMESTAMP,
  payload STRING
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.events_json_ex3 VALUES
  ('EVT-101', 'U-001', 'page_view', '2024-01-15 10:00:00', '{{"page": "/home", "duration_sec": 45, "referrer": "google"}}'),
  ('EVT-102', 'U-002', 'page_view', '2024-01-15 10:05:00', '{{"page": "/products", "duration_sec": 120, "referrer": "direct"}}'),
  ('EVT-103', 'U-001', 'click', '2024-01-15 10:10:00', '{{"page": "/products", "duration_sec": 5, "referrer": null}}'),
  ('EVT-104', 'U-003', 'purchase', '2024-01-15 10:15:00', '{{"page": "/checkout", "duration_sec": 200, "referrer": "email"}}'),
  ('EVT-105', 'U-002', 'page_view', '2024-01-15 10:20:00', '{{"page": "/about", "duration_sec": 30, "referrer": "google"}}'),
  ('EVT-106', 'U-004', 'signup', '2024-01-15 10:25:00', '{{"page": "/register", "duration_sec": 90, "referrer": "linkedin"}}'),
  ('EVT-107', 'U-001', 'click', '2024-01-15 10:30:00', '{{"page": "/home", "duration_sec": 10, "referrer": "direct"}}'),
  ('EVT-108', 'U-005', 'page_view', '2024-01-15 10:35:00', NULL),
  ('EVT-109', 'U-003', 'page_view', '2024-01-15 10:40:00', '{{"page": "/pricing", "duration_sec": 60, "referrer": "google"}}'),
  ('EVT-110', 'U-004', 'click', '2024-01-15 10:45:00', '{{"page": "/docs", "duration_sec": 15, "referrer": null}}'),
  ('EVT-111', 'U-002', 'purchase', '2024-01-15 10:50:00', '{{"page": "/checkout", "duration_sec": 180, "referrer": "email"}}'),
  ('EVT-112', 'U-006', 'page_view', '2024-01-15 10:55:00', NULL),
  ('EVT-113', 'U-007', 'click', '2024-01-15 11:00:00', 'not-valid-json')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Map operations
# MAGIC Source: products with a MAP<STRING, STRING> column for attributes. Includes empty maps.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.products_map_ex4 (
  product_id STRING,
  name STRING,
  attributes MAP<STRING, STRING>
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.products_map_ex4 VALUES
  ('P-001', 'Laptop Pro', map('color', 'silver', 'weight_kg', '1.4', 'warranty_years', '2')),
  ('P-002', 'Wireless Mouse', map('color', 'black', 'weight_kg', '0.1', 'wireless', 'true')),
  ('P-003', 'USB-C Hub', map('ports', '7', 'color', 'gray', 'weight_kg', '0.2')),
  ('P-004', 'Monitor 27"', map('resolution', '4K', 'panel', 'IPS', 'weight_kg', '5.5')),
  ('P-005', 'Keyboard', map('layout', 'US', 'wireless', 'true', 'color', 'white')),
  ('P-006', 'Webcam HD', map('resolution', '1080p', 'color', 'black')),
  ('P-007', 'Cable Pack', CAST(map() AS MAP<STRING, STRING>)),
  ('P-008', 'Dock Station', map('ports', '12', 'color', 'black', 'weight_kg', '0.8', 'warranty_years', '3'))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: collect_list/collect_set aggregation
# MAGIC Source: order line items to aggregate product_ids per order. Includes duplicate products and edge cases.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.order_items_ex5 (
  order_id STRING,
  product_id STRING,
  quantity INT,
  unit_price DOUBLE
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.order_items_ex5 VALUES
  ('ORD-001', 'P-100', 2, CAST(25.00 AS DOUBLE)),
  ('ORD-001', 'P-200', 1, CAST(100.00 AS DOUBLE)),
  ('ORD-002', 'P-100', 1, CAST(25.00 AS DOUBLE)),
  ('ORD-002', 'P-100', 3, CAST(25.00 AS DOUBLE)),
  ('ORD-002', 'P-300', 1, CAST(20.50 AS DOUBLE)),
  ('ORD-003', 'P-200', 2, CAST(100.00 AS DOUBLE)),
  ('ORD-003', 'P-400', 1, CAST(20.00 AS DOUBLE)),
  ('ORD-004', 'P-100', 1, CAST(25.00 AS DOUBLE)),
  ('ORD-005', 'P-500', 5, CAST(10.00 AS DOUBLE)),
  ('ORD-005', 'P-200', 1, CAST(100.00 AS DOUBLE)),
  ('ORD-005', 'P-300', 2, CAST(20.50 AS DOUBLE)),
  ('ORD-005', 'P-400', 1, CAST(20.00 AS DOUBLE)),
  ('ORD-006', 'P-100', 1, CAST(25.00 AS DOUBLE)),
  ('ORD-007', 'P-500', 2, CAST(10.00 AS DOUBLE)),
  ('ORD-007', 'P-500', 1, CAST(10.00 AS DOUBLE))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Higher-order functions (transform, filter)
# MAGIC Source: table with array<int> column for applying higher-order functions.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.sensor_readings_ex6 (
  sensor_id STRING,
  reading_date DATE,
  temperatures ARRAY<INT>,
  labels ARRAY<STRING>
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.sensor_readings_ex6 VALUES
  ('S-001', '2024-03-01', array(68, 72, 75, 71, 69), array('normal', 'normal', 'warm', 'normal', 'normal')),
  ('S-002', '2024-03-01', array(85, 90, 88, 92, 87), array('warm', 'hot', 'warm', 'hot', 'warm')),
  ('S-003', '2024-03-01', array(32, 28, 35, 30, 27), array('cold', 'cold', 'cold', 'cold', 'cold')),
  ('S-004', '2024-03-01', array(70, 105, 72, 110, 68), array('normal', 'critical', 'normal', 'critical', 'normal')),
  ('S-005', '2024-03-01', array(50), array('cool')),
  ('S-006', '2024-03-01', CAST(array() AS ARRAY<INT>), CAST(array() AS ARRAY<STRING>)),
  ('S-007', '2024-03-01', CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<STRING>))
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Flatten deeply nested JSON
# MAGIC Source: API responses with structs containing arrays of structs. NULLs at each nesting level.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.api_responses_ex7 (
  response_id STRING,
  status_code INT,
  body STRUCT<
    user: STRUCT<id: STRING, name: STRING, email: STRING>,
    orders: ARRAY<STRUCT<order_id: STRING, total: DOUBLE, items: ARRAY<STRUCT<sku: STRING, qty: INT>>>>
  >
)
""")

# Use PySpark to build ex7 data - SQL VALUES struggles with deeply nested struct/array types
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

_item_type = StructType([StructField("sku", StringType()), StructField("qty", IntegerType())])
_order_type = StructType([
    StructField("order_id", StringType()),
    StructField("total", DoubleType()),
    StructField("items", ArrayType(_item_type)),
])
_user_type = StructType([StructField("id", StringType()), StructField("name", StringType()), StructField("email", StringType())])
_body_type = StructType([StructField("user", _user_type), StructField("orders", ArrayType(_order_type))])
_schema_ex7 = StructType([
    StructField("response_id", StringType()),
    StructField("status_code", IntegerType()),
    StructField("body", _body_type),
])

_ex7_data = [
    ("R-001", 200, (("U-001", "Alice Kim", "alice@example.com"), [("ORD-001", 150.0, [("SKU-A1", 2), ("SKU-B2", 1)]), ("ORD-002", 45.5, [("SKU-A1", 1)])])),
    ("R-002", 200, (("U-002", "Bob Patel", "bob@example.com"), [("ORD-003", 220.0, [("SKU-C3", 3), ("SKU-D4", 2), ("SKU-A1", 1)])])),
    ("R-003", 200, (("U-003", "Carol Diaz", None), [("ORD-004", 89.99, [("SKU-B2", 1)]), ("ORD-005", 310.0, [("SKU-E5", 5), ("SKU-C3", 2)])])),
    ("R-004", 200, (("U-004", "Dan Lee", "dan@example.com"), None)),
    ("R-005", 200, (("U-005", "Eve Torres", "eve@example.com"), [("ORD-006", 12.0, [None])])),
    ("R-006", 200, (None, [("ORD-007", 75.0, [("SKU-F6", 1), ("SKU-A1", 2)])])),
]
spark.createDataFrame(_ex7_data, _schema_ex7).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.api_responses_ex7")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Build complex types from flat data
# MAGIC Source: flat order + line item data that users will restructure into structs and arrays.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.flat_order_lines_ex8 (
  order_id STRING,
  customer_name STRING,
  customer_email STRING,
  customer_tier STRING,
  product_name STRING,
  quantity INT,
  unit_price DOUBLE
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.flat_order_lines_ex8 VALUES
  ('ORD-001', 'Alice Kim', 'alice@example.com', 'gold', 'Laptop Pro', 1, CAST(999.99 AS DOUBLE)),
  ('ORD-001', 'Alice Kim', 'alice@example.com', 'gold', 'Wireless Mouse', 2, CAST(29.99 AS DOUBLE)),
  ('ORD-001', 'Alice Kim', 'alice@example.com', 'gold', 'USB-C Hub', 1, CAST(49.99 AS DOUBLE)),
  ('ORD-002', 'Bob Patel', 'bob@example.com', 'silver', 'Monitor 27"', 1, CAST(449.00 AS DOUBLE)),
  ('ORD-002', 'Bob Patel', 'bob@example.com', 'silver', 'Keyboard', 1, CAST(79.99 AS DOUBLE)),
  ('ORD-003', 'Carol Diaz', NULL, 'bronze', 'Webcam HD', 3, CAST(59.99 AS DOUBLE)),
  ('ORD-004', 'Dan Lee', 'dan@example.com', 'gold', 'Dock Station', 1, CAST(199.99 AS DOUBLE)),
  ('ORD-004', 'Dan Lee', 'dan@example.com', 'gold', 'Laptop Pro', 1, CAST(999.99 AS DOUBLE)),
  ('ORD-005', 'Eve Torres', 'eve@example.com', 'silver', 'Cable Pack', 5, CAST(9.99 AS DOUBLE))
""")

# COMMAND ----------

print(f"Setup complete for Complex Data Types")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Exercise 1: customers_struct_ex1 ({spark.table(f'{CATALOG}.{SCHEMA}.customers_struct_ex1').count()} rows)")
print(f"  Exercise 2: orders_tags_ex2 ({spark.table(f'{CATALOG}.{SCHEMA}.orders_tags_ex2').count()} rows)")
print(f"  Exercise 3: events_json_ex3 ({spark.table(f'{CATALOG}.{SCHEMA}.events_json_ex3').count()} rows)")
print(f"  Exercise 4: products_map_ex4 ({spark.table(f'{CATALOG}.{SCHEMA}.products_map_ex4').count()} rows)")
print(f"  Exercise 5: order_items_ex5 ({spark.table(f'{CATALOG}.{SCHEMA}.order_items_ex5').count()} rows)")
print(f"  Exercise 6: sensor_readings_ex6 ({spark.table(f'{CATALOG}.{SCHEMA}.sensor_readings_ex6').count()} rows)")
print(f"  Exercise 7: api_responses_ex7 ({spark.table(f'{CATALOG}.{SCHEMA}.api_responses_ex7').count()} rows)")
print(f"  Exercise 8: flat_order_lines_ex8 ({spark.table(f'{CATALOG}.{SCHEMA}.flat_order_lines_ex8').count()} rows)")
