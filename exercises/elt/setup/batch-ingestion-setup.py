# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Batch Data Ingestion & File Formats
# MAGIC Creates per-exercise source files in Volumes and target table schemas.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "batch_ingestion"
BASE_SCHEMA = "elt"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.source_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Pipe-delimited CSV files with header

# COMMAND ----------

import json

ex1_csv_batch1 = """product_id|product_name|category|price|in_stock
P-001|Laptop Pro 15|Electronics|1299.99|true
P-002|Wireless Mouse|Electronics|29.99|true
P-003|Standing Desk|Furniture|549.00|false
P-004|USB-C Hub|Electronics|49.99|true
P-005|Monitor Arm|Furniture|189.50|true
P-006|Mechanical Keyboard|Electronics|149.99|true
P-007|Desk Lamp|Furniture|79.99|false
P-008|Webcam HD|Electronics|89.99|true
"""

dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex1_csv")
dbutils.fs.put(f"{VOLUME_PATH}/ex1_csv/products_batch1.csv", ex1_csv_batch1, overwrite=True)

ex1_csv_batch2 = """product_id|product_name|category|price|in_stock
P-009|Noise Cancelling Headphones|Electronics|299.99|true
P-010|Ergonomic Chair|Furniture|699.00|true
P-011|Portable Charger|Electronics|39.99|true
P-012|Bookshelf|Furniture|219.00|false
"""

dbutils.fs.put(f"{VOLUME_PATH}/ex1_csv/products_batch2.csv", ex1_csv_batch2, overwrite=True)

print("Exercise 1: 2 pipe-delimited CSV files (12 products total) written to ex1_csv/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: JSON files with flat structure (explicit schema read)

# COMMAND ----------

ex2_json_records = [
    {"order_id": "ORD-001", "customer_name": "Alice Chen", "product": "Laptop", "amount": 1299.99, "order_date": "2026-01-10"},
    {"order_id": "ORD-002", "customer_name": "Bob Kumar", "product": "Mouse", "amount": 29.99, "order_date": "2026-01-11"},
    {"order_id": "ORD-003", "customer_name": "Carol Diaz", "product": "Monitor", "amount": 549.00, "order_date": "2026-01-12"},
    {"order_id": "ORD-004", "customer_name": "Dan Park", "product": "Keyboard", "amount": 149.99, "order_date": "2026-01-13"},
    {"order_id": "ORD-005", "customer_name": "Eva Nowak", "product": "Webcam", "amount": 89.99, "order_date": "2026-01-14"},
    {"order_id": "ORD-006", "customer_name": "Frank Lee", "product": "Headphones", "amount": 299.99, "order_date": "2026-01-15"},
    {"order_id": "ORD-007", "customer_name": "Grace Kim", "product": "Charger", "amount": 39.99, "order_date": "2026-01-16"},
    {"order_id": "ORD-008", "customer_name": "Hiro Tanaka", "product": "Desk", "amount": 699.00, "order_date": "2026-01-17"},
]

ex2_ndjson = "\n".join([json.dumps(r) for r in ex2_json_records])
dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex2_json")
dbutils.fs.put(f"{VOLUME_PATH}/ex2_json/orders_batch1.json", ex2_ndjson, overwrite=True)

print("Exercise 2: 1 JSON file (8 orders, flat structure) written to ex2_json/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: CSV files for COPY INTO (idempotent incremental loading)

# COMMAND ----------

ex3_batch1 = """transaction_id,customer_id,amount,currency,tx_date
TXN-001,C-101,250.00,USD,2026-01-15
TXN-002,C-102,89.50,USD,2026-01-15
TXN-003,C-103,1200.00,EUR,2026-01-16
TXN-004,C-101,45.00,USD,2026-01-16
TXN-005,C-104,675.00,USD,2026-01-17
"""

ex3_batch2 = """transaction_id,customer_id,amount,currency,tx_date
TXN-006,C-105,320.00,USD,2026-01-18
TXN-007,C-102,150.75,USD,2026-01-18
TXN-008,C-106,999.99,EUR,2026-01-19
"""

dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex3_csv")
dbutils.fs.put(f"{VOLUME_PATH}/ex3_csv/transactions_20260115.csv", ex3_batch1, overwrite=True)
dbutils.fs.put(f"{VOLUME_PATH}/ex3_csv/transactions_20260118.csv", ex3_batch2, overwrite=True)

# Create the empty target Delta table for COPY INTO
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ex3_transactions (
  transaction_id STRING,
  customer_id STRING,
  amount DOUBLE,
  currency STRING,
  tx_date DATE
)
""")

print("Exercise 3: 2 CSV files (8 transactions) + empty target table created for COPY INTO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: JSON files with nested structs

# COMMAND ----------

ex4_json_records = [
    {"event_id": "EVT-001", "event_type": "purchase", "timestamp": "2026-02-01T10:30:00Z", "user": {"user_id": "U-201", "name": "Alice Chen", "email": "alice@example.com", "location": {"city": "Seattle", "state": "WA"}}, "amount": 149.97},
    {"event_id": "EVT-002", "event_type": "signup", "timestamp": "2026-02-01T11:00:00Z", "user": {"user_id": "U-202", "name": "Bob Kumar", "email": "bob@example.com", "location": {"city": "Austin", "state": "TX"}}, "amount": 0.0},
    {"event_id": "EVT-003", "event_type": "purchase", "timestamp": "2026-02-01T12:15:00Z", "user": {"user_id": "U-203", "name": "Carol Diaz", "email": "carol@example.com", "location": {"city": "Denver", "state": "CO"}}, "amount": 79.99},
    {"event_id": "EVT-004", "event_type": "refund", "timestamp": "2026-02-01T14:00:00Z", "user": {"user_id": "U-201", "name": "Alice Chen", "email": "alice@example.com", "location": {"city": "Seattle", "state": "WA"}}, "amount": 49.99},
    {"event_id": "EVT-005", "event_type": "purchase", "timestamp": "2026-02-02T09:00:00Z", "user": {"user_id": "U-204", "name": "Dan Park", "email": "dan@example.com", "location": {"city": "Portland", "state": "OR"}}, "amount": 324.50},
    {"event_id": "EVT-006", "event_type": "signup", "timestamp": "2026-02-02T10:30:00Z", "user": {"user_id": "U-205", "name": "Eva Nowak", "email": "eva@example.com", "location": {"city": "Chicago", "state": "IL"}}, "amount": 0.0},
]

ex4_ndjson = "\n".join([json.dumps(r) for r in ex4_json_records])
dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex4_json")
dbutils.fs.put(f"{VOLUME_PATH}/ex4_json/events_batch1.json", ex4_ndjson, overwrite=True)

print("Exercise 4: 1 JSON file (6 events with nested user/location structs) written to ex4_json/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: CSV with intentionally malformed rows

# COMMAND ----------

ex5_content = """sensor_id,reading,unit,recorded_at
SENS-001,23.5,celsius,2026-03-01T08:00:00
SENS-002,NOT_A_NUMBER,celsius,2026-03-01T08:01:00
SENS-003,45.2,fahrenheit,2026-03-01T08:02:00
SENS-004,18.9,celsius,INVALID_TIMESTAMP
SENS-005,31.0,celsius,2026-03-01T08:04:00
SENS-006,,celsius,2026-03-01T08:05:00
SENS-007,22.1,celsius,2026-03-01T08:06:00
SENS-008,99.9,fahrenheit,2026-03-01T08:07:00
SENS-009,BROKEN,fahrenheit,ALSO_BROKEN
SENS-010,27.3,celsius,2026-03-01T08:09:00
"""

dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex5_csv")
dbutils.fs.put(f"{VOLUME_PATH}/ex5_csv/sensor_readings.csv", ex5_content, overwrite=True)

print("Exercise 5: 1 CSV file (10 rows, 3 malformed: bad numeric, bad timestamp, both bad) written to ex5_csv/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Multi-format files (CSV + JSON + Parquet) for schema unification

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import datetime

# CSV source: employee records from system A
ex6_csv = """employee_id,full_name,department,salary,hire_date
EMP-001,Sarah Johnson,Engineering,125000,2023-06-15
EMP-002,Mike Torres,Marketing,95000,2024-01-10
EMP-003,Lisa Wang,Engineering,140000,2022-03-01
"""

dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex6_csv")
dbutils.fs.put(f"{VOLUME_PATH}/ex6_csv/employees_system_a.csv", ex6_csv, overwrite=True)

# JSON source: employee records from system B (different field names)
ex6_json_records = [
    {"emp_id": "EMP-004", "name": "James Lee", "dept": "Sales", "annual_salary": 105000, "start_date": "2023-09-20"},
    {"emp_id": "EMP-005", "name": "Anna Petrov", "dept": "Engineering", "annual_salary": 135000, "start_date": "2024-04-01"},
    {"emp_id": "EMP-006", "name": "Carlos Ruiz", "dept": "Marketing", "annual_salary": 88000, "start_date": "2023-11-15"},
]

ex6_ndjson = "\n".join([json.dumps(r) for r in ex6_json_records])
dbutils.fs.mkdirs(f"{VOLUME_PATH}/ex6_json")
dbutils.fs.put(f"{VOLUME_PATH}/ex6_json/employees_system_b.json", ex6_ndjson, overwrite=True)

# Parquet source: employee records from system C (yet another naming convention)
ex6_parquet_data = [
    ("EMP-007", "Priya Sharma", "Engineering", 150000, datetime.date(2021, 8, 10)),
    ("EMP-008", "Tom Baker", "Sales", 92000, datetime.date(2024, 2, 28)),
    ("EMP-009", "Nina Kowalski", "Engineering", 130000, datetime.date(2023, 1, 5)),
]

ex6_parquet_schema = StructType([
    StructField("id", StringType(), False),
    StructField("employee_name", StringType(), False),
    StructField("department_name", StringType(), False),
    StructField("compensation", IntegerType(), False),
    StructField("date_hired", DateType(), False),
])

ex6_df = spark.createDataFrame(ex6_parquet_data, ex6_parquet_schema)
ex6_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}/ex6_parquet")

print("Exercise 6: CSV (3 employees) + JSON (3 employees) + Parquet (3 employees) = 9 total, all different column names")

# COMMAND ----------

# Clean up any leftover exercise output tables for idempotency
for i in range(1, 7):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_output")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex3_transactions")

# Recreate COPY INTO target table (must exist before COPY INTO can run)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ex3_transactions (
  transaction_id STRING,
  customer_id STRING,
  amount DOUBLE,
  currency STRING,
  tx_date DATE
)
""")

print(f"\nSetup complete for Batch Data Ingestion & File Formats")
print(f"  Catalog: {CATALOG}")
print(f"  Schema:  {SCHEMA}")
print(f"  Volume:  {VOLUME_PATH}")
print(f"  Exercise source files: ex1_csv/, ex2_json/, ex3_csv/, ex4_json/, ex5_csv/, ex6_csv/, ex6_json/, ex6_parquet/")
