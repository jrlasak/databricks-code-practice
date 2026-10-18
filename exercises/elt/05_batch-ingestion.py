# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Data Ingestion & File Formats
# MAGIC **Topic**: ELT | **Exercises**: 6 | **Total Time**: ~60 min
# MAGIC
# MAGIC Practice batch ingestion patterns every Databricks data engineer uses: reading CSV and JSON from Volumes
# MAGIC with explicit options and schemas, incremental loading with COPY INTO, flattening nested structs,
# MAGIC handling malformed records with permissive mode, and unifying schemas across multiple file formats.
# MAGIC
# MAGIC **Prerequisites**: none. The next two cells run `00_Setup` and this topic's setup for you.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/batch-ingestion-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Code**: TODO cells are Python. The variables `CATALOG`, `SCHEMA` and `VOLUME_PATH` are already defined; run SQL such as `COPY INTO` with `spark.sql(...)`. Every exercise except Exercise 3 writes its result as a Delta table in overwrite mode.
# MAGIC
# MAGIC **Locations**:
# MAGIC - Source files: `/Volumes/db_code/batch_ingestion/source_files/`
# MAGIC - Output tables: `db_code.batch_ingestion.ex1_output` to `ex6_output` (Exercise 3 loads `db_code.batch_ingestion.ex3_transactions` instead)

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/batch-ingestion-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the setup created
# MAGIC
# MAGIC | Exercise | Source Location | Format | Records | Notes |
# MAGIC |----------|---------------|--------|---------|-------|
# MAGIC | 1 | `ex1_csv/` | CSV (pipe-delimited) | 12 products across 2 files | Header row, `\|` delimiter |
# MAGIC | 2 | `ex2_json/` | Newline-delimited JSON | 8 orders in 1 file | Flat structure, explicit schema required |
# MAGIC | 3 | `ex3_csv/` | CSV (comma-delimited) | 8 transactions across 2 files | Target table `ex3_transactions` already exists (empty) |
# MAGIC | 4 | `ex4_json/` | Newline-delimited JSON | 6 events in 1 file | Nested `user` struct (`user_id`, `name`, `email`) holding a nested `location` struct (`city`, `state`) |
# MAGIC | 5 | `ex5_csv/` | CSV | 10 sensor readings in 1 file | 3 malformed rows (bad numeric, bad timestamp, both) |
# MAGIC | 6 | `ex6_csv/`, `ex6_json/`, `ex6_parquet/` | CSV + JSON + Parquet | 9 employees (3 per format) | Different column names per source system |
# MAGIC
# MAGIC Each exercise reads its own source folder and writes its own table. Exercises are independent: skip to any one.

# COMMAND ----------

# Shared variables (same as setup notebook)
CATALOG = "db_code"
SCHEMA = "batch_ingestion"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Read CSV with Header, Delimiter, and Schema Options
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Read pipe-delimited CSV files from a Volume using `spark.read` with the correct format options,
# MAGIC then write the result as a Delta table.
# MAGIC
# MAGIC **Source**: CSV files at `/Volumes/db_code/batch_ingestion/source_files/ex1_csv/`
# MAGIC - 2 files (`products_batch1.csv`, `products_batch2.csv`), 12 total product rows
# MAGIC - Delimiter is `|` (pipe), not comma
# MAGIC - Files have a header row
# MAGIC - Columns: `product_id` (STRING), `product_name` (STRING), `category` (STRING), `price` (DOUBLE), `in_stock` (BOOLEAN)
# MAGIC
# MAGIC **Target**: Write to `db_code.batch_ingestion.ex1_output`. Expected 12 rows: `P-001` has `price` 1299.99, and `P-012` (from `products_batch2.csv`) is present.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read all CSV files from the `ex1_csv/` directory
# MAGIC 2. Set the correct delimiter option (pipe character)
# MAGIC 3. Enable header detection so column names come from the first row
# MAGIC 4. Infer the schema so `price` is read as a numeric type (not string)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Both files have the same header row and column order
# MAGIC - `product_id` is unique across the 2 files (P-001 to P-012)

# COMMAND ----------

# EXERCISE_KEY: batch_ex1
# TODO: Read pipe-delimited CSV files from the Volume and write to Delta
# Your code here

# COMMAND ----------

result_ex1 = spark.table(f"{CATALOG}.{SCHEMA}.ex1_output")

# Check 1: Row count
assert result_ex1.count() == 12, f"Expected 12 rows, got {result_ex1.count()}"

# Check 2: Correct columns exist
expected_cols = {"product_id", "product_name", "category", "price", "in_stock"}
actual_cols = set(result_ex1.columns)
assert expected_cols.issubset(actual_cols), f"Missing columns: {expected_cols - actual_cols}"

# Check 3: price is numeric, not string
from pyspark.sql.types import StringType
assert not isinstance(result_ex1.schema["price"].dataType, StringType), "price column should be numeric, not string"

# Check 4: Specific value check
laptop = result_ex1.filter("product_id = 'P-001'").collect()[0]
assert abs(laptop.price - 1299.99) < 0.01, f"Expected P-001 price 1299.99, got {laptop.price}"

# Check 5: Both files loaded
p012 = result_ex1.filter("product_id = 'P-012'").count()
assert p012 == 1, f"Expected P-012 (from products_batch2.csv) once, got {p012} rows"

print("Exercise 1 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Read JSON with Explicit Schema
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Read a JSON file using an explicitly defined `StructType` schema (not schema inference),
# MAGIC then write the result as a Delta table.
# MAGIC
# MAGIC **Source**: JSON file at `/Volumes/db_code/batch_ingestion/source_files/ex2_json/`
# MAGIC - 1 file, 8 order records (newline-delimited JSON)
# MAGIC - Fields: `order_id` (STRING), `customer_name` (STRING), `product` (STRING), `amount` (DOUBLE), `order_date` (STRING)
# MAGIC
# MAGIC **Target**: Write to `db_code.batch_ingestion.ex2_output`. Expected 8 rows: `ORD-001` is `Alice Chen` with `amount` 1299.99, and `ORD-008` has `amount` 699.00.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Define a `StructType` schema with the 5 fields listed above (use StringType, DoubleType)
# MAGIC 2. Read the JSON file using `spark.read.schema(your_schema).json(path)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Every record has all 5 fields, and none is null

# COMMAND ----------

# EXERCISE_KEY: batch_ex2
# TODO: Define a StructType schema and read JSON with it, then write to Delta
# Your code here

# COMMAND ----------

result_ex2 = spark.table(f"{CATALOG}.{SCHEMA}.ex2_output")

# Check 1: Row count
assert result_ex2.count() == 8, f"Expected 8 rows, got {result_ex2.count()}"

# Check 2: Correct columns
expected_cols_ex2 = {"order_id", "customer_name", "product", "amount", "order_date"}
actual_cols_ex2 = set(result_ex2.columns)
assert expected_cols_ex2 == actual_cols_ex2, f"Expected columns {expected_cols_ex2}, got {actual_cols_ex2}"

# Check 3: amount is DOUBLE (not string)
from pyspark.sql.types import DoubleType
assert isinstance(result_ex2.schema["amount"].dataType, DoubleType), f"amount should be DoubleType, got {result_ex2.schema['amount'].dataType}"

# Check 4: Specific value check
ord1 = result_ex2.filter("order_id = 'ORD-001'").collect()[0]
assert abs(ord1.amount - 1299.99) < 0.01, f"Expected ORD-001 amount 1299.99, got {ord1.amount}"
assert ord1.customer_name == "Alice Chen", f"Expected Alice Chen, got {ord1.customer_name}"

# Check 5: Last record present
ord8 = result_ex2.filter("order_id = 'ORD-008'").collect()
assert len(ord8) == 1, f"Expected ORD-008 to be present, got {len(ord8)} rows"
assert abs(ord8[0].amount - 699.00) < 0.01, f"Expected ORD-008 amount 699.00, got {ord8[0].amount}"

print("Exercise 2 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: COPY INTO for Idempotent Incremental Loading
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Use `COPY INTO` to incrementally load CSV files from a Volume into an existing Delta table.
# MAGIC COPY INTO is idempotent: it tracks which files have already been loaded and skips them on re-run.
# MAGIC
# MAGIC **Source**: CSV files at `/Volumes/db_code/batch_ingestion/source_files/ex3_csv/`
# MAGIC - 2 files: `transactions_20260115.csv` (5 rows) and `transactions_20260118.csv` (3 rows)
# MAGIC - Standard comma delimiter with header row
# MAGIC - Columns (with the target table's types): `transaction_id` (STRING), `customer_id` (STRING), `amount` (DOUBLE), `currency` (STRING), `tx_date` (DATE)
# MAGIC
# MAGIC **Target**: The table `db_code.batch_ingestion.ex3_transactions` already exists (empty).
# MAGIC Expected 8 rows after loading: `TXN-003` has `amount` 1200.00 and `currency` `EUR`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `COPY INTO` (not spark.read) to load all CSV files from the `ex3_csv/` directory
# MAGIC 2. Specify the file format as CSV
# MAGIC 3. Set format options: `'header' = 'true'` and `'inferSchema' = 'true'` (without `inferSchema` every CSV column is read as STRING, which does not match the table's DOUBLE and DATE columns)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Must use `COPY INTO`. Run your cell twice: the table still holds 8 rows, not 16, because COPY INTO skips files it already loaded.

# COMMAND ----------

# EXERCISE_KEY: batch_ex3
# TODO: Use COPY INTO to load CSV files into the existing Delta table
# Your code here

# COMMAND ----------

result_ex3 = spark.table(f"{CATALOG}.{SCHEMA}.ex3_transactions")

# Check 1: Row count (COPY INTO should have loaded exactly 8 rows)
assert result_ex3.count() == 8, f"Expected 8 rows, got {result_ex3.count()}"

# Check 2: Specific value check
txn3 = result_ex3.filter("transaction_id = 'TXN-003'").collect()[0]
assert abs(txn3.amount - 1200.00) < 0.01, f"Expected TXN-003 amount 1200.00, got {txn3.amount}"
assert txn3.currency == "EUR", f"Expected TXN-003 currency EUR, got {txn3.currency}"

# Check 3: Both files loaded
batch2_count = result_ex3.filter("transaction_id >= 'TXN-006'").count()
assert batch2_count == 3, f"Expected 3 rows from transactions_20260118.csv (TXN-006 to TXN-008), got {batch2_count}"

# Check 4: The rows were loaded by COPY INTO
ops = [r.operation for r in spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.ex3_transactions").collect()]
assert "COPY INTO" in ops, f"Load the files with COPY INTO; the table history shows only {ops}"

print("Exercise 3 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Nested JSON - Read, Flatten Structs, Write to Delta
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Read a JSON file containing nested structs, flatten all nested fields into a flat table, and write to Delta.
# MAGIC
# MAGIC **Source**: JSON file at `/Volumes/db_code/batch_ingestion/source_files/ex4_json/`
# MAGIC - 6 event records, each with a nested `user` struct containing `user_id`, `name`, `email`, and
# MAGIC   a further nested `location` struct with `city` and `state`
# MAGIC - Structure: `{event_id, event_type, timestamp, amount, user: {user_id, name, email, location: {city, state}}}`
# MAGIC
# MAGIC **Target**: Write to `db_code.batch_ingestion.ex4_output`. Expected 6 rows and 9 flat columns: `EVT-001` has `user_id` `U-201`, `user_email` `alice@example.com`, `city` `Seattle` and `amount` 149.97; `EVT-005` has `user_name` `Dan Park` and `state` `OR`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read the JSON file(s) from `ex4_json/`
# MAGIC 2. Flatten all nested fields into top-level columns:
# MAGIC    - `user.user_id` -> `user_id`
# MAGIC    - `user.name` -> `user_name`
# MAGIC    - `user.email` -> `user_email`
# MAGIC    - `user.location.city` -> `city`
# MAGIC    - `user.location.state` -> `state`
# MAGIC 3. Keep `event_id`, `event_type`, `timestamp`, and `amount` as-is
# MAGIC 4. Final table should have exactly 9 columns: `event_id`, `event_type`, `timestamp`, `amount`, `user_id`, `user_name`, `user_email`, `city`, `state`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No struct columns in the output (all scalar types)

# COMMAND ----------

# EXERCISE_KEY: batch_ex4
# TODO: Read nested JSON, flatten all structs, and write flat Delta table
# Your code here

# COMMAND ----------

result_ex4 = spark.table(f"{CATALOG}.{SCHEMA}.ex4_output")

# Check 1: Row count
assert result_ex4.count() == 6, f"Expected 6 rows, got {result_ex4.count()}"

# Check 2: Correct flat columns exist
expected_cols_ex4 = {"event_id", "event_type", "timestamp", "amount", "user_id", "user_name", "user_email", "city", "state"}
actual_cols_ex4 = set(result_ex4.columns)
assert expected_cols_ex4 == actual_cols_ex4, f"Expected columns {expected_cols_ex4}, got {actual_cols_ex4}"

# Check 3: No struct columns remain
from pyspark.sql.types import StructType as ST
for field in result_ex4.schema.fields:
    assert not isinstance(field.dataType, ST), f"Column '{field.name}' is still a struct - flatten it"

# Check 4: Specific value check - Alice Chen's purchase
alice = result_ex4.filter("event_id = 'EVT-001'").collect()[0]
assert alice.user_id == "U-201", f"Expected user_id U-201, got {alice.user_id}"
assert alice.city == "Seattle", f"Expected city Seattle, got {alice.city}"
assert alice.user_email == "alice@example.com", f"Expected alice@example.com, got {alice.user_email}"
assert abs(alice.amount - 149.97) < 0.01, f"Expected amount 149.97, got {alice.amount}"

# Check 5: Dan Park from Portland
dan = result_ex4.filter("event_id = 'EVT-005'").collect()[0]
assert dan.state == "OR", f"Expected state OR, got {dan.state}"
assert dan.user_name == "Dan Park", f"Expected Dan Park, got {dan.user_name}"

print("Exercise 4 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Handle Malformed Records with Permissive Mode
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Read a CSV file where some records have type mismatches (non-numeric values in a numeric column,
# MAGIC invalid timestamps). Use permissive mode to capture malformed records separately instead of failing.
# MAGIC
# MAGIC **Source**: CSV file at `/Volumes/db_code/batch_ingestion/source_files/ex5_csv/`
# MAGIC - 10 sensor readings: `sensor_id` (STRING), `reading` (DOUBLE), `unit` (STRING), `recorded_at` (TIMESTAMP)
# MAGIC - Comma-delimited with a header row
# MAGIC - 3 malformed rows:
# MAGIC   - SENS-002: `reading` = "NOT_A_NUMBER" (not a valid double)
# MAGIC   - SENS-004: `recorded_at` = "INVALID_TIMESTAMP" (not a valid timestamp)
# MAGIC   - SENS-009: both `reading` = "BROKEN" and `recorded_at` = "ALSO_BROKEN"
# MAGIC - 1 row with an empty `reading` field: SENS-006 (Spark reads it as null)
# MAGIC
# MAGIC **Target**: Write ONLY the valid rows to `db_code.batch_ingestion.ex5_output`. Expected 7 rows and 4 columns: `SENS-001` has `reading` 23.5.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read the CSV file with `header` = `true` and an explicit schema that enforces types (DOUBLE for `reading`, TIMESTAMP for `recorded_at`)
# MAGIC 2. Use `PERMISSIVE` mode and add a fifth field, `_corrupt_record` (STRING), to the end of your schema: Spark puts the raw line of each malformed row there and leaves it null for valid rows
# MAGIC 3. Filter OUT rows where `_corrupt_record` is not null (those are the malformed ones)
# MAGIC 4. Drop the `_corrupt_record` column from the final output
# MAGIC
# MAGIC **Constraints**:
# MAGIC - SENS-006 (null reading) is valid and should be included (null is not malformed)

# COMMAND ----------

# EXERCISE_KEY: batch_ex5
# TODO: Read CSV with explicit schema, use permissive mode, filter out malformed records
# Your code here

# COMMAND ----------

result_ex5 = spark.table(f"{CATALOG}.{SCHEMA}.ex5_output")

# Check 1: Row count - only 7 valid rows (10 total - 3 malformed)
assert result_ex5.count() == 7, f"Expected 7 rows, got {result_ex5.count()}"

# Check 2: Malformed rows excluded
for bad_sensor in ["SENS-002", "SENS-004", "SENS-009"]:
    assert result_ex5.filter(f"sensor_id = '{bad_sensor}'").count() == 0, f"{bad_sensor} should be excluded (malformed)"

# Check 3: SENS-006 (null reading) is included - null is not malformed
sens006 = result_ex5.filter("sensor_id = 'SENS-006'").collect()
assert len(sens006) == 1, "SENS-006 should be included (null reading is valid, not malformed)"
assert sens006[0].reading is None, "SENS-006 reading should be null"

# Check 4: No _corrupt_record column in output
assert "_corrupt_record" not in result_ex5.columns, "_corrupt_record column should be dropped from output"

# Check 5: Valid value check
sens001 = result_ex5.filter("sensor_id = 'SENS-001'").collect()[0]
assert abs(sens001.reading - 23.5) < 0.01, f"Expected SENS-001 reading 23.5, got {sens001.reading}"

# Check 6: Schema check - 4 columns only
assert len(result_ex5.columns) == 4, f"Expected 4 columns, got {len(result_ex5.columns)}: {result_ex5.columns}"

print("Exercise 5 passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Multi-Format Ingestion with Schema Unification
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Three source systems export employee data in different formats with different column names.
# MAGIC Read all three, rename columns to a unified schema, and write to a single Delta table.
# MAGIC
# MAGIC **Sources**:
# MAGIC
# MAGIC | System | Path | Format | Columns |
# MAGIC |--------|------|--------|---------|
# MAGIC | A | `ex6_csv/` | CSV | `employee_id`, `full_name`, `department`, `salary`, `hire_date` |
# MAGIC | B | `ex6_json/` | JSON | `emp_id`, `name`, `dept`, `annual_salary`, `start_date` |
# MAGIC | C | `ex6_parquet/` | Parquet | `id`, `employee_name`, `department_name`, `compensation`, `date_hired` |
# MAGIC
# MAGIC **Target**: Write to `db_code.batch_ingestion.ex6_output`. Expected 9 rows: `EMP-001` is `Sarah Johnson` with `salary` 125000, `EMP-005` is in `Engineering`, and `EMP-007` has `salary` 150000.
# MAGIC
# MAGIC **Unified schema** (all sources must be renamed to these columns):
# MAGIC
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `employee_id` | STRING | Employee identifier |
# MAGIC | `name` | STRING | Full name |
# MAGIC | `department` | STRING | Department name |
# MAGIC | `salary` | INT | Annual salary |
# MAGIC | `hire_date` | DATE | Date hired |
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read CSV from `ex6_csv/` (comma-delimited, header=true, inferSchema=true)
# MAGIC 2. Read JSON from `ex6_json/`
# MAGIC 3. Read Parquet from `ex6_parquet/`
# MAGIC 4. Rename each source's columns to match the unified schema above
# MAGIC 5. Ensure `salary` is cast to INT and `hire_date` is cast to DATE across all sources
# MAGIC 6. Union all three DataFrames
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No nulls in any column

# COMMAND ----------

# EXERCISE_KEY: batch_ex6
# TODO: Read 3 formats, rename columns to unified schema, union, and write to Delta
# Your code here

# COMMAND ----------

result_ex6 = spark.table(f"{CATALOG}.{SCHEMA}.ex6_output")

# Check 1: Row count
assert result_ex6.count() == 9, f"Expected 9 rows, got {result_ex6.count()}"

# Check 2: Correct unified columns
expected_cols_ex6 = {"employee_id", "name", "department", "salary", "hire_date"}
actual_cols_ex6 = set(result_ex6.columns)
assert expected_cols_ex6 == actual_cols_ex6, f"Expected columns {expected_cols_ex6}, got {actual_cols_ex6}"

# Check 3: No nulls in any column
for col_name in result_ex6.columns:
    null_count = result_ex6.filter(f"{col_name} IS NULL").count()
    assert null_count == 0, f"Found {null_count} nulls in column '{col_name}'"

# Check 4: Employee from each source present
# From CSV (System A)
emp001 = result_ex6.filter("employee_id = 'EMP-001'").collect()
assert len(emp001) == 1, "EMP-001 (from CSV) should be present"
assert emp001[0].name == "Sarah Johnson", f"Expected Sarah Johnson, got {emp001[0].name}"
assert emp001[0].salary == 125000, f"Expected salary 125000, got {emp001[0].salary}"

# From JSON (System B)
emp005 = result_ex6.filter("employee_id = 'EMP-005'").collect()
assert len(emp005) == 1, "EMP-005 (from JSON) should be present"
assert emp005[0].department == "Engineering", f"Expected Engineering, got {emp005[0].department}"

# From Parquet (System C)
emp007 = result_ex6.filter("employee_id = 'EMP-007'").collect()
assert len(emp007) == 1, "EMP-007 (from Parquet) should be present"
assert emp007[0].salary == 150000, f"Expected salary 150000, got {emp007[0].salary}"

# Check 5: salary is INT
from pyspark.sql.types import IntegerType
salary_type = result_ex6.schema["salary"].dataType
assert isinstance(salary_type, IntegerType), f"salary should be INT (cast it in every source), got {salary_type}. An overwrite keeps the column types of a table you already wrote, so rerun the `%run ./setup/batch-ingestion-setup` cell at the top of the notebook, then your Exercise 6 cell."

print("Exercise 6 passed!")
