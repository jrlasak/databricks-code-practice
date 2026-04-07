# Databricks notebook source
# MAGIC %md
# MAGIC # Solutions: Batch Data Ingestion & File Formats
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "batch_ingestion"
BASE_SCHEMA = "elt"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 1: Read CSV with Header, Delimiter, and Schema Options
# MAGIC
# MAGIC **Hints** (try before reading the solution):
# MAGIC 1. `spark.read` has a `.csv()` method that accepts a path and options via chaining
# MAGIC 2. The key options are `header`, `inferSchema`, and `sep` (or `delimiter`)
# MAGIC 3. Point `spark.read.csv()` at the directory (not individual files) to read all CSVs in it
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting `header=True` and getting `_c0, _c1, ...` as column names
# MAGIC - Using the default comma delimiter when the file uses pipes
# MAGIC - Not using `inferSchema=True`, which makes all columns StringType
# MAGIC - Pointing to a single file instead of the directory (misses the second batch)

# COMMAND ----------

# EXERCISE_KEY: batch_ex1
df_ex1 = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "|")
    .csv(f"{VOLUME_PATH}/ex1_csv/")
)

df_ex1.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex1_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 2: Read JSON with Explicit Schema
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Import `StructType`, `StructField`, `StringType`, and `DoubleType` from `pyspark.sql.types`
# MAGIC 2. Build the schema as `StructType([StructField("col_name", ColType(), True), ...])`
# MAGIC 3. Pass the schema to `spark.read.schema(your_schema).json(path)`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read.json()` without `.schema()` - this uses schema inference, which defeats the purpose
# MAGIC - Defining `amount` as StringType instead of DoubleType
# MAGIC - Forgetting `nullable=True` on StructField (third positional argument)
# MAGIC - Misspelling field names (must match the JSON keys exactly)

# COMMAND ----------

# EXERCISE_KEY: batch_ex2
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ex2_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
])

df_ex2 = spark.read.schema(ex2_schema).json(f"{VOLUME_PATH}/ex2_json/")

df_ex2.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex2_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 3: COPY INTO for Idempotent Incremental Loading
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `COPY INTO` is a SQL command, so use `spark.sql()` to execute it
# MAGIC 2. The syntax is `COPY INTO target_table FROM 'source_path' FILEFORMAT = CSV FORMAT_OPTIONS (...)`
# MAGIC 3. Format options go inside `FORMAT_OPTIONS(...)` as key-value pairs
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read` instead of COPY INTO (works but defeats the purpose and is not idempotent)
# MAGIC - Wrong syntax for FORMAT_OPTIONS (it uses parentheses, not curly braces)
# MAGIC - Forgetting `header = 'true'` in format options (loads header row as data)
# MAGIC - Not quoting the source path (must be a string literal in SQL)

# COMMAND ----------

# EXERCISE_KEY: batch_ex3
spark.sql(f"""
COPY INTO {CATALOG}.{SCHEMA}.ex3_transactions
FROM '{VOLUME_PATH}/ex3_csv/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 4: Nested JSON - Read, Flatten Structs, Write to Delta
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `spark.read.json()` to read the JSON file - Spark infers the nested schema automatically
# MAGIC 2. Access nested fields with dot notation: `col("user.user_id")`
# MAGIC 3. Use `.select()` with `.alias()` to rename nested fields to flat column names
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `.*` (star expansion) on a struct without selecting specific fields
# MAGIC - Forgetting to alias the nested column (ends up with `user.user_id` as column name including the dot)
# MAGIC - Missing the double nesting: `user.location.city` is two levels deep
# MAGIC - Using `explode()` on structs (explode is for arrays, not structs)

# COMMAND ----------

# EXERCISE_KEY: batch_ex4
from pyspark.sql.functions import col

df_ex4 = spark.read.json(f"{VOLUME_PATH}/ex4_json/")

df_ex4_flat = df_ex4.select(
    col("event_id"),
    col("event_type"),
    col("timestamp"),
    col("amount"),
    col("user.user_id").alias("user_id"),
    col("user.name").alias("user_name"),
    col("user.email").alias("user_email"),
    col("user.location.city").alias("city"),
    col("user.location.state").alias("state"),
)

df_ex4_flat.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex4_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 5: Handle Malformed Records with Permissive Mode
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Define a `StructType` schema explicitly with the expected types (DOUBLE for reading, TimestampType for recorded_at)
# MAGIC 2. Add a `_corrupt_record` column of StringType at the end of your schema
# MAGIC 3. Set `.option("mode", "PERMISSIVE")` and `.option("columnNameOfCorruptRecord", "_corrupt_record")`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to add `_corrupt_record` to the schema definition (Spark silently ignores the option)
# MAGIC - Using `DROPMALFORMED` mode instead of `PERMISSIVE` (drops rows silently, no way to inspect them)
# MAGIC - Filtering on `reading IS NULL` instead of `_corrupt_record IS NOT NULL` (SENS-006 has a legitimately null reading)
# MAGIC - Not using an explicit schema (inferSchema with permissive mode infers everything as STRING, so nothing is "malformed")

# COMMAND ----------

# EXERCISE_KEY: batch_ex5
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

ex5_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("reading", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("recorded_at", TimestampType(), True),
    StructField("_corrupt_record", StringType(), True),
])

df_ex5 = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(ex5_schema)
    .csv(f"{VOLUME_PATH}/ex5_csv/")
)

# Filter out malformed rows and drop the corrupt record column
df_ex5_clean = (
    df_ex5
    .filter("_corrupt_record IS NULL")
    .drop("_corrupt_record")
)

df_ex5_clean.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex5_output")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercise 6: Multi-Format Ingestion with Schema Unification
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Read each format separately: `spark.read.csv()`, `spark.read.json()`, `spark.read.parquet()`
# MAGIC 2. Use `.withColumnRenamed()` or `.select(col("old_name").alias("new_name"))` to rename columns
# MAGIC 3. After renaming, use `.unionByName()` to combine DataFrames (safer than `.union()` because it matches by column name)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `.union()` instead of `.unionByName()` (union matches by position, so mismatched column order = wrong data)
# MAGIC - Forgetting to cast `salary`/`compensation` to the same type across all sources (union fails on type mismatch)
# MAGIC - Not casting `start_date` from JSON (JSON dates are strings, need explicit cast to DateType)
# MAGIC - Leaving extra columns from one source that don't exist in others (unionByName fails unless `allowMissingColumns=True`)

# COMMAND ----------

# EXERCISE_KEY: batch_ex6
from pyspark.sql.functions import col

# Source A: CSV
df_a = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/ex6_csv/")
    .select(
        col("employee_id"),
        col("full_name").alias("name"),
        col("department"),
        col("salary").cast("int").alias("salary"),
        col("hire_date").cast("date").alias("hire_date"),
    )
)

# Source B: JSON
df_b = (
    spark.read
    .json(f"{VOLUME_PATH}/ex6_json/")
    .select(
        col("emp_id").alias("employee_id"),
        col("name"),
        col("dept").alias("department"),
        col("annual_salary").cast("int").alias("salary"),
        col("start_date").cast("date").alias("hire_date"),
    )
)

# Source C: Parquet
df_c = (
    spark.read
    .parquet(f"{VOLUME_PATH}/ex6_parquet/")
    .select(
        col("id").alias("employee_id"),
        col("employee_name").alias("name"),
        col("department_name").alias("department"),
        col("compensation").cast("int").alias("salary"),
        col("date_hired").cast("date").alias("hire_date"),
    )
)

# Union all three and write
df_combined = df_a.unionByName(df_b).unionByName(df_c)
df_combined.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex6_output")
