# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Auto Loader Ingestion - Solutions
# MAGIC **Topic**: ELT | **Exercises**: 6
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "auto_loader"
BASE_SCHEMA = "elt"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Basic cloudFiles Read
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Auto Loader uses `spark.readStream.format("cloudFiles")` with the `cloudFiles.format` option
# MAGIC 2. The write side needs `.format("delta")`, a checkpoint location, and a table name
# MAGIC 3. `trigger(availableNow=True)` processes all files and stops the stream
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `trigger(once=True)` instead of `trigger(availableNow=True)` (deprecated)
# MAGIC - Forgetting `.option("checkpointLocation", ...)` on the write side
# MAGIC - Using `spark.read` instead of `spark.readStream` (not streaming, won't track files)
# MAGIC - Not calling `.awaitTermination()` (stream starts but code continues before it finishes)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex1
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex1_schema")
    .load(f"{VOLUME_BASE}/ex1_files/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Schema Hints for Type Control
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Schema hints use the option `cloudFiles.schemaHints`
# MAGIC 2. The syntax is a comma-separated list of `column_name type` pairs
# MAGIC 3. Schema hints override inferred types without requiring a full schema
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Providing a full schema instead of hints (works but defeats the purpose of inference)
# MAGIC - Wrong hint syntax: must be `"amount DOUBLE"`, not `"amount: DOUBLE"` or `"amount=DOUBLE"`
# MAGIC - Forgetting that hints layer on top of inference - you only need to specify the columns you want to override

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex2
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "amount DOUBLE")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex2_schema")
    .load(f"{VOLUME_BASE}/ex2_files/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Schema Evolution Handling
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `cloudFiles.schemaEvolutionMode` set to `addNewColumns`
# MAGIC 2. Process batch 1 first (creates table with base schema), then batch 2 (adds priority column)
# MAGIC 3. Both streams write to the same target table but use separate schema locations and separate checkpoints
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Processing both batches in a single stream pointing at one directory (files not in same dir)
# MAGIC - Using `rescue` mode instead of `addNewColumns` (rescue captures new columns as JSON, doesn't add them)
# MAGIC - Not using `mergeSchema` on the write side (needed for Delta to accept new columns)
# MAGIC - Sharing the same schema location for both streams (batch 2 loads batch 1's schema, then fails with UNKNOWN_FIELD_EXCEPTION when it encounters the new column)
# MAGIC - Reusing the same checkpoint for both streams (checkpoints are per-stream)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex3
# Step 1: Process batch 1 (base schema - establishes the table)
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex3_schema_batch1")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{VOLUME_BASE}/ex3_files_batch1/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3_batch1")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_output")
    .awaitTermination()
)

# Step 2: Process batch 2 (has new 'priority' column - triggers schema evolution)
# Uses a separate schema location so Auto Loader infers the full schema (including
# the new 'priority' column) fresh, rather than loading batch 1's schema and failing
# with UNKNOWN_FIELD_EXCEPTION. mergeSchema on the write side lets Delta accept the
# new column.
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex3_schema_batch2")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{VOLUME_BASE}/ex3_files_batch2/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3_batch2")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Rescued Data Column
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Provide an explicit schema to Auto Loader so it knows `amount` should be DOUBLE
# MAGIC 2. Set `cloudFiles.rescuedDataColumn` to `_rescued_data`
# MAGIC 3. Records that don't match the schema aren't dropped - they appear with null in the mismatched column and the original value in `_rescued_data`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Relying on schema inference (infers STRING for amount since some values are strings - no rescue needed)
# MAGIC - Forgetting to provide a schema (without explicit schema, Auto Loader can't know what's "wrong")
# MAGIC - Using `columnNameOfCorruptRecord` (that's for the built-in JSON reader, not Auto Loader)
# MAGIC - Confusing rescued data with schema evolution (rescued data = type mismatches, schema evolution = new columns)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex4
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ex4_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(ex4_schema)
    .option("rescuedDataColumn", "_rescued_data")
    .load(f"{VOLUME_BASE}/ex4_files/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Auto Loader with CSV Format and Custom Options
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Set `cloudFiles.format` to `csv` and use the `delimiter` option for the pipe character
# MAGIC 2. Set `header` to `true` so Auto Loader reads column names from the first row
# MAGIC 3. Use `cloudFiles.schemaHints` to cast `amount` as DOUBLE (CSV infers everything as STRING)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `sep` instead of `delimiter` (for cloudFiles CSV, the option is `delimiter`)
# MAGIC - Forgetting `header` = `true` (columns become `_c0`, `_c1`, etc.)
# MAGIC - Not casting numeric columns (amount stays STRING, downstream comparisons fail)
# MAGIC - Confusing cloudFiles options (prefixed with `cloudFiles.`) with reader options (no prefix)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex5
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("cloudFiles.schemaHints", "amount DOUBLE")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex5_schema")
    .load(f"{VOLUME_BASE}/ex5_files/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Auto Loader + MERGE for Incremental Dedup Upsert
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `foreachBatch` on the writeStream to get a static DataFrame per micro-batch
# MAGIC 2. Inside the `foreachBatch` function, create a temp view from the batch and run a MERGE
# MAGIC 3. The MERGE matches on `order_id` and does UPDATE SET * when matched, INSERT * when not matched
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying to run MERGE directly on a streaming DataFrame (MERGE only works on static DataFrames)
# MAGIC - Using `.writeStream.format("delta").toTable()` (this appends, doesn't merge)
# MAGIC - Not registering the batch DataFrame as a temp view (needed for SQL MERGE syntax)
# MAGIC - Forgetting `.awaitTermination()` (MERGE runs async, assertions fail)
# MAGIC - Using the same checkpoint as the regular write exercises (checkpoints are stream-specific)

# COMMAND ----------

# EXERCISE_KEY: auto_loader_ex6
def upsert_to_target(batch_df, batch_id):
    """Process each micro-batch by MERGing into the target table."""
    batch_df.createOrReplaceTempView("auto_loader_ex6_batch")
    batch_df.sparkSession.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.ex6_target t
        USING auto_loader_ex6_batch s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/ex6_schema")
    .load(f"{VOLUME_BASE}/ex6_files/")
    .writeStream
    .foreachBatch(upsert_to_target)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)
