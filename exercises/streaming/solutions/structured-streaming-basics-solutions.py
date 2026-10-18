# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Structured Streaming Basics - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 8
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "structured_streaming_basics"
BASE_SCHEMA = "streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Read a Delta Table as a Stream, Write to Another Delta Table
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `spark.readStream.table("...")` treats an existing Delta table as a streaming source - the first run reads the rows already there, and every later commit produces new streamed rows
# MAGIC 2. The write side needs `.format("delta")`, a `checkpointLocation`, and `.toTable(...)` with the full target table name
# MAGIC 3. `trigger(availableNow=True)` plus `.awaitTermination()` is the "run once and stop" pattern in an interactive notebook; in a Lakeflow Jobs task, leave out `awaitTermination()` (the Jobs service waits for the stream)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read` instead of `spark.readStream` (no offsets, won't track progress)
# MAGIC - Using `trigger(once=True)` (deprecated; `availableNow=True` is its replacement)
# MAGIC - Forgetting `.awaitTermination()` (cell returns before stream finishes, assertions fail)
# MAGIC - Putting the checkpoint somewhere outside a Volume (works in some environments, fails on
# MAGIC   Free Edition where DBFS is restricted)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex1
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex1_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Filter a Streaming DataFrame Before Writing
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Streaming DataFrames support `.filter(...)` and `.where(...)` exactly like batch DataFrames
# MAGIC 2. Compose the filter directly on the result of `spark.readStream.table(...)` before
# MAGIC    starting the writeStream
# MAGIC 3. Filter the streaming DataFrame itself: the filter becomes part of the streaming plan, so non-purchase rows never reach the target
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running the filter as a post-write SQL query against the target (defeats the purpose -
# MAGIC   non-purchase rows still land in the target)
# MAGIC - Filtering on the wrong column or wrong value (assertion count mismatch)
# MAGIC - Mixing PySpark column syntax with SQL string syntax: use `F.col("event_type") == "purchase"`
# MAGIC   in Python OR `"event_type = 'purchase'"` as a SQL string - not both

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex2
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex2_source")
    .filter("event_type = 'purchase'")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Add a Derived Column to a Streaming DataFrame
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Streaming DataFrames support `withColumn` exactly like batch DataFrames; the new column becomes part of the streaming plan
# MAGIC 2. Import `pyspark.sql.functions as F` once at the top of the cell
# MAGIC 3. `.withColumn("amount_with_tax", F.col("amount") * 1.10)` on the streaming DataFrame, before `.writeStream`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Referring to the column by bare Python name (e.g., `amount * 1.10` with no import) -
# MAGIC    use `F.col("amount") * 1.10` or a SQL expression in `selectExpr`/`expr`.
# MAGIC - Using `selectExpr` and dropping other columns - assertions check that `amount` is still present
# MAGIC - Naming the column something other than `amount_with_tax` (the validation cell looks for that exact name)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex3
from pyspark.sql import functions as F

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex3_source")
    .withColumn("amount_with_tax", F.col("amount") * 1.10)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Pick the Right Output Mode for an Ingestion Stream
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Output mode rules: `append` works for any non-aggregating query and for aggregations with a watermark that emits closed windows; `complete` requires an aggregation and emits the FULL result table every batch; `update` emits only the rows that changed
# MAGIC 2. This query has no aggregation, so `complete` fails; `update` is legal Spark but the Delta sink rejects it; only `append` runs
# MAGIC 3. Always state output mode explicitly in production code so readers see your intent
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Picking `complete` because "it sounds safer" - it's illegal without an aggregation and
# MAGIC    Spark throws `AnalysisException` immediately
# MAGIC - Picking `update` thinking it means "update the target table" - it means "emit only the rows that changed", and the Delta sink does not support it, so the query fails to start
# MAGIC - Relying on the default mode without stating it - a code reviewer cannot tell whether you chose `append` on purpose

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex4
correct_mode = "append"

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex4_source")
    .writeStream
    .format("delta")
    .outputMode(correct_mode)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Trigger Modes - Understanding the Cost/Latency Tradeoff
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `trigger(availableNow=True)` processes everything available (in one or more micro-batches), then stops and `awaitTermination()` returns. On Free Edition serverless only `availableNow` and the deprecated `once` trigger run.
# MAGIC 2. The cost/latency tradeoff is conceptual: `processingTime` keeps compute warm for lower
# MAGIC    latency; `availableNow` spins compute down between runs for lower cost.
# MAGIC 3. In production on classic compute you could use `.trigger(processingTime='30 seconds')`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `.trigger(processingTime=...)` on Free Edition serverless - serverless rejects the trigger.
# MAGIC - Confusing the two winners: lower LATENCY comes from a `processingTime` trigger (the query stays up); lower COST comes from `availableNow` because compute spins down between scheduled runs.

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex5
# availableNow - processes everything available, then stops by itself
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex5_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5_an")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_target_available_now")
    .awaitTermination()
)

latency_winner = "processingTime"  # query stays up -> new rows land within one interval
cost_winner = "availableNow"        # compute spins down between runs -> lower cost

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Streaming Aggregation in Complete Output Mode
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `groupBy("event_type").count()` returns a DataFrame with columns `event_type` and
# MAGIC    `count`. Rename `count` to `cnt` with `.withColumnRenamed("count", "cnt")` or use
# MAGIC    `.agg(F.count("*").alias("cnt"))` for clarity
# MAGIC 2. Output mode MUST be `complete` here: the Delta sink supports only `append` and `complete`, and `append` on an aggregation needs an event-time window or column with a watermark in the grouping, which `groupBy("event_type")` does not have. Complete mode overwrites the full result table on every micro-batch
# MAGIC 3. `availableNow` plus `awaitTermination()` still applies - the cell must block until the
# MAGIC    batch is done so assertions read the final result table
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying `append` mode - the query fails to start with an `AnalysisException` saying append output mode is not supported for streaming aggregations without a watermark
# MAGIC - Using `count()` and letting the column name stay `count` - assertions look for `cnt`
# MAGIC - Forgetting that `complete` mode rewrites the target every batch - fine here (small key space); for large key spaces write the aggregate with `foreachBatch` and a MERGE (the Delta sink has no `update` mode)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex6
from pyspark.sql import functions as F

(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex6_source")
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Name a Query and Inspect lastProgress
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `.queryName("ss_basics_ex7")` must be called on the writeStream BEFORE `.toTable(...)`
# MAGIC 2. To persist, build a 1-row DataFrame with `spark.createDataFrame([(...)], schema)` and
# MAGIC    `.write.format("delta").mode("overwrite").saveAsTable(...)` - this is a batch write of
# MAGIC    a captured metric, not a streaming write
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Reading `lastProgress` before the stream terminates - it is `None` until the first batch finishes, and after that it may describe an earlier batch, not the final one
# MAGIC - Using `recentProgress` (a list) and forgetting it's a list, not a dict
# MAGIC - Calling `.queryName(...)` after `.toTable(...)` - `.toTable(...)` returns a running `StreamingQuery`, which has no `queryName` method, so the call raises `AttributeError` and the query stays unnamed (`name` is `None`)

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex7
from pyspark.sql.types import StructType, StructField, StringType, LongType

query = (spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex7_source")
    .writeStream
    .queryName("ss_basics_ex7")
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex7_target")
)
query.awaitTermination()

progress = query.lastProgress
metrics_schema = StructType([
    StructField("query_name", StringType()),
    StructField("num_input_rows", LongType()),
])
metrics_df = spark.createDataFrame(
    [(progress["name"], int(progress["numInputRows"]))],
    schema=metrics_schema,
)
metrics_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.ex7_progress"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Deduplicate a Stream by event_id
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Deduplicating a stream is a stateful operation: Spark remembers every key it has seen, so a repeat is dropped even when it arrives in a later micro-batch
# MAGIC 2. Apply the dedup on the streaming DataFrame BEFORE `.writeStream`, so it is part of the streaming plan
# MAGIC 3. `dropDuplicates(["event_id"])` keeps one row per `event_id`. Spark does not promise which copy survives within a micro-batch, which does not matter here because the copies are identical
# MAGIC 4. In production, bound the dedup state: `withWatermark("event_ts", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])`, or keep the same `withWatermark(...)` and put `event_ts` in the `dropDuplicates` key. Without a watermark, `dropDuplicates` keeps every key forever, even with `event_ts` in the key.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Running a post-write `SELECT DISTINCT * FROM target` - works for a single run but not
# MAGIC    incremental: subsequent batches would re-introduce duplicates
# MAGIC - Calling `dropDuplicates()` with no arguments - dedups on ALL columns, which fails the
# MAGIC    moment one duplicate has slightly different non-key columns
# MAGIC - Forgetting that dropDuplicates is stateful - on a long-running stream, `dropDuplicates(["event_id"])` keeps every key in state forever (bound it as in Hint 4). For this exercise's bounded source it's fine

# COMMAND ----------

# EXERCISE_KEY: ss_basics_ex8
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.ex8_source")
    .dropDuplicates(["event_id"])
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex8")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex8_target")
    .awaitTermination()
)
