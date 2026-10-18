# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Windowed Aggregations & Watermarks - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 9
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "windowed_aggregations"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: 5-Minute Tumbling Window Count by event_type
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `pyspark.sql.functions.window(event_ts, "5 minutes")` inside `groupBy`
# MAGIC 2. The `window` function returns a struct - select `window.start` and `window.end` after the aggregation
# MAGIC 3. A `foreachBatch` stream ends in `.start()`, not `.toTable(...)`: chain `.foreachBatch(fn)`, `.outputMode("complete")`, the checkpoint option and the trigger, then `.start().awaitTermination()`.
# MAGIC 4. Complete mode can also write straight to Delta with `.toTable()`, which replaces the table every batch; reach for `foreachBatch` when each batch needs a write the streaming sink does not offer, such as a MERGE or a write to more than one table.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting to flatten the `window` struct - downstream tools and assertions need explicit
# MAGIC   `window_start` / `window_end` columns
# MAGIC - Using `outputMode("append")` without a watermark - Spark rejects this for stateful streaming aggregations
# MAGIC - Using `outputMode("update")` with `.toTable()` - the Delta sink supports only append and complete; for update-style output use `foreachBatch` with a MERGE
# MAGIC - Omitting `.awaitTermination()` - the validate cell runs before the stream finishes

# COMMAND ----------

# EXERCISE_KEY: windows_ex1
from pyspark.sql import functions as F


def overwrite_ex1(batch_df, batch_id):
    (batch_df
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
        )
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex1_output"))


(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .groupBy(F.window("event_ts", "5 minutes"), "event_type")
    .agg(F.count("*").alias("event_count"))
    .writeStream
    .foreachBatch(overwrite_ex1)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: 5-Minute Tumbling Window Sum of amount by user_id
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same pattern as Ex 1 but group by `window(event_ts, "5 minutes")` AND `user_id`
# MAGIC 2. Use `sum("amount")` for the aggregation, alias as `total_amount`
# MAGIC 3. Flatten the window struct after aggregation
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting `user_id` in the window expression instead of `groupBy` - `window()` only accepts the time column
# MAGIC - Using `groupBy(...).sum("amount")` - it computes the right values, but the column comes out as `sum(amount)`; use `.agg(F.sum("amount").alias("total_amount"))`

# COMMAND ----------

# EXERCISE_KEY: windows_ex2
def overwrite_ex2(batch_df, batch_id):
    (batch_df
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "user_id",
            "total_amount",
        )
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex2_output"))


(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .groupBy(F.window("event_ts", "5 minutes"), "user_id")
    .agg(F.sum("amount").alias("total_amount"))
    .writeStream
    .foreachBatch(overwrite_ex2)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: 10-Minute Sliding Window with 5-Minute Slide
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. `window(event_ts, "10 minutes", "5 minutes")` - the third argument is the slide duration
# MAGIC 2. Each event will be assigned to TWO overlapping windows (window_duration / slide_duration = 2)
# MAGIC 3. No other grouping keys - just window and `count(*)`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Swapping the window duration and slide arguments - syntax is `window(ts, window_duration, slide_duration)`
# MAGIC - Setting slide_duration > window_duration - Spark rejects the call (the slide must be <= the window duration); swapping the two arguments here does exactly that
# MAGIC - Expecting one window per event - sliding windows duplicate counts across overlaps

# COMMAND ----------

# EXERCISE_KEY: windows_ex3
def overwrite_ex3(batch_df, batch_id):
    (batch_df
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
        )
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex3_output"))


(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .groupBy(F.window("event_ts", "10 minutes", "5 minutes"))
    .agg(F.count("*").alias("event_count"))
    .writeStream
    .foreachBatch(overwrite_ex3)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Session Window with 10-Minute Inactivity Gap
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Use `session_window(event_ts, "10 minutes")` - 10-minute inactivity gap means a new session
# MAGIC    starts when the next event comes more than 10 minutes after the previous one
# MAGIC 2. Group by `user_id` AND the session_window (order matters less since groupBy is symmetric)
# MAGIC 3. The session_window struct has `.start` (first event) and `.end` (last event + gap duration)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `window()` instead of `session_window()` - tumbling/sliding windows have fixed boundaries,
# MAGIC   session windows expand to fit each user's activity
# MAGIC - Forgetting `user_id` in the groupBy - a streaming query rejects a `session_window` with no other grouping key (a global session works only in batch)
# MAGIC - Confusing session_window with stateful operators that require watermarks - session_window in
# MAGIC   complete mode does NOT require a watermark

# COMMAND ----------

# EXERCISE_KEY: windows_ex4
def overwrite_ex4(batch_df, batch_id):
    (batch_df
        .select(
            "user_id",
            F.col("session_window.start").alias("session_start"),
            F.col("session_window.end").alias("session_end"),
            "event_count",
        )
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex4_output"))


(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .groupBy("user_id", F.session_window("event_ts", "10 minutes"))
    .agg(F.count("*").alias("event_count"))
    .writeStream
    .foreachBatch(overwrite_ex4)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Watermark + Append Mode Emits Only Closed Windows
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Apply `withWatermark("event_ts", "10 minutes")` BEFORE the `groupBy`
# MAGIC 2. Use `outputMode("append")` - append mode emits a window once `window.end <= watermark`
# MAGIC    (the window is "closed"). Open windows remain in state and are not emitted.
# MAGIC 3. Write the aggregation straight to Delta with `.format("delta").toTable(...)` instead of
# MAGIC    `foreachBatch`
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Applying `withWatermark` AFTER the aggregation - the watermark must be on the streaming
# MAGIC   DataFrame before the stateful operator sees it
# MAGIC - Choosing a watermark delay so large that no window closes during `availableNow`: with a 10-minute delay against the 34-minute main-event span, 5 windows close.
# MAGIC - Expecting EVT-049 to be dropped: all 49 rows arrive in one micro-batch before the watermark moves, so it lands in its closed window [09:30,09:35). A row is dropped only when it arrives after its window has already closed (window.end <= watermark), and even then Spark only guarantees it MAY be dropped.

# COMMAND ----------

# EXERCISE_KEY: windows_ex5
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(F.count("*").alias("event_count"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_count",
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Same Query With and Without a Watermark
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Run the no-watermark version first using `foreachBatch` overwrite + complete mode
# MAGIC 2. Run the watermark version second using append mode + direct `.toTable(...)`
# MAGIC 3. The two streams MUST use separate checkpoint locations (a checkpoint encodes the query plan;
# MAGIC    sharing one across two different queries causes failure or silently corrupted state)
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Reusing one checkpoint for both streams - the second query resumes from the first one's offsets and state, so it reads no new rows (or fails if the state does not match its plan)
# MAGIC - Running both streams in append mode without a watermark on the first - append mode requires
# MAGIC   a watermark for stateful aggregations, so the no-watermark stream MUST be complete mode
# MAGIC - Forgetting `.awaitTermination()` on either stream - the cell returns while the stream is still running,
# MAGIC   so the validate cell reads a missing or partial table

# COMMAND ----------

# EXERCISE_KEY: windows_ex6
# Run 1: no watermark, complete mode, foreachBatch overwrite
def overwrite_no_wm(batch_df, batch_id):
    (batch_df
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
        )
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.ex6_output_no_watermark"))


(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(F.count("*").alias("event_count"))
    .writeStream
    .foreachBatch(overwrite_no_wm)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_no_watermark")
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# Run 2: with watermark, append mode, direct toTable
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(F.count("*").alias("event_count"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_count",
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_watermark")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Watermarked Aggregation Grouped by Window + user_id
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Start from the Ex 5 query: add `user_id` to `groupBy` and put both aggregates in one `.agg(...)` call.
# MAGIC 2. If rows for windows starting at 10:20 or later show up, the query is not in append mode; append holds open windows in state.
# MAGIC 3. Expect 15 rows: each closed window has one row per user active in it, so [10:10,10:15) and [10:15,10:20) have 4 rows each and [09:30,09:35) has only USER-005.
# MAGIC 4. `count("*")` counts rows; `count("amount")` skips NULL amounts. They agree here because no amount is NULL.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Expecting open windows ([10:20,10:25), [10:25,10:30), [10:30,10:35)) to appear - they remain in state under
# MAGIC   append mode because their window.end is NOT <= watermark at end-of-stream
# MAGIC - Forgetting that the aggregation key must include `window` (windowed aggregations require
# MAGIC   a window column in groupBy)

# COMMAND ----------

# EXERCISE_KEY: windows_ex7
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"), "user_id")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("total_amount"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "user_id",
        "event_count",
        "total_amount",
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex7")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex7_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Append Mode Requires Watermark and Emits Only Closed Windows
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same skeleton as Ex 5/6, but aggregate `sum(amount)` instead of `count(*)`
# MAGIC 2. Append mode + `.toTable()` writes directly to Delta - no foreachBatch needed
# MAGIC 3. Call `withWatermark` on the source stream, before `groupBy`: after the aggregation `event_ts` no longer exists.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Trying append mode without a watermark - Spark raises AnalysisException at query start
# MAGIC - Expecting open windows to show with partial sums - they are held in state, not emitted
# MAGIC - Picking too short a watermark delay - with a delay of 0 the watermark equals the newest event_ts, so
# MAGIC   a window closes as soon as a newer event passes its end and any out-of-order row for it may be dropped;
# MAGIC   size the delay to how late your data really arrives

# COMMAND ----------

# EXERCISE_KEY: windows_ex8
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"))
    .agg(F.sum("amount").alias("total_amount"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_amount",
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex8")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex8_output")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: Tumbling Window Grouped by Two Keys with Watermark
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. This is the Ex 5 query with two extra grouping keys; nothing else changes.
# MAGIC 2. Each closed window yields one row per (event_type, region) pair present in it, so [10:05,10:10) has 5 rows, not 6.
# MAGIC 3. If you see windows starting at 10:20 or later, check `outputMode("append")`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Adding non-grouping columns to the select after agg - only grouping keys and aggregates
# MAGIC   survive the aggregation, so the resulting DataFrame schema is fixed
# MAGIC - Forgetting that state size grows with the cardinality of grouping keys - watermark + window
# MAGIC   keep state bounded only if event_ts is correlated with arrival time

# COMMAND ----------

# EXERCISE_KEY: windows_ex9
(spark.readStream
    .table(f"{CATALOG}.{SCHEMA}.windows_events")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(F.window("event_ts", "5 minutes"), "event_type", "region")
    .agg(F.count("*").alias("event_count"))
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "event_type",
        "region",
        "event_count",
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex9")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex9_output")
    .awaitTermination()
)
