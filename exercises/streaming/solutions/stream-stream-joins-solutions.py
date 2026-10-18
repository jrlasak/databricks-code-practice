# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Stream Joins - Solutions
# MAGIC **Topic**: Streaming | **Exercises**: 6
# MAGIC
# MAGIC Reference solutions, hints, and common mistakes for each exercise.
# MAGIC Try solving the exercises first before looking here.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "stream_stream_joins"
BASE_SCHEMA = "streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join with No Watermark, No Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Read each side with `spark.readStream.table(...)`. Once both are streaming DataFrames,
# MAGIC    the `.join(...)` call on streaming DFs is a stream-stream join automatically.
# MAGIC 2. Join on the column name, `on="order_id"` (default join type is inner), so the output keeps
# MAGIC    one `order_id` column.
# MAGIC 3. The write side needs `.format("delta")`, a checkpoint location, `trigger(availableNow=True)`,
# MAGIC    and `.toTable(...)`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Using `spark.read.table` instead of `spark.readStream.table` - that's a static read, not a stream.
# MAGIC - Forgetting `.awaitTermination()` - the next cell runs before the stream completes and the
# MAGIC   target table is empty when assertions fire.
# MAGIC - Using `trigger(once=True)` - deprecated. Use `trigger(availableNow=True)`.
# MAGIC - Keeping both `order_id` columns. A join on an expression (`orders.order_id == shipments.order_id`) returns two `order_id` columns; pass `on="order_id"` or select the columns explicitly so the output has one.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex1
from pyspark.sql.functions import col

orders = spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
shipments = spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")

joined = orders.join(shipments, on="order_id", how="inner").select(
    "order_id",
    "customer_id",
    "amount",
    "order_ts",
    "shipment_id",
    "carrier",
    "shipment_ts",
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex1")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex1_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inner Join with Watermarks on Both Sides
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Apply `.withWatermark("<event_time_col>", "<delay>")` to each streaming DataFrame BEFORE
# MAGIC    joining.
# MAGIC 2. The watermark delay is a string like `"1 minute"` or `"10 minutes"`.
# MAGIC 3. The join condition is still just `order_id` equality - no time-range predicate yet.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Applying watermark AFTER the join - Spark needs the watermark on the source DataFrames
# MAGIC   so it can track event-time progress per side.
# MAGIC - Wrong unit spelling: `"1 minutes"` works, but `"1 min"` does NOT.
# MAGIC - Thinking watermark alone bounds state. It does not - without a time-range predicate,
# MAGIC   Spark cannot drop state because any future row could still match.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex2
orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

joined = orders_wm.join(shipments_wm, on="order_id", how="inner").select(
    "order_id",
    "customer_id",
    "amount",
    "order_ts",
    "shipment_id",
    "carrier",
    "shipment_ts",
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex2")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex2_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Inner Join with Watermark + 5-Minute Time-Range Constraint
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. The time-range predicate goes inside the join condition, alongside the `order_id`
# MAGIC    equality. Use `&` to combine in PySpark.
# MAGIC 2. Reference both timestamp columns via the source DataFrames (e.g.,
# MAGIC    `shipments_wm.shipment_ts`) so Spark can statically detect the time bound.
# MAGIC 3. The SQL interval syntax `INTERVAL 5 MINUTES` is available via `expr(...)` or in raw
# MAGIC    SQL; in PySpark you can also write `orders_wm.order_ts + expr("INTERVAL 5 MINUTES")`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Putting the time-range predicate in a `.filter(...)` after the join. In this inner join the optimizer moves it into the join condition, so nothing breaks, but the same habit in a left or right outer join leaves the join with no time bound and Spark rejects the query.
# MAGIC - Using a one-sided bound (only `shipment_ts >= order_ts`) - that doesn't cap how far apart
# MAGIC   matches can be in event time, so state still grows.
# MAGIC - Forgetting `withWatermark` on one side. The inner join still runs, but the OTHER side's state is never expired: without a shipments watermark Spark cannot know when an order has seen its last possible shipment, so orders pile up in state (and the mirror case for a missing orders watermark).

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex3
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="inner").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex3")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex3_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Left Outer Join with Watermark + Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Switch the join type to `"leftOuter"` (or `"left_outer"`). Everything else from Exercise
# MAGIC    3 stays the same.
# MAGIC 2. NULL rows for unmatched left orders are emitted when the watermark passes the order's
# MAGIC    window end (`order_ts + 5 minutes`). The setup includes two watermark-advancing orders
# MAGIC    so the watermark gets there.
# MAGIC 3. The output schema should still have all 7 columns. Unmatched-left rows have NULL in
# MAGIC    `shipment_id`, `carrier`, and `shipment_ts`.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Forgetting that outer joins REQUIRE both `withWatermark` AND a time-range predicate.
# MAGIC   Spark raises `AnalysisException` if either is missing.
# MAGIC - Expecting unmatched rows to appear immediately. They only emit AFTER the watermark passes
# MAGIC   the upper bound of the window. With `availableNow`, the watermark advances based on event
# MAGIC   time across the data - rows with later event times push it forward and let earlier unmatched rows be emitted.
# MAGIC - Passing `on="order_id"` (a string column name) instead of a full join condition
# MAGIC   expression. For stream-stream outer joins with a time predicate you need the expression
# MAGIC   form so the predicate is part of the join.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex4
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="leftOuter").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex4")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex4_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Right Outer Join with Watermark + Time Bound
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Same join condition as Exercise 4. Only `how` changes - use `"rightOuter"`.
# MAGIC 2. Now `shipment_id` is the "always present" column. Unmatched rows have NULL `order_id`,
# MAGIC    `customer_id`, `amount`, `order_ts`.
# MAGIC 3. Spark requires both watermark and time bound for outer joins - same as left outer.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Confusing which side gets NULLed. In `leftOuter`, the RIGHT side gets nulls when no match.
# MAGIC   In `rightOuter`, the LEFT side gets nulls.
# MAGIC - Filtering the output for `order_id IS NOT NULL` and wondering why unmatched shipments
# MAGIC   disappear - that filter removes exactly the rows the outer join produced.
# MAGIC - Expecting both sides to leave state at the same time. With `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`, an order stays in state until the watermark passes `order_ts + 5 minutes`, a shipment only until it passes `shipment_ts`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex5
from pyspark.sql.functions import expr

orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

joined = orders_wm.join(shipments_wm, on=join_cond, how="rightOuter").select(
    orders_wm.order_id,
    orders_wm.customer_id,
    orders_wm.amount,
    orders_wm.order_ts,
    shipments_wm.shipment_id,
    shipments_wm.carrier,
    shipments_wm.shipment_ts,
)

(joined.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex5")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex5_target")
    .awaitTermination()
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Measure Unbounded Join State
# MAGIC
# MAGIC **Hints**:
# MAGIC 1. Reuse the Exercise 1 join for the unbounded side and the Exercise 3 join for the bounded side. Give each its own checkpoint and target, and keep the `StreamingQuery` each `.toTable(...)` returns.
# MAGIC 2. Under `availableNow`, each query stops after one or two micro-batches, so the state growth over time does not show; on a stream that keeps running it would. What does show is how many rows each join still holds when it stops.
# MAGIC 3. `query.lastProgress["stateOperators"]` is a list with one entry per stateful operator. A stream-stream join has one, and its `numRowsTotal` is the number of rows in its state.
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC - Reading `lastProgress` before `.awaitTermination()` returns - it may describe an earlier batch, not the final one.
# MAGIC - Typing the numbers you saw in the query progress into `ex6_state` instead of reading them from `lastProgress` in code.
# MAGIC - Overwriting the first `StreamingQuery` variable with the second query: keep one handle per join so you can read each one's `lastProgress`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex6
from pyspark.sql.functions import expr

# Unbounded: no withWatermark, no time-range predicate (the Exercise 1 join)
orders = spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
shipments = spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")

unbounded_query = (orders.join(shipments, on="order_id", how="inner")
    .select("order_id", "customer_id", "amount", "order_ts", "shipment_id", "carrier", "shipment_ts")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_unbounded")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_unbounded_target")
)
unbounded_query.awaitTermination()
unbounded_state_rows = unbounded_query.lastProgress["stateOperators"][0]["numRowsTotal"]

# Bounded: watermark on both sides + time-range predicate in the join condition (the Exercise 3 join)
orders_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.orders_stream")
    .withWatermark("order_ts", "1 minute")
)
shipments_wm = (
    spark.readStream.table(f"{CATALOG}.{SCHEMA}.shipments_stream")
    .withWatermark("shipment_ts", "1 minute")
)

join_cond = (
    (orders_wm.order_id == shipments_wm.order_id)
    & (shipments_wm.shipment_ts >= orders_wm.order_ts)
    & (shipments_wm.shipment_ts <= orders_wm.order_ts + expr("INTERVAL 5 MINUTES"))
)

bounded_query = (orders_wm.join(shipments_wm, on=join_cond, how="inner")
    .select(
        orders_wm.order_id,
        orders_wm.customer_id,
        orders_wm.amount,
        orders_wm.order_ts,
        shipments_wm.shipment_id,
        shipments_wm.carrier,
        shipments_wm.shipment_ts,
    )
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/ex6_bounded")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.ex6_bounded_target")
)
bounded_query.awaitTermination()
bounded_state_rows = bounded_query.lastProgress["stateOperators"][0]["numRowsTotal"]

spark.createDataFrame(
    [(int(unbounded_state_rows), int(bounded_state_rows))],
    "unbounded_state_rows BIGINT, bounded_state_rows BIGINT",
).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ex6_state")
