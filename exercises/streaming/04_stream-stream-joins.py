# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Stream-Stream Joins
# MAGIC **Topic**: Streaming | **Exercises**: 6 | **Total Time**: ~100 min
# MAGIC
# MAGIC Practice joining two streaming DataFrames in Structured Streaming. Covers inner and outer
# MAGIC semantics, the role of watermarks on both sides, and how a time-range constraint together
# MAGIC with a watermark bounds the state Spark must keep. Also measures how much state an
# MAGIC unbounded join keeps compared with a bounded one.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/stream-stream-joins-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - A stream-stream join needs both sides to be streaming DataFrames (`spark.readStream.table(...)`).
# MAGIC - Inner joins run without watermarks, but their state then grows without limit: Spark expires it only with watermarks plus an event-time constraint in the join condition (a time-range predicate, or a join on matching event-time windows).
# MAGIC - Outer joins (left/right) REQUIRE watermark + time bound. Unmatched-side NULL rows are
# MAGIC   emitted only AFTER the watermark passes the join window.
# MAGIC - The sources are Delta tables (Kafka is unavailable on Free Edition).
# MAGIC
# MAGIC **Tables used** (created by the setup notebook):
# MAGIC - `db_code.stream_stream_joins.orders_stream` (32 rows): `order_id`, `customer_id`, `amount`, `order_ts`
# MAGIC - `db_code.stream_stream_joins.shipments_stream` (32 rows): `shipment_id`, `order_id`, `carrier`, `shipment_ts`
# MAGIC
# MAGIC **Pair design** (reference table for the exercises below):
# MAGIC | Range | Count | Time gap (shipment_ts - order_ts) | In 5-min window? |
# MAGIC |---|---|---|---|
# MAGIC | ORD-001..ORD-020 paired with SHP-001..SHP-020 | 20 | +2 min | YES |
# MAGIC | ORD-021..ORD-025 paired with SHP-021..SHP-025 | 5 | +10 min | NO |
# MAGIC | ORD-026..ORD-030 (no shipment) | 5 | n/a | unmatched left |
# MAGIC | ORD-200..ORD-201 (watermark-advancing, no shipment) | 2 | n/a | unmatched left |
# MAGIC | SHP-026..SHP-030 reference ORD-901..ORD-905 (no order) | 5 | n/a | unmatched right |
# MAGIC | SHP-200..SHP-201 reference ORD-906..ORD-907 (watermark-advancing, no order) | 2 | n/a | unmatched right |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/stream-stream-joins-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Two streaming source tables and the `stream_stream_joins` schema are ready.
# MAGIC
# MAGIC Exercises 1-5 each write to their own target table (`db_code.stream_stream_joins.exN_target`) and checkpoint to `/Volumes/db_code/stream_stream_joins/checkpoints/exN` (`f"{CHECKPOINT_BASE}/exN"` in code); Exercise 6 names its own tables and checkpoints. The setup defines three Python variables you can use: `CATALOG = "db_code"`, `SCHEMA = "stream_stream_joins"` and `CHECKPOINT_BASE = "/Volumes/db_code/stream_stream_joins/checkpoints"`.
# MAGIC
# MAGIC Every join output table has the same 7 columns: `order_id` (from `orders_stream`), `customer_id`, `amount`, `order_ts`, `shipment_id`, `carrier`, `shipment_ts`.
# MAGIC
# MAGIC **Free Edition rules** (apply to every exercise):
# MAGIC - Read each side with `spark.readStream.table("db_code.stream_stream_joins.<source>")`.
# MAGIC - Every stream needs `.trigger(availableNow=True)` (serverless). End each query with `.awaitTermination()` so the validation cell runs after the stream has finished.
# MAGIC - No Spark config is needed: serverless lets you set only a short list of properties, and none of these exercises uses one.
# MAGIC
# MAGIC **Redo an exercise**: a checkpoint remembers which rows its stream already read, so running a fixed TODO cell again reads nothing new. Re-run the `%run ./setup/stream-stream-joins-setup` cell first: it drops every target and checkpoint of this notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join with No Watermark, No Time Bound
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Inner-join the two streams on `order_id`. No watermark, no time-range predicate. Verify
# MAGIC that every order with a matching shipment appears in the output, regardless of how far
# MAGIC apart their timestamps are.
# MAGIC
# MAGIC **Source 1** (`db_code.stream_stream_joins.orders_stream`): 32 rows of orders.
# MAGIC **Source 2** (`db_code.stream_stream_joins.shipments_stream`): 32 rows of shipments.
# MAGIC
# MAGIC **Target**: Write the joined stream to `db_code.stream_stream_joins.ex1_target` as Delta.
# MAGIC Columns: `order_id`, `customer_id`, `amount`, `order_ts`, `shipment_id`, `carrier`, `shipment_ts`.
# MAGIC
# MAGIC **Expected Output**: 25 rows. Every `order_id` that exists in BOTH streams. Specifically
# MAGIC ORD-001..ORD-025 each appear once. ORD-026..ORD-030 (no shipment), ORD-200..ORD-201 (no
# MAGIC shipment), ORD-901..ORD-907 (no order, only orphan shipments) do NOT appear.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner-join on `order_id`.
# MAGIC 2. Write to `ex1_target` as Delta.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT add `withWatermark` or a time-range predicate (those come in later exercises).

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex1
# TODO: Inner join orders_stream and shipments_stream on order_id, write to ex1_target

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_target")

assert result.count() == 25, f"Expected 25 joined rows, got {result.count()}"
assert "shipment_id" in result.columns, "Output must include shipment_id from the right side"
assert "order_ts" in result.columns, "Output must include order_ts from the left side"

# Specific in-window pair (ORD-001 paired with SHP-001)
ord1 = result.filter("order_id = 'ORD-001'").collect()
assert len(ord1) == 1, f"Expected 1 row for ORD-001, got {len(ord1)}"
assert ord1[0]["shipment_id"] == "SHP-001", f"ORD-001 should pair with SHP-001, got {ord1[0]['shipment_id']}"

# Specific out-of-window pair (still matched here because no time bound)
ord21 = result.filter("order_id = 'ORD-021'").collect()
assert len(ord21) == 1, "ORD-021 should be matched (no time bound in this exercise)"
assert ord21[0]["shipment_id"] == "SHP-021", "ORD-021 should pair with SHP-021"

# Unmatched orders must not appear
assert result.filter("order_id = 'ORD-026'").count() == 0, "ORD-026 has no shipment, should not appear"
assert result.filter("order_id = 'ORD-200'").count() == 0, "ORD-200 has no shipment, should not appear"
# Orphan shipments must not appear
assert result.filter("order_id = 'ORD-901'").count() == 0, "ORD-901 only exists in shipments, should not appear"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Inner Join with Watermarks on Both Sides
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Add `withWatermark` on both sides, but DO NOT yet add a time-range predicate. For inner
# MAGIC joins, watermarks alone do not change which rows match - they only declare a notion of
# MAGIC event-time progress. Verify the output matches Exercise 1.
# MAGIC
# MAGIC A watermark alone does not bound stream-stream join state. Without a time-range predicate, Spark cannot know how late a match might arrive, so state can still grow without limit.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex2_target`.
# MAGIC
# MAGIC **Expected Output**: 25 rows. Same set as Exercise 1.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `.withWatermark("order_ts", "1 minute")` to the orders stream.
# MAGIC 2. Apply `.withWatermark("shipment_ts", "1 minute")` to the shipments stream.
# MAGIC 3. Inner-join on `order_id`. No additional time-range predicate.
# MAGIC 4. Write to `ex2_target`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Watermark must be applied BEFORE the join (not after).
# MAGIC - Delay string follows the `"<value> <unit>"` format (e.g., `"1 minute"`, `"10 minutes"`).

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex2
# TODO: Inner join with withWatermark on both sides; no time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_target")

assert result.count() == 25, f"Expected 25 joined rows, got {result.count()}"

# In-window pair still matched
ord5 = result.filter("order_id = 'ORD-005'").collect()
assert len(ord5) == 1, "ORD-005 should be matched"
assert ord5[0]["shipment_id"] == "SHP-005", "ORD-005 should pair with SHP-005"

# Out-of-window pair still matched (no time-range predicate)
assert result.filter("order_id = 'ORD-022'").count() == 1, "ORD-022 should match SHP-022 (no time bound)"

# Unmatched and orphan rows still absent
assert result.filter("order_id = 'ORD-030'").count() == 0, "ORD-030 has no shipment"
assert result.filter("order_id = 'ORD-905'").count() == 0, "ORD-905 only exists in shipments"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Inner Join with Watermark + 5-Minute Time-Range Constraint
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Now add the time-range predicate that bounds the join window:
# MAGIC `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC Pairs whose shipment arrives more than 5 minutes after the order are now excluded.
# MAGIC The watermark + the time bound together let Spark drop state past the window.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex3_target`.
# MAGIC
# MAGIC **Expected Output**: 20 rows. Only ORD-001..ORD-020 paired with SHP-001..SHP-020 (their
# MAGIC shipment_ts is order_ts + 2 minutes, well within the 5-minute bound). ORD-021..ORD-025
# MAGIC are matched on `order_id` but their shipment is 10 minutes later, so the time predicate
# MAGIC excludes them. Every row satisfies `shipment_ts <= order_ts + INTERVAL 5 MINUTES`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"` delay on each).
# MAGIC 2. Inner-join on `order_id` AND the time-range predicate
# MAGIC    `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 3. Write to `ex3_target`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - The join condition must include both `order_id` equality AND the time-range predicate.
# MAGIC - You can express the time predicate either via DataFrame expression or via Spark SQL.
# MAGIC - Put the time predicate in the join condition. (In this inner join the optimizer would also move a post-join `.filter` comparing both sides into the join condition, but a left or right outer join gets no such help: Spark rejects it without a time bound in the join.)

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex3
# TODO: Inner join with watermark on both sides AND shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_target")

assert result.count() == 20, f"Expected 20 joined rows (in-window only), got {result.count()}"

# In-window pair present
ord10 = result.filter("order_id = 'ORD-010'").collect()
assert len(ord10) == 1, "ORD-010 should be matched (2-min gap, in window)"
assert ord10[0]["shipment_id"] == "SHP-010", "ORD-010 should pair with SHP-010"

# Out-of-window pairs MUST be excluded
assert result.filter("order_id = 'ORD-021'").count() == 0, "ORD-021 pair is 10 min apart, should be excluded"
assert result.filter("order_id = 'ORD-025'").count() == 0, "ORD-025 pair is 10 min apart, should be excluded"

# Sanity: all 20 in-window order_ids present
in_window_ids = [f"ORD-{i:03d}" for i in range(1, 21)]
present_ids = set(r["order_id"] for r in result.select("order_id").collect())
missing = [oid for oid in in_window_ids if oid not in present_ids]
assert not missing, f"Missing in-window order_ids: {missing}"

# Every row must satisfy the 5-minute time bound
from pyspark.sql.functions import expr
violations = result.filter(
    expr("shipment_ts > order_ts + INTERVAL 5 MINUTES OR shipment_ts < order_ts")
).count()
assert violations == 0, f"Found {violations} rows violating the 5-minute window bound"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Left Outer Join with Watermark + Time Bound
# MAGIC **Difficulty**: Medium | **Time**: ~20 min
# MAGIC
# MAGIC Left outer join orders to shipments with the same time-range predicate as Exercise 3.
# MAGIC Orders with no in-window shipment must appear in the output with NULL shipment fields.
# MAGIC
# MAGIC **When NULL rows appear**: the join tracks one watermark, the minimum of the two sides' watermarks (each side: latest event time minus 1 minute). An unmatched order is emitted with NULL shipment fields once that watermark passes `order_ts + 5 minutes`. Until then, a matching shipment could still arrive. ORD-200 (11:00) and ORD-201 (11:01) move the orders watermark to 11:00. SHP-200 (12:00) and SHP-201 (12:01) move the shipments watermark to 12:00. The join watermark is therefore 11:00. That passes the windows of ORD-021..ORD-030 (which end 10:25..10:34).
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex4_target`.
# MAGIC
# MAGIC **Expected Output**: 30 rows.
# MAGIC - 20 matched pairs (ORD-001..ORD-020 with their shipments).
# MAGIC - 10 unmatched-left rows with NULL shipment fields:
# MAGIC   - ORD-021..ORD-025 (shipment exists but is out of window)
# MAGIC   - ORD-026..ORD-030 (no shipment at all)
# MAGIC
# MAGIC ORD-200 and ORD-201 are NOT emitted in this run because the watermark has not yet passed
# MAGIC their windows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"` delay).
# MAGIC 2. Use `how="leftOuter"` (or `"left_outer"`) in `.join(...)`.
# MAGIC 3. Time-range predicate: `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 4. Write to `ex4_target`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Outer joins REQUIRE both a watermark and a time-range predicate. Spark will throw if
# MAGIC   either is missing.
# MAGIC - Unmatched-left rows must contain NULL for `shipment_id` and `carrier`.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex4
# TODO: Left outer join with watermark + time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_target")

assert result.count() == 30, f"Expected 30 rows (20 matched + 10 unmatched-left), got {result.count()}"

# Matched rows: 20
matched = result.filter("shipment_id IS NOT NULL")
assert matched.count() == 20, f"Expected 20 matched rows, got {matched.count()}"

# Unmatched-left rows: 10
unmatched = result.filter("shipment_id IS NULL")
assert unmatched.count() == 10, f"Expected 10 unmatched-left rows, got {unmatched.count()}"

# Specific matched row
ord15 = result.filter("order_id = 'ORD-015'").collect()
assert len(ord15) == 1, "ORD-015 should appear once"
assert ord15[0]["shipment_id"] == "SHP-015", "ORD-015 should be matched to SHP-015"

# Specific unmatched-left row: ORD-021's shipment is out of window -> NULL right side
ord21 = result.filter("order_id = 'ORD-021'").collect()
assert len(ord21) == 1, "ORD-021 should appear once (unmatched-left)"
assert ord21[0]["shipment_id"] is None, "ORD-021 must have NULL shipment_id (out of window)"

# ORD-028 has no shipment at all
ord28 = result.filter("order_id = 'ORD-028'").collect()
assert len(ord28) == 1, "ORD-028 should appear once (unmatched-left)"
assert ord28[0]["shipment_id"] is None, "ORD-028 must have NULL shipment_id (no shipment)"

# ORD-200 should NOT be emitted yet (watermark hasn't passed its window)
assert result.filter("order_id = 'ORD-200'").count() == 0, \
    "ORD-200 (watermark-advancing) should still be in state, not yet emitted"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Right Outer Join with Watermark + Time Bound
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Right outer join orders to shipments. Shipments with no in-window matching order must
# MAGIC appear with NULL order fields. Same watermark + time bound rules as Exercise 4 - the
# MAGIC requirements for outer joins are symmetric.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Target**: `db_code.stream_stream_joins.ex5_target`.
# MAGIC
# MAGIC **Expected Output**: 30 rows.
# MAGIC - 20 matched pairs (ORD-001..ORD-020 with SHP-001..SHP-020).
# MAGIC - 10 unmatched-right rows with NULL order fields:
# MAGIC   - SHP-021..SHP-025 (matching order exists but the shipment is 10 min after the order,
# MAGIC     outside the 5-min bound)
# MAGIC   - SHP-026..SHP-030 (orphan shipments referencing ORD-901..ORD-905)
# MAGIC
# MAGIC SHP-200 and SHP-201 are NOT emitted in this run (watermark hasn't passed their windows).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Watermark both streams (`"1 minute"`).
# MAGIC 2. Use `how="rightOuter"` (or `"right_outer"`) in `.join(...)`.
# MAGIC 3. Time-range predicate: `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`.
# MAGIC 4. Write to `ex5_target`.
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Unmatched-right rows must contain NULL for `order_id` and `customer_id` from the orders side.
# MAGIC - `shipment_id` is never NULL in `ex5_target`. Take `order_id` from `orders_stream`, so it is NULL on the 10 unmatched-right rows.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex5
# TODO: Right outer join with watermark + time-range predicate

# Your code here


# COMMAND ----------

# Validate Exercise 5
from pyspark.sql.functions import col

result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_target")

assert result.count() == 30, f"Expected 30 rows (20 matched + 10 unmatched-right), got {result.count()}"

# Matched rows: 20 (have non-null order_id from the left side)
matched = result.filter("order_id IS NOT NULL")
assert matched.count() == 20, f"Expected 20 matched rows, got {matched.count()}"

# Unmatched-right rows: 10 (NULL order_id, non-null shipment_id)
unmatched = result.filter("order_id IS NULL")
assert unmatched.count() == 10, f"Expected 10 unmatched-right rows, got {unmatched.count()}"

# Specific matched row
shp7 = result.filter("shipment_id = 'SHP-007'").collect()
assert len(shp7) == 1, "SHP-007 should appear once"
assert shp7[0]["order_id"] == "ORD-007", "SHP-007 should be matched to ORD-007"

# Out-of-window pair: SHP-021 - order exists but is too early relative to shipment
shp21 = result.filter("shipment_id = 'SHP-021'").collect()
assert len(shp21) == 1, "SHP-021 should appear once (unmatched-right)"
assert shp21[0]["order_id"] is None, "SHP-021 must have NULL order_id (out of window)"

# Orphan shipment: SHP-027 references ORD-902 which doesn't exist
shp27 = result.filter("shipment_id = 'SHP-027'").collect()
assert len(shp27) == 1, "SHP-027 should appear once (unmatched-right)"
assert shp27[0]["order_id"] is None, "SHP-027 must have NULL order_id (orphan)"

# Watermark-advancing shipment not yet emitted
assert result.filter("shipment_id = 'SHP-200'").count() == 0, \
    "SHP-200 (watermark-advancing) should still be in state, not yet emitted"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Measure Unbounded Join State
# MAGIC **Difficulty**: Hard | **Time**: ~20 min
# MAGIC
# MAGIC Run the unbounded join from Exercise 1 and the bounded join from Exercise 3 side by side, and measure how many rows each one still holds in state when it stops.
# MAGIC
# MAGIC **Why state grows unboundedly**: Stream-stream joins keep every row received in state until Spark can prove that no future row could match it. Without a watermark, Spark has no notion of event-time progress, so it can never drop any row. Without an event-time constraint in the join condition (a time-range predicate like the one in Exercise 3, or a join on matching event-time windows), even with a watermark Spark cannot know how far apart two matching rows may be. Either piece alone is insufficient: the state is bounded only with BOTH a watermark AND an event-time constraint.
# MAGIC
# MAGIC **What happens in production without the fix**: the join state on the executors grows with every batch, each micro-batch slows as more state is read and snapshotted to the checkpoint, and the executors eventually run out of memory or disk. The fix is what you wrote in Exercise 3: `.withWatermark` on both sides plus a time-range predicate in the join condition.
# MAGIC
# MAGIC `availableNow=True` stops once the source rows are processed, so the growth over time does not show here. What does show is the state each join keeps when it stops: the unbounded join never evicts a row, while the bounded join drops every row whose window the watermark has passed. On a long-running stream with a `processingTime` trigger, the unbounded number keeps climbing batch after batch.
# MAGIC
# MAGIC **Source 1** / **Source 2**: same as Exercise 1.
# MAGIC
# MAGIC **Targets**:
# MAGIC - `db_code.stream_stream_joins.ex6_unbounded_target`: the unbounded join's output
# MAGIC - `db_code.stream_stream_joins.ex6_bounded_target`: the bounded join's output
# MAGIC - `db_code.stream_stream_joins.ex6_state`: 1 row, columns `unbounded_state_rows BIGINT`, `bounded_state_rows BIGINT`
# MAGIC
# MAGIC **Expected Output**: `ex6_unbounded_target` has 25 rows (same set as Exercise 1) and `ex6_bounded_target` has 20 rows (same set as Exercise 3). In `ex6_state`, `unbounded_state_rows` is 64 (all 32 orders and all 32 shipments, none evicted) and `bounded_state_rows` is 4 (the rows the watermark has not passed yet).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. **Unbounded join**: inner-join the two streams on `order_id` with no `withWatermark` and no time-range predicate. Write to `ex6_unbounded_target` with checkpoint `f"{CHECKPOINT_BASE}/ex6_unbounded"`, and keep the `StreamingQuery` that `.toTable(...)` returns.
# MAGIC 2. **Bounded join**: watermark both streams (`"1 minute"`) and inner-join on `order_id` AND `shipment_ts BETWEEN order_ts AND order_ts + INTERVAL 5 MINUTES`. Write to `ex6_bounded_target` with checkpoint `f"{CHECKPOINT_BASE}/ex6_bounded"`, and keep its `StreamingQuery` too.
# MAGIC 3. After each query's `.awaitTermination()`, read `query.lastProgress["stateOperators"][0]["numRowsTotal"]`: the number of rows that join still holds in state.
# MAGIC 4. Write both numbers as one row to `ex6_state` (`unbounded_state_rows`, `bounded_state_rows`).
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Read both numbers from `lastProgress`; do not type them in.
# MAGIC - Both queries must run to completion under `availableNow` without raising.

# COMMAND ----------

# EXERCISE_KEY: stream_stream_ex6
# TODO: Run the unbounded join and the bounded join, read numRowsTotal from each query's
# lastProgress, and write both numbers to ex6_state

# Your code here


# COMMAND ----------

# Validate Exercise 6
unbounded = spark.table(f"{CATALOG}.{SCHEMA}.ex6_unbounded_target")
bounded = spark.table(f"{CATALOG}.{SCHEMA}.ex6_bounded_target")
state = spark.table(f"{CATALOG}.{SCHEMA}.ex6_state")

assert unbounded.count() == 25, f"ex6_unbounded_target should have 25 rows (same set as Exercise 1), got {unbounded.count()}"
assert bounded.count() == 20, f"ex6_bounded_target should have 20 rows (same set as Exercise 3), got {bounded.count()}"

for c in ["unbounded_state_rows", "bounded_state_rows"]:
    assert c in state.columns, f"ex6_state is missing column {c}"
assert state.count() == 1, f"ex6_state should have exactly 1 row, got {state.count()}"

row = state.collect()[0]
unbounded_rows, bounded_rows = row["unbounded_state_rows"], row["bounded_state_rows"]
assert isinstance(unbounded_rows, int) and isinstance(bounded_rows, int), \
    f"Both state counts must be BIGINT values, got {unbounded_rows!r} and {bounded_rows!r}"
assert unbounded_rows == 64, \
    f"The unbounded join should still hold all 64 source rows (32 orders + 32 shipments) in state, got {unbounded_rows}"
assert bounded_rows == 4, \
    f"The bounded join should hold 4 rows in state after the watermark evicts the rest, got {bounded_rows}"

print("Exercise 6 passed!")
print(f"Rows in join state when each query stopped: unbounded {unbounded_rows}, bounded {bounded_rows}.")
