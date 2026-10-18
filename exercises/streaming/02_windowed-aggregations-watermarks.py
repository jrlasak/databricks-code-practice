# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Windowed Aggregations & Watermarks
# MAGIC **Topic**: Streaming | **Exercises**: 9 | **Total Time**: ~120 min
# MAGIC
# MAGIC Practice time-windowed aggregations on event streams: tumbling, sliding, and session windows,
# MAGIC plus watermarks to bound state and control append-mode window closing. Covers the core stateful
# MAGIC pattern for any real-time metric pipeline.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/windowed-aggregations-watermarks-solutions.py` for
# MAGIC hints and answers.
# MAGIC
# MAGIC **Key concepts**:
# MAGIC - `window(event_ts, "5 minutes")` for tumbling, `window(event_ts, "10 minutes", "5 minutes")` for sliding
# MAGIC - `session_window(event_ts, "10 minutes")` for session windows
# MAGIC - `withWatermark("event_ts", "10 minutes")` sets the watermark to the newest `event_ts` seen minus 10 minutes. Under append mode a window is emitted, and its state dropped, once window.end <= watermark; a row that arrives after its window has closed may be dropped.
# MAGIC - Append mode emits only CLOSED windows; complete mode emits the full result table every batch
# MAGIC - `foreachBatch(fn)` calls `fn(batch_df, batch_id)` once per micro-batch; `batch_df` is a plain DataFrame, so you write it with the batch writer (`batch_df.write.mode("overwrite").saveAsTable(...)`)
# MAGIC - Boundaries are start-inclusive, end-exclusive: an event at exactly 10:05:00 belongs to [10:05, 10:10)
# MAGIC
# MAGIC **Source table** (from setup): `db_code.windowed_aggregations.windows_events`
# MAGIC
# MAGIC **Schema** (`windows_events`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | event_id | STRING | EVT-001 .. EVT-049 |
# MAGIC | event_type | STRING | click, view, purchase |
# MAGIC | user_id | STRING | USER-001 .. USER-005 |
# MAGIC | amount | DOUBLE | Event monetary amount |
# MAGIC | event_ts | TIMESTAMP | Event timestamp (2026-03-10) |
# MAGIC | region | STRING | NA or EU |
# MAGIC
# MAGIC **Timing layout** (all 2026-03-10, UTC):
# MAGIC - 48 "main" events span 10:00:00 -> 10:34:00, distributed across seven 5-min windows
# MAGIC - 1 "late" event (EVT-049) at 09:30:00 - isolated 30 min before the main cluster
# MAGIC - Max event_ts = 10:34:00; end-of-stream watermark (10-min delay) = 10:24:00
# MAGIC - Append mode emits a window once `window.end <= watermark`.
# MAGIC   Closed windows at watermark 10:24:00:
# MAGIC   [09:30,09:35), [10:00,10:05), [10:05,10:10), [10:10,10:15), [10:15,10:20) = 5 windows.
# MAGIC   [10:20,10:25) has window.end = 10:25:00 which is NOT <= 10:24:00, so it stays open.
# MAGIC - Open windows [10:20,10:25), [10:25,10:30), [10:30,10:35) are held in state.
# MAGIC - The 49 rows are written in one commit (one file), so `availableNow` reads them in a single micro-batch and the watermark only moves after it: EVT-049 is counted in [09:30,09:35). If the same rows arrived over several commits (a live feed), they would land in several micro-batches and the drop rule in Key concepts would apply; a row behind the watermark is still counted while its window is open.

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/windowed-aggregations-watermarks-setup

# COMMAND ----------
# MAGIC %md
# MAGIC **Setup complete.** Source table `db_code.windowed_aggregations.windows_events` has 49 rows
# MAGIC (48 main events + 1 late event at 09:30:00). All exercise output tables live in
# MAGIC `db_code.windowed_aggregations.exN_output`.
# MAGIC
# MAGIC **Python variables**: the setup defines three variables you can use in your code: `CATALOG = "db_code"`, `SCHEMA = "windowed_aggregations"` and `CHECKPOINT_BASE = "/Volumes/db_code/windowed_aggregations/checkpoints"`. Write tables by their full name (`db_code.windowed_aggregations.exN_output`).
# MAGIC
# MAGIC **Checkpoints**: each stream checkpoints to its own subdirectory, `f"{CHECKPOINT_BASE}/exN"` (Ex 6 uses `ex6_no_watermark` and `ex6_watermark`).
# MAGIC
# MAGIC **Pattern**: Each exercise reads `windows_events` as a stream, applies a windowed
# MAGIC aggregation, and writes the result table. Use `.trigger(availableNow=True)` and
# MAGIC `.awaitTermination()` on every stream so the validate cell runs against the final state.
# MAGIC
# MAGIC **Redo an exercise**: a checkpoint remembers which rows its stream already read, so running a fixed TODO cell again reads nothing new. Re-run the `%run ./setup/windowed-aggregations-watermarks-setup` cell first: it drops every output table and checkpoint of this notebook.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: 5-Minute Tumbling Window Count by event_type
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Count events per `event_type` per 5-minute tumbling window.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows. Seven 5-min windows from 10:00 -> 10:35,
# MAGIC plus one extra window at 09:30 -> 09:35 containing the single late event.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex1_output`. 22 rows - one per
# MAGIC (window, event_type) combination that has at least one event. For [10:00,10:05): click 4, view 3, purchase 2.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read `windows_events` as a stream via `spark.readStream.table(...)`
# MAGIC 2. Group by `window(event_ts, "5 minutes")` and `event_type`
# MAGIC 3. Aggregate as `count(*) AS event_count`
# MAGIC 4. Select `window.start AS window_start`, `window.end AS window_end`, `event_type`, `event_count`
# MAGIC    (flatten the window struct so downstream queries read `window_start` and `window_end` as plain columns)
# MAGIC 5. Use `outputMode("complete")` and `foreachBatch` with a function that overwrites `ex1_output` each batch
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Do NOT add a watermark in this exercise - include ALL events including the late one
# MAGIC - Output columns must be exactly: `window_start`, `window_end`, `event_type`, `event_count`

# COMMAND ----------

# EXERCISE_KEY: windows_ex1
# TODO: 5-minute tumbling window count per event_type, write flattened result to ex1_output

# Your code here


# COMMAND ----------

# Validate Exercise 1
from pyspark.sql import functions as F

result = spark.table(f"{CATALOG}.{SCHEMA}.ex1_output")

assert result.count() == 22, f"Expected 22 rows (7 main windows x 3 event_types + 1 late click row), got {result.count()}"
for col in ["window_start", "window_end", "event_type", "event_count"]:
    assert col in result.columns, f"Missing {col} column"

# Window [10:00, 10:05): click=4, view=3, purchase=2
w1_click = result.filter(
    "window_start = TIMESTAMP'2026-03-10 10:00:00' AND event_type = 'click'"
).select("event_count").collect()[0][0]
assert w1_click == 4, f"Window [10:00,10:05) click count should be 4, got {w1_click}"

w1_view = result.filter(
    "window_start = TIMESTAMP'2026-03-10 10:00:00' AND event_type = 'view'"
).select("event_count").collect()[0][0]
assert w1_view == 3, f"Window [10:00,10:05) view count should be 3, got {w1_view}"

# Window [10:10, 10:15): view=4 (largest view bucket)
w3_view = result.filter(
    "window_start = TIMESTAMP'2026-03-10 10:10:00' AND event_type = 'view'"
).select("event_count").collect()[0][0]
assert w3_view == 4, f"Window [10:10,10:15) view count should be 4, got {w3_view}"

# Late event window [09:30, 09:35) contains EVT-049 only (click, 1 row in output)
late = result.filter("window_start = TIMESTAMP'2026-03-10 09:30:00'")
assert late.count() == 1, f"Late-event window should have 1 row (click), got {late.count()}"
assert late.collect()[0]["event_type"] == "click", "Late event in [09:30,09:35) should be a click"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: 5-Minute Tumbling Window Sum of amount by user_id
# MAGIC **Difficulty**: Easy | **Time**: ~10 min
# MAGIC
# MAGIC Sum `amount` per `user_id` per 5-minute tumbling window.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex2_output`. 27 rows, one per (window, user_id) pair that has at least one event. USER-001 in [10:00,10:05) totals 145.0; USER-005's only row is [09:30,09:35) with 99.0.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read the source as a stream
# MAGIC 2. Group by `window(event_ts, "5 minutes")` and `user_id`
# MAGIC 3. Aggregate as `sum(amount) AS total_amount`
# MAGIC 4. Select `window.start AS window_start`, `window.end AS window_end`, `user_id`, `total_amount`
# MAGIC 5. Use `outputMode("complete")` with a `foreachBatch` overwrite
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No watermark - include the late event
# MAGIC - Output columns must be exactly: `window_start`, `window_end`, `user_id`, `total_amount`

# COMMAND ----------

# EXERCISE_KEY: windows_ex2
# TODO: 5-minute tumbling window sum of amount per user_id, flattened

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.ex2_output")

for col in ["window_start", "window_end", "user_id", "total_amount"]:
    assert col in result.columns, f"Missing {col} column"

# Window [10:00, 10:05) USER-001: EVT-001(10) + EVT-003(30) + EVT-006(5) + EVT-008(100) = 145
u1_w1 = result.filter(
    "window_start = TIMESTAMP'2026-03-10 10:00:00' AND user_id = 'USER-001'"
).select("total_amount").collect()[0][0]
assert u1_w1 == 145.00, f"USER-001 [10:00,10:05) total should be 145.00, got {u1_w1}"

# Window [10:00, 10:05) USER-002: EVT-002(20) + EVT-005(5) + EVT-009(200) = 225
u2_w1 = result.filter(
    "window_start = TIMESTAMP'2026-03-10 10:00:00' AND user_id = 'USER-002'"
).select("total_amount").collect()[0][0]
assert u2_w1 == 225.00, f"USER-002 [10:00,10:05) total should be 225.00, got {u2_w1}"

# Late window [09:30, 09:35) USER-005: EVT-049 amount 99.00
u5_late = result.filter(
    "window_start = TIMESTAMP'2026-03-10 09:30:00' AND user_id = 'USER-005'"
).select("total_amount").collect()[0][0]
assert u5_late == 99.00, f"USER-005 [09:30,09:35) total should be 99.00 (late event), got {u5_late}"

# Row count: count distinct (window, user_id) pairs that actually have events.
# Expected pairs per window:
#  [09:30,09:35): USER-005 -> 1
#  [10:00,10:05): USER-001, USER-002, USER-003 -> 3
#  [10:05,10:10): USER-001, USER-002, USER-003 -> 3
#  [10:10,10:15): USER-001, USER-002, USER-003, USER-004 -> 4
#  [10:15,10:20): USER-001, USER-002, USER-003, USER-004 -> 4
#  [10:20,10:25): USER-001, USER-002, USER-003, USER-004 -> 4
#  [10:25,10:30): USER-001, USER-002, USER-003, USER-004 -> 4
#  [10:30,10:35): USER-001, USER-002, USER-003, USER-004 -> 4
# Total = 27 rows
assert result.count() == 27, f"Expected 27 (window, user_id) rows, got {result.count()}"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: 10-Minute Sliding Window with 5-Minute Slide
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Compute event counts using a 10-minute sliding window that slides every 5 minutes.
# MAGIC Each event appears in exactly TWO overlapping windows (10-minute window / 5-minute slide = 2).
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex3_output`. Columns: `window_start`, `window_end`, `event_count`. 10 rows: 8 windows starting 09:55, 10:00, ..., 10:30, plus [09:25,09:35) and [09:30,09:40) with 1 event each.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `window(event_ts, "10 minutes", "5 minutes")` - 10-minute window, 5-minute slide
# MAGIC 2. Group by the window only (no other key)
# MAGIC 3. Aggregate as `count(*) AS event_count`
# MAGIC 4. Flatten to `window_start`, `window_end`, `event_count`
# MAGIC 5. Use `outputMode("complete")` with a `foreachBatch` overwrite
# MAGIC
# MAGIC **Expected Output**:
# MAGIC - Window [09:55,10:05) contains only the 9 events in [10:00,10:05) (no events fall in [09:55,10:00)).
# MAGIC - The window starting at 10:00 (covering 10:00 -> 10:10) contains all events in [10:00, 10:10) = 9 + 6 = 15 events
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No watermark - include the late event

# COMMAND ----------

# EXERCISE_KEY: windows_ex3
# TODO: 10-min sliding window with 5-min slide, count(*) per window

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.ex3_output")

for col in ["window_start", "window_end", "event_count"]:
    assert col in result.columns, f"Missing {col} column"

# Window starting 10:00:00 covers [10:00, 10:10) = 9 + 6 = 15 events
w_10_00 = result.filter("window_start = TIMESTAMP'2026-03-10 10:00:00'").collect()[0]
assert w_10_00.event_count == 15, f"Window [10:00,10:10) count should be 15, got {w_10_00.event_count}"
# window end = start + 10 min
assert str(w_10_00.window_end) == "2026-03-10 10:10:00", \
    f"Window starting 10:00 should end at 10:10:00, got {w_10_00.window_end}"

# Window starting 10:05:00 covers [10:05, 10:15) = 6 + 8 = 14 events
w_10_05 = result.filter("window_start = TIMESTAMP'2026-03-10 10:05:00'").collect()[0]
assert w_10_05.event_count == 14, f"Window [10:05,10:15) count should be 14, got {w_10_05.event_count}"

# Window starting 10:10:00 covers [10:10, 10:20) = 8 + 7 = 15 events
w_10_10 = result.filter("window_start = TIMESTAMP'2026-03-10 10:10:00'").collect()[0]
assert w_10_10.event_count == 15, f"Window [10:10,10:20) count should be 15, got {w_10_10.event_count}"

# Window starting 09:55:00 (overlap edge): covers [09:55, 10:05) = 9 events
w_09_55 = result.filter("window_start = TIMESTAMP'2026-03-10 09:55:00'").collect()[0]
assert w_09_55.event_count == 9, f"Window [09:55,10:05) count should be 9, got {w_09_55.event_count}"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Session Window with 10-Minute Inactivity Gap
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Group consecutive events per `user_id` into sessions using a 10-minute inactivity gap.
# MAGIC Use Spark's `session_window` function. A 10-minute gap is wide enough that each user's
# MAGIC main-window events form a single session, while USER-005's lone late event at 09:30:00
# MAGIC is naturally isolated as its own session.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows. Per user totals and session shape:
# MAGIC - USER-001: 19 events, max consecutive gap = 3.5 min -> 1 session
# MAGIC - USER-002: 13 events, max consecutive gap = 4 min -> 1 session
# MAGIC - USER-003: 9 events, max consecutive gap = 6 min -> 1 session
# MAGIC - USER-004: 7 events, max consecutive gap = 6 min -> 1 session
# MAGIC - USER-005: 1 late event at 09:30 -> 1 session of size 1
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex4_output`. 5 rows (one session per user).
# MAGIC Columns: `user_id`, `session_start`, `session_end`, `event_count`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Use `session_window(event_ts, "10 minutes")` - 10-minute inactivity gap
# MAGIC 2. Group by `user_id` and the session_window
# MAGIC 3. Aggregate as `count(*) AS event_count`
# MAGIC 4. Flatten: `user_id`, `session_window.start AS session_start`, `session_window.end AS session_end`, `event_count`
# MAGIC 5. Use `outputMode("complete")` with a `foreachBatch` overwrite
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No watermark - session_window in complete mode does not require a watermark
# MAGIC - Output must have exactly 5 rows (one per user)

# COMMAND ----------

# EXERCISE_KEY: windows_ex4
# TODO: Session window grouped by user_id with 10-minute inactivity gap

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.ex4_output")

assert result.count() == 5, f"Expected 5 rows (one session per user), got {result.count()}"
for col in ["user_id", "session_start", "session_end", "event_count"]:
    assert col in result.columns, f"Missing {col} column"

# USER-001: 19 events in a single session
u1 = result.filter("user_id = 'USER-001'").collect()[0]
assert u1.event_count == 19, f"USER-001 should have 19 events in 1 session, got {u1.event_count}"
assert str(u1.session_start) == "2026-03-10 10:00:00", \
    f"USER-001 session_start should be 10:00:00, got {u1.session_start}"

# USER-005: only the late event - 1 session of size 1, starting at 09:30:00
u5 = result.filter("user_id = 'USER-005'").collect()[0]
assert u5.event_count == 1, f"USER-005 should have 1 event, got {u5.event_count}"
assert str(u5.session_start) == "2026-03-10 09:30:00", \
    f"USER-005 session_start should be 09:30:00, got {u5.session_start}"

# Every user should have exactly 1 session
for u in ["USER-001", "USER-002", "USER-003", "USER-004", "USER-005"]:
    cnt = result.filter(f"user_id = '{u}'").count()
    assert cnt == 1, f"{u} should have exactly 1 session row, got {cnt}"

# Sum of event_count across all sessions must equal 49 (the full source)
from pyspark.sql import functions as _F
total = result.agg(_F.sum("event_count")).collect()[0][0]
assert total == 49, f"Sum of session event_counts should be 49 (all source rows), got {total}"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Watermark + Append Mode Emits Only Closed Windows
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Add `withWatermark("event_ts", "10 minutes")` to a 5-min tumbling-window count of all events.
# MAGIC Use **append output mode** so only CLOSED windows are emitted.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC - Max event_ts = 10:34:00, so end-of-stream watermark (with 10-min delay) = 10:24:00
# MAGIC - Windows whose `window.end` <= 10:24:00 are closed and emitted; later windows stay open
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex5_output`. One row per closed tumbling window. [10:00,10:05) has 9 events; [09:30,09:35) has 1.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Read the source as a stream via `spark.readStream.table(...)`
# MAGIC 2. Apply `withWatermark("event_ts", "10 minutes")` on the streaming DataFrame BEFORE the
# MAGIC    aggregation
# MAGIC 3. Group by `window(event_ts, "5 minutes")` only (no other key); aggregate `count(*) AS event_count`
# MAGIC 4. Use `outputMode("append")`
# MAGIC 5. Flatten: `window_start`, `window_end`, `event_count`
# MAGIC 6. Write directly with `.toTable(...)` (no foreachBatch needed)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Expected rows: 5, one per closed window: [09:30,09:35), [10:00,10:05), [10:05,10:10), [10:10,10:15), [10:15,10:20). [10:20,10:25), [10:25,10:30) and [10:30,10:35) stay open.

# COMMAND ----------

# EXERCISE_KEY: windows_ex5
# TODO: Tumbling-window count with 10-min watermark, append mode, direct toTable write

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.ex5_output")

for col in ["window_start", "window_end", "event_count"]:
    assert col in result.columns, f"Missing {col} column"

# Closed windows under Spark's append-mode emission rule (window.end <= watermark) at
# watermark 10:24:00:
#  [09:30,09:35): 1 event (EVT-049, the late event - window closed since 09:35 <= 10:24)
#  [10:00,10:05): 9 events
#  [10:05,10:10): 6 events
#  [10:10,10:15): 8 events
#  [10:15,10:20): 7 events
# Expect 5 closed window rows. [10:20,10:25) is still OPEN (end=10:25 not <= watermark=10:24).
assert result.count() == 5, f"Expected 5 closed windows in append mode, got {result.count()}"

# Open windows MUST NOT be present (window_end > watermark)
open_w = result.filter("window_start >= TIMESTAMP'2026-03-10 10:20:00'")
assert open_w.count() == 0, f"Open windows (start >= 10:20) must not be emitted, got {open_w.count()}"

# Specific closed-window count
w1 = result.filter("window_start = TIMESTAMP'2026-03-10 10:00:00'").collect()[0]
assert w1.event_count == 9, f"Window [10:00,10:05) count should be 9, got {w1.event_count}"

# Late window [09:30, 09:35) IS emitted as a closed window (window_end <= watermark)
late = result.filter("window_start = TIMESTAMP'2026-03-10 09:30:00'").collect()[0]
assert late.event_count == 1, f"Late window [09:30,09:35) should contain 1 event, got {late.event_count}"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Same Query With and Without a Watermark
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Run the SAME 5-min tumbling event count twice and write to TWO tables:
# MAGIC 1. `ex6_output_no_watermark` - no watermark, complete mode (via foreachBatch overwrite). Includes
# MAGIC    EVERY window (closed AND open) - complete mode emits the full result table each batch.
# MAGIC 2. `ex6_output` - with `withWatermark("event_ts", "10 minutes")`, append mode (via `.toTable`).
# MAGIC    Only CLOSED windows emitted - open windows (window_end > watermark) stay in state.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC
# MAGIC **Target tables**:
# MAGIC - `db_code.windowed_aggregations.ex6_output_no_watermark` (complete mode, no watermark): 8 windows
# MAGIC - `db_code.windowed_aggregations.ex6_output` (append mode + 10-min watermark): 5 closed windows
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Write the no-watermark stream first: `foreachBatch` overwrite, complete mode, no watermark
# MAGIC 2. Write the watermark stream second: append mode + 10-min watermark, direct `.toTable()`
# MAGIC 3. Both streams group by `window(event_ts, "5 minutes")` only and compute `count(*) AS event_count`
# MAGIC 4. Flatten to `window_start`, `window_end`, `event_count` in BOTH outputs
# MAGIC 5. Use DIFFERENT checkpoint paths for the two streams (`ex6_no_watermark` and `ex6_watermark`)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - No-watermark output (complete mode) must contain ALL 8 windows including [09:30,09:35)
# MAGIC   and the 3 windows starting at 10:20, 10:25, 10:30
# MAGIC - Watermark output (append mode) must contain ONLY the 5 closed windows, [09:30,09:35) and the four from [10:00,10:05) to [10:15,10:20); the 3 open windows MUST be absent
# MAGIC - The diff between the two output row counts (8 - 5 = 3) equals the number of still-open
# MAGIC   windows held in state at end-of-stream

# COMMAND ----------

# EXERCISE_KEY: windows_ex6
# TODO: Run the same tumbling-count query twice - once without watermark (complete mode),
# TODO: once with 10-min watermark (append mode). Write to two separate tables.

# Your code here


# COMMAND ----------

# Validate Exercise 6
no_watermark = spark.table(f"{CATALOG}.{SCHEMA}.ex6_output_no_watermark")
with_wm = spark.table(f"{CATALOG}.{SCHEMA}.ex6_output")

# No-watermark: all 8 windows (including the late one and the 3 still-open windows)
assert no_watermark.count() == 8, f"no_watermark output should have 8 windows, got {no_watermark.count()}"
late_in_no_watermark = no_watermark.filter("window_start = TIMESTAMP'2026-03-10 09:30:00'")
assert late_in_no_watermark.count() == 1, "no_watermark output MUST include the late window [09:30,09:35)"
assert late_in_no_watermark.collect()[0]["event_count"] == 1, "Late window count should be 1 (EVT-049)"

# Watermark + append mode: only CLOSED windows (window.end <= watermark).
# Closed: [09:30,09:35), [10:00,10:05), [10:05,10:10), [10:10,10:15), [10:15,10:20) = 5
# [10:20,10:25) has window.end=10:25:00 which is NOT <= watermark 10:24:00, so it stays open.
assert with_wm.count() == 5, f"watermark output should have 5 closed windows, got {with_wm.count()}"

# Open windows (window_end > watermark) MUST be absent from the append-mode output
open_in_wm = with_wm.filter("window_start >= TIMESTAMP'2026-03-10 10:20:00'")
assert open_in_wm.count() == 0, f"watermark output MUST NOT include open windows, got {open_in_wm.count()}"

# The diff is exactly 3 windows: the 3 still-open windows held in state
diff = no_watermark.count() - with_wm.count()
assert diff == 3, f"Expected difference of 3 (open windows held in state), got {diff}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: Watermarked Aggregation Grouped by Window + user_id
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Build a watermark-aware aggregation grouped by `(window, user_id)`. Only closed windows (window.end <= watermark) reach the output. Watermark + append mode is the standard production pattern for stateful streaming.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex7_output`. 15 rows, one per (closed window, user_id) pair. Columns: `window_start`, `window_end`, `user_id`, `event_count`, `total_amount`. USER-001 in [10:00,10:05): `event_count` 4, `total_amount` 145.0. USER-005 appears once, in [09:30,09:35), with 1 event and 99.0.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `withWatermark("event_ts", "10 minutes")` to the streaming source
# MAGIC 2. Group by `window(event_ts, "5 minutes")` AND `user_id`
# MAGIC 3. Aggregate `count(*) AS event_count` and `sum(amount) AS total_amount`
# MAGIC 4. Output mode = `append`; flatten the window struct to `window.start AS window_start`, `window.end AS window_end`
# MAGIC 5. Write to `ex7_output` with `.toTable(...)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Output covers only the 5 closed windows: [09:30,09:35), [10:00,10:05), [10:05,10:10), [10:10,10:15), [10:15,10:20)
# MAGIC - Open windows (window_start >= 10:20:00) MUST NOT appear

# COMMAND ----------

# EXERCISE_KEY: windows_ex7
# TODO: Watermarked aggregation grouped by window + user_id, append mode, direct toTable

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.ex7_output")

for col in ["window_start", "window_end", "user_id", "event_count", "total_amount"]:
    assert col in result.columns, f"Missing {col} column"

# Open windows (window_start >= 10:20:00) MUST NOT appear in append-mode output
open_w = result.filter("window_start >= TIMESTAMP'2026-03-10 10:20:00'")
assert open_w.count() == 0, f"Open windows must NOT be in append-mode output, got {open_w.count()}"

# Closed windows under append-mode emission: 5 distinct closed windows
distinct_windows = result.select("window_start").distinct().count()
assert distinct_windows == 5, f"Expected 5 distinct closed windows, got {distinct_windows}"

# USER-005 (late event at 09:30) IS present in [09:30,09:35) - that window is closed
u5 = result.filter("user_id = 'USER-005'").collect()
assert len(u5) == 1, f"USER-005 should appear once in closed window [09:30,09:35), got {len(u5)} rows"
assert u5[0].event_count == 1, f"USER-005 should have 1 event, got {u5[0].event_count}"
assert u5[0].total_amount == 99.00, f"USER-005 total should be 99.00 (EVT-049), got {u5[0].total_amount}"

# USER-001 in [10:00,10:05): EVT-001(10) + EVT-003(30) + EVT-006(5) + EVT-008(100) = 145, 4 events
u1_w1 = result.filter(
    "user_id = 'USER-001' AND window_start = TIMESTAMP'2026-03-10 10:00:00'"
).collect()[0]
assert u1_w1.event_count == 4, f"USER-001 [10:00,10:05) count should be 4, got {u1_w1.event_count}"
assert u1_w1.total_amount == 145.00, f"USER-001 [10:00,10:05) sum should be 145.00, got {u1_w1.total_amount}"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: Append Mode Requires Watermark and Emits Only Closed Windows
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Write a tumbling-window sum of `amount` in **append mode** with a watermark. Verify that
# MAGIC open windows are absent from the output (they would be present in complete mode).
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex8_output`. Columns: `window_start`,
# MAGIC `window_end`, `total_amount`. 5 closed windows: [09:30,09:35) 99.0, [10:00,10:05) 415.0, [10:05,10:10) 265.0, plus [10:10,10:15) and [10:15,10:20).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `withWatermark("event_ts", "10 minutes")`
# MAGIC 2. Group by `window(event_ts, "5 minutes")` only, aggregate `sum(amount) AS total_amount`
# MAGIC 3. Flatten the window struct
# MAGIC 4. Use `outputMode("append")` and `.toTable(...)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Append mode WILL FAIL without a watermark - watermark is mandatory for stateful aggregations
# MAGIC   in append mode
# MAGIC - Output must NOT contain any window with window_start >= 10:20:00 (those are still open;
# MAGIC   their window.end is NOT <= watermark 10:24:00)

# COMMAND ----------

# EXERCISE_KEY: windows_ex8
# TODO: Append-mode tumbling sum with watermark, only closed windows emitted

# Your code here


# COMMAND ----------

# Validate Exercise 8
result = spark.table(f"{CATALOG}.{SCHEMA}.ex8_output")

for col in ["window_start", "window_end", "total_amount"]:
    assert col in result.columns, f"Missing {col} column"

# 5 closed windows present (no open windows). Late window [09:30,09:35) is closed and included.
assert result.count() == 5, f"Expected 5 closed windows in append mode, got {result.count()}"

# All open windows must be absent
open_w = result.filter("window_start >= TIMESTAMP'2026-03-10 10:20:00'")
assert open_w.count() == 0, f"Open windows must NOT be in append-mode output, got {open_w.count()}"

# Late window [09:30, 09:35) IS present (closed window, contains EVT-049 at amount=99.00)
late = result.filter("window_start = TIMESTAMP'2026-03-10 09:30:00'").collect()[0]
assert late.total_amount == 99.00, f"Late window [09:30,09:35) total_amount should be 99.00, got {late.total_amount}"

# Window [10:00, 10:05) total_amount:
#   USER-001: 10+30+5+100=145; USER-002: 20+5+200=225; USER-003: 40+5=45
#   Total = 145 + 225 + 45 = 415
w1 = result.filter("window_start = TIMESTAMP'2026-03-10 10:00:00'").collect()[0]
assert w1.total_amount == 415.00, f"[10:00,10:05) total_amount should be 415.00, got {w1.total_amount}"

# Window [10:05, 10:10) total_amount:
#   USER-001 (EVT-011 35, EVT-013 5, EVT-015 150) = 190
#   USER-002 (EVT-010 25, EVT-014 5) = 30
#   USER-003 (EVT-012 45) = 45
#   Total = 190 + 30 + 45 = 265
w2 = result.filter("window_start = TIMESTAMP'2026-03-10 10:05:00'").collect()[0]
assert w2.total_amount == 265.00, f"[10:05,10:10) total_amount should be 265.00, got {w2.total_amount}"

print("Exercise 8 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: Tumbling Window Grouped by Two Keys with Watermark
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Count events per `event_type` and `region` in each 5-min tumbling window, with a watermark so append mode can emit closed windows.
# MAGIC
# MAGIC **Source** (`db_code.windowed_aggregations.windows_events`): 49 rows. `region` is NA or EU.
# MAGIC
# MAGIC **Target**: Write to `db_code.windowed_aggregations.ex9_output`. Columns: `window_start`,
# MAGIC `window_end`, `event_type`, `region`, `event_count`. Append mode -> only closed windows.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Apply `withWatermark("event_ts", "10 minutes")`
# MAGIC 2. Group by `window(event_ts, "5 minutes")`, `event_type`, `region`
# MAGIC 3. Aggregate `count(*) AS event_count`
# MAGIC 4. Flatten the window struct
# MAGIC 5. Use `outputMode("append")` and write with `.toTable(...)`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Output covers only the closed windows [09:30,09:35) and [10:00,10:05) to [10:15,10:20), 5 in total
# MAGIC - Open windows (window_start >= 10:20:00) must be absent (window.end > watermark 10:24:00)
# MAGIC - Expected: 24 rows. [10:00,10:05) has 6 cells: (click, NA) 3, (click, EU) 1, (view, NA) 2, (view, EU) 1, (purchase, NA) 1, (purchase, EU) 1. [09:30,09:35) has one cell: (click, NA) 1.

# COMMAND ----------

# EXERCISE_KEY: windows_ex9
# TODO: Tumbling window grouped by event_type + region, watermark + append mode

# Your code here


# COMMAND ----------

# Validate Exercise 9
result = spark.table(f"{CATALOG}.{SCHEMA}.ex9_output")

for col in ["window_start", "window_end", "event_type", "region", "event_count"]:
    assert col in result.columns, f"Missing {col} column"

# Open windows (window_start >= 10:20:00) must be absent (still in state)
open_w = result.filter("window_start >= TIMESTAMP'2026-03-10 10:20:00'")
assert open_w.count() == 0, f"Open windows must NOT be in append-mode output, got {open_w.count()}"

# Closed windows: 5 distinct window_start values (including the late event's window [09:30,09:35))
distinct_windows = result.select("window_start").distinct().count()
assert distinct_windows == 5, f"Expected 5 closed distinct windows, got {distinct_windows}"

# Late window [09:30,09:35) IS present (single cell: click, NA from EVT-049)
late_cell = result.filter(
    "window_start = TIMESTAMP'2026-03-10 09:30:00' AND event_type = 'click' AND region = 'NA'"
).collect()
assert len(late_cell) == 1 and late_cell[0]["event_count"] == 1, \
    f"Late window should have 1 (click,NA) cell with count 1, got {late_cell}"

# Window [10:00, 10:05) breakdown:
#   (click, NA): EVT-001, 003, 004 = 3
#   (click, EU): EVT-002 = 1
#   (view, NA): EVT-006, 007 = 2
#   (view, EU): EVT-005 = 1
#   (purchase, NA): EVT-008 = 1
#   (purchase, EU): EVT-009 = 1
w_str = "TIMESTAMP'2026-03-10 10:00:00'"
click_na = result.filter(
    f"window_start = {w_str} AND event_type = 'click' AND region = 'NA'"
).collect()[0]["event_count"]
assert click_na == 3, f"[10:00,10:05) (click,NA) should be 3, got {click_na}"

click_eu = result.filter(
    f"window_start = {w_str} AND event_type = 'click' AND region = 'EU'"
).collect()[0]["event_count"]
assert click_eu == 1, f"[10:00,10:05) (click,EU) should be 1, got {click_eu}"

view_na = result.filter(
    f"window_start = {w_str} AND event_type = 'view' AND region = 'NA'"
).collect()[0]["event_count"]
assert view_na == 2, f"[10:00,10:05) (view,NA) should be 2, got {view_na}"

purchase_eu = result.filter(
    f"window_start = {w_str} AND event_type = 'purchase' AND region = 'EU'"
).collect()[0]["event_count"]
assert purchase_eu == 1, f"[10:00,10:05) (purchase,EU) should be 1, got {purchase_eu}"

# Window [10:00, 10:05) should have exactly 6 (event_type, region) cells
w1_cells = result.filter(f"window_start = {w_str}").count()
assert w1_cells == 6, f"[10:00,10:05) should have 6 (event_type,region) cells, got {w1_cells}"

print("Exercise 9 passed!")
