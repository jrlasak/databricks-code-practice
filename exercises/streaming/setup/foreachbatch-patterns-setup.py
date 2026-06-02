# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: foreachBatch Patterns
# MAGIC Creates per-exercise streaming source tables, target tables, a metrics table, and a
# MAGIC quarantine table for the `foreachbatch_patterns` schema. Each exercise gets its own
# MAGIC isolated `exN_source` / `exN_target` pair so streams cannot interfere with each other.
# MAGIC Checkpoints live under a Unity Catalog volume so streams are restartable.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "foreachbatch_patterns"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BASE_SCHEMA}")

# Volume for checkpoint locations (one subdirectory per exercise)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Clean previous runs --
# Drop per-exercise source, target, metrics, and quarantine tables and wipe checkpoints
# so the notebook is fully idempotent. The base streaming schema is left alone.
for i in range(1, 8):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Exercise-specific extra tables
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex3_target_raw")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex3_target_clean")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex5_quarantine")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_metrics")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex7_quarantine")

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise streaming source tables
# MAGIC Each `exN_source` is a Delta table with ~40 events. Rows include intentional edge
# MAGIC cases (negative `amount`, NULL `user_id`) used by the quality-routing exercises.
# MAGIC
# MAGIC Schema:
# MAGIC | Column     | Type      | Notes |
# MAGIC |------------|-----------|-------|
# MAGIC | event_id   | STRING    | Primary key (unique within source) |
# MAGIC | user_id    | STRING    | Nullable; ~2 rows have NULL |
# MAGIC | event_type | STRING    | purchase / view / click |
# MAGIC | amount     | DOUBLE    | Nullable; ~2 rows are negative (bad data) |
# MAGIC | event_ts   | TIMESTAMP | Event timestamp |

# COMMAND ----------

# Canonical event dataset for the topic.
# 40 rows. 36 "good" rows + 4 "bad" rows for the quality-routing exercises:
#   - 2 rows with negative amount (EV-901, EV-902)
#   - 2 rows with NULL user_id (EV-903, EV-904)
events_rows = """
    ('EV-301', 'purchase', 'U-1',  50.00,  TIMESTAMP '2026-05-01 10:00:00'),
    ('EV-302', 'view',     'U-2',  0.00,   TIMESTAMP '2026-05-01 10:01:00'),
    ('EV-303', 'purchase', 'U-3',  120.50, TIMESTAMP '2026-05-01 10:02:00'),
    ('EV-304', 'click',    'U-1',  0.00,   TIMESTAMP '2026-05-01 10:03:00'),
    ('EV-305', 'purchase', 'U-2',  75.25,  TIMESTAMP '2026-05-01 10:04:00'),
    ('EV-306', 'view',     'U-4',  0.00,   TIMESTAMP '2026-05-01 10:05:00'),
    ('EV-307', 'purchase', 'U-3',  200.00, TIMESTAMP '2026-05-01 10:06:00'),
    ('EV-308', 'click',    'U-5',  0.00,   TIMESTAMP '2026-05-01 10:07:00'),
    ('EV-309', 'purchase', 'U-1',  45.00,  TIMESTAMP '2026-05-01 10:08:00'),
    ('EV-310', 'view',     'U-2',  0.00,   TIMESTAMP '2026-05-01 10:09:00'),
    ('EV-311', 'purchase', 'U-4',  310.75, TIMESTAMP '2026-05-01 10:10:00'),
    ('EV-312', 'click',    'U-3',  0.00,   TIMESTAMP '2026-05-01 10:11:00'),
    ('EV-313', 'purchase', 'U-5',  89.99,  TIMESTAMP '2026-05-01 10:12:00'),
    ('EV-314', 'view',     'U-1',  0.00,   TIMESTAMP '2026-05-01 10:13:00'),
    ('EV-315', 'purchase', 'U-2',  125.00, TIMESTAMP '2026-05-01 10:14:00'),
    ('EV-316', 'click',    'U-4',  0.00,   TIMESTAMP '2026-05-01 10:15:00'),
    ('EV-317', 'purchase', 'U-3',  60.00,  TIMESTAMP '2026-05-01 10:16:00'),
    ('EV-318', 'view',     'U-5',  0.00,   TIMESTAMP '2026-05-01 10:17:00'),
    ('EV-319', 'purchase', 'U-1',  175.50, TIMESTAMP '2026-05-01 10:18:00'),
    ('EV-320', 'click',    'U-2',  0.00,   TIMESTAMP '2026-05-01 10:19:00'),
    ('EV-321', 'purchase', 'U-4',  99.50,  TIMESTAMP '2026-05-01 10:20:00'),
    ('EV-322', 'view',     'U-3',  0.00,   TIMESTAMP '2026-05-01 10:21:00'),
    ('EV-323', 'purchase', 'U-5',  220.00, TIMESTAMP '2026-05-01 10:22:00'),
    ('EV-324', 'click',    'U-1',  0.00,   TIMESTAMP '2026-05-01 10:23:00'),
    ('EV-325', 'purchase', 'U-2',  30.00,  TIMESTAMP '2026-05-01 10:24:00'),
    ('EV-326', 'view',     'U-4',  0.00,   TIMESTAMP '2026-05-01 10:25:00'),
    ('EV-327', 'purchase', 'U-3',  410.00, TIMESTAMP '2026-05-01 10:26:00'),
    ('EV-328', 'click',    'U-5',  0.00,   TIMESTAMP '2026-05-01 10:27:00'),
    ('EV-329', 'purchase', 'U-1',  15.50,  TIMESTAMP '2026-05-01 10:28:00'),
    ('EV-330', 'view',     'U-2',  0.00,   TIMESTAMP '2026-05-01 10:29:00'),
    ('EV-331', 'purchase', 'U-4',  270.00, TIMESTAMP '2026-05-01 10:30:00'),
    ('EV-332', 'click',    'U-3',  0.00,   TIMESTAMP '2026-05-01 10:31:00'),
    ('EV-333', 'purchase', 'U-5',  55.00,  TIMESTAMP '2026-05-01 10:32:00'),
    ('EV-334', 'view',     'U-1',  0.00,   TIMESTAMP '2026-05-01 10:33:00'),
    ('EV-335', 'purchase', 'U-2',  145.75, TIMESTAMP '2026-05-01 10:34:00'),
    ('EV-336', 'purchase', 'U-3',  88.00,  TIMESTAMP '2026-05-01 10:35:00'),
    -- "Bad" rows for quality routing (filter target: amount >= 0 AND user_id IS NOT NULL)
    ('EV-901', 'purchase', 'U-1',  -10.00, TIMESTAMP '2026-05-01 11:00:00'),
    ('EV-902', 'purchase', 'U-4',  -25.50, TIMESTAMP '2026-05-01 11:01:00'),
    ('EV-903', 'purchase', NULL,   50.00,  TIMESTAMP '2026-05-01 11:02:00'),
    ('EV-904', 'view',     NULL,   0.00,   TIMESTAMP '2026-05-01 11:03:00')
"""

def create_events_source(table_name: str):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{table_name} (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            amount DOUBLE,
            event_ts TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.{table_name} VALUES {events_rows}
    """)

# Exercises 1-7 all stream from a copy of the canonical 40-row source.
# Each gets its own copy so simultaneous reruns of the notebook don't cause read-write
# conflicts on a shared source.
for ex in [1, 2, 3, 4, 5, 6, 7]:
    create_events_source(f"ex{ex}_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2 + 4: pre-existing target table for MERGE upsert exercises
# MAGIC `ex2_target` has 10 rows. Streaming events for matching `event_id`s will UPDATE
# MAGIC the existing rows; new `event_id`s will INSERT.

# COMMAND ----------

# Target for Exercise 2 (foreachBatch + MERGE).
# 10 rows pre-populated. Streaming source has rows for EV-301..EV-310 (overlap) plus
# EV-311..EV-336 + bad rows. MERGE on event_id should UPDATE the 10 existing rows
# (event_type/user_id/amount get the source values) and INSERT the remaining new rows.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_target (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex2_target VALUES
        ('EV-301', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-302', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-303', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-304', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-305', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-306', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-307', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-308', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-309', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-310', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: two target tables (raw + clean) for multi-sink writes

# COMMAND ----------

# Raw target: every event in the batch, no transformation
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex3_target_raw (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

# Clean target: only event_type='purchase' AND amount > 0
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex3_target_clean (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: idempotent target table (tracks batch_id)
# MAGIC Empty target. User's foreachBatch tags each row with `batch_id` and uses a MERGE
# MAGIC that skips already-applied batches, so calling the function twice with the same
# MAGIC `batch_id` is a no-op.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex4_target (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP,
        batch_id LONG
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: quarantine + main target for quality routing

# COMMAND ----------

# Main target receives the "good" rows (amount >= 0 AND user_id IS NOT NULL)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_target (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

# Quarantine receives the "bad" rows with a `reason` column explaining why
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_quarantine (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP,
        reason STRING
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: metrics table for running-metrics pattern

# COMMAND ----------

# One row per microbatch with operational metrics.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_metrics (
        batch_id LONG,
        rows_in LONG,
        rows_good LONG,
        rows_quarantined LONG,
        processed_at TIMESTAMP
    ) USING DELTA
""")

# Main target for Exercise 6 (clean rows only, like Exercise 5)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_target (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: materialized batch_df target + quarantine
# MAGIC The exercise reads the batch DataFrame twice (once for a MERGE upsert into the
# MAGIC main target, once for routing rejects into quarantine). Without materializing the
# MAGIC batch via collect(), Spark may recompute the batch DataFrame.

# COMMAND ----------

# Pre-populated main target (will be upserted via MERGE)
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex7_target (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex7_target VALUES
        ('EV-301', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-302', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00'),
        ('EV-303', 'OLD', 'OLD', 0.01, TIMESTAMP '2020-01-01 00:00:00')
""")

# Quarantine for the rejects
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex7_quarantine (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

print(f"Setup complete for foreachBatch Patterns")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print(f"  Per-exercise streaming sources: ex1_source ... ex7_source (40 rows each: 36 good + 4 bad)")
print(f"  ex2_target: 10 pre-existing rows (for MERGE upsert)")
print(f"  ex3_target_raw / ex3_target_clean: empty (multi-sink)")
print(f"  ex4_target: empty (tracks batch_id for idempotency)")
print(f"  ex5_target / ex5_quarantine: empty (quality routing)")
print(f"  ex6_target / ex6_metrics: empty (running metrics)")
print(f"  ex7_target: 3 pre-existing rows / ex7_quarantine: empty (materialize + double-read)")
