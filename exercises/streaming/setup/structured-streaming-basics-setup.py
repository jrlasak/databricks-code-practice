# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Structured Streaming Basics
# MAGIC Creates per-exercise source tables (cloned from the shared `db_code.streaming.events_source`
# MAGIC base table) and per-exercise target tables in the `structured_streaming_basics` schema.
# MAGIC Each exercise gets its own isolated source and target so streams cannot interfere with
# MAGIC each other. Checkpoints live under a Unity Catalog volume so streams are restartable.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "structured_streaming_basics"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BASE_SCHEMA}")

# Volume for checkpoint locations (one per exercise)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Defensive fallback: create the base streaming source if 00_Setup.py has not run yet --
# In production this table is created by `00_Setup.py` and lives in `db_code.streaming`.
# We re-create it here only if it is missing so this notebook can be validated standalone.
base_exists = spark.sql(
    f"SHOW TABLES IN {CATALOG}.{BASE_SCHEMA}"
).filter("tableName = 'events_source'").count() > 0

if not base_exists:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{BASE_SCHEMA}.events_source (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            amount DOUBLE,
            event_ts TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {CATALOG}.{BASE_SCHEMA}.events_source VALUES
            ('EV-001', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-01 10:00:00'),
            ('EV-002', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:01:00'),
            ('EV-003', 'purchase', 'U-3', 120.50, TIMESTAMP '2026-04-01 10:02:00'),
            ('EV-004', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-01 10:03:00'),
            ('EV-005', 'purchase', 'U-2', 75.25, TIMESTAMP '2026-04-01 10:04:00'),
            ('EV-006', 'view',     'U-4', 0.00,  TIMESTAMP '2026-04-01 10:05:00'),
            ('EV-007', 'purchase', 'U-3', 200.00, TIMESTAMP '2026-04-01 10:06:00'),
            ('EV-008', 'click',    'U-5', 0.00,  TIMESTAMP '2026-04-01 10:07:00'),
            ('EV-009', 'purchase', 'U-1', 45.00, TIMESTAMP '2026-04-01 10:08:00'),
            ('EV-010', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:09:00'),
            ('EV-011', 'purchase', 'U-4', 310.75, TIMESTAMP '2026-04-01 10:10:00'),
            ('EV-012', 'click',    'U-3', 0.00,  TIMESTAMP '2026-04-01 10:11:00'),
            ('EV-013', 'purchase', 'U-5', 89.99, TIMESTAMP '2026-04-01 10:12:00'),
            ('EV-014', 'view',     'U-1', 0.00,  TIMESTAMP '2026-04-01 10:13:00'),
            ('EV-015', 'purchase', 'U-2', 125.00, TIMESTAMP '2026-04-01 10:14:00'),
            ('EV-016', 'click',    'U-4', 0.00,  TIMESTAMP '2026-04-01 10:15:00'),
            ('EV-017', 'purchase', 'U-3', 60.00, TIMESTAMP '2026-04-01 10:16:00'),
            ('EV-018', 'view',     'U-5', 0.00,  TIMESTAMP '2026-04-01 10:17:00'),
            ('EV-019', 'purchase', 'U-1', 175.50, TIMESTAMP '2026-04-01 10:18:00'),
            ('EV-020', 'click',    'U-2', 0.00,  TIMESTAMP '2026-04-01 10:19:00'),
            ('EV-021', 'purchase', 'U-4', 99.50, TIMESTAMP '2026-04-01 10:20:00'),
            ('EV-022', 'view',     'U-3', 0.00,  TIMESTAMP '2026-04-01 10:21:00'),
            ('EV-023', 'purchase', 'U-5', 220.00, TIMESTAMP '2026-04-01 10:22:00'),
            ('EV-024', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-01 10:23:00'),
            ('EV-025', 'purchase', 'U-2', 30.00, TIMESTAMP '2026-04-01 10:24:00'),
            ('EV-026', 'view',     'U-4', 0.00,  TIMESTAMP '2026-04-01 10:25:00'),
            ('EV-027', 'purchase', 'U-3', 410.00, TIMESTAMP '2026-04-01 10:26:00'),
            ('EV-028', 'click',    'U-5', 0.00,  TIMESTAMP '2026-04-01 10:27:00'),
            ('EV-029', 'purchase', 'U-1', 15.50, TIMESTAMP '2026-04-01 10:28:00'),
            ('EV-030', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:29:00'),
            ('EV-031', 'purchase', 'U-4', 270.00, TIMESTAMP '2026-04-01 10:30:00'),
            ('EV-032', 'click',    'U-3', 0.00,  TIMESTAMP '2026-04-01 10:31:00'),
            ('EV-033', 'purchase', 'U-5', 55.00, TIMESTAMP '2026-04-01 10:32:00'),
            ('EV-034', 'view',     'U-1', 0.00,  TIMESTAMP '2026-04-01 10:33:00'),
            ('EV-035', 'purchase', 'U-2', 145.75, TIMESTAMP '2026-04-01 10:34:00')
    """)

# COMMAND ----------

# -- Clean previous runs --
# Drop per-exercise source and target tables, and wipe checkpoints, so the notebook
# is fully idempotent. The base table in BASE_SCHEMA is left alone.
for i in range(1, 9):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Exercise 5 writes to one target (only availableNow is supported on Free Edition); Ex7 also has a progress table.
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex5_target_available_now")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex7_progress")

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise source tables
# MAGIC One source clone per exercise so streams cannot pollute each other.
# MAGIC Each `exN_source` is a Delta CTAS from `db_code.streaming.events_source`.

# COMMAND ----------

# Exercises 1, 4, 6: vanilla copy of the base events source (~35 rows)
for ex in [1, 4, 6]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex{ex}_source
        USING DELTA
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    """)

# COMMAND ----------

# Exercise 2: filter exercise - source has both purchase and non-purchase events.
# Reuses the full base table. Target keeps only event_type='purchase' (18 rows expected).
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex2_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 3: derived column exercise - amount_with_tax = amount * 1.10.
# Same row set as base; assertions check the new column value.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex3_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 5: trigger modes - source for the availableNow query.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 7: lastProgress / named query - same source as ex1.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex7_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
""")

# COMMAND ----------

# Exercise 8: dropDuplicates - introduces deliberate duplicate event_ids.
# Duplicate event_ids appear 2-3 times with identical payload, so the unique event_id
# count (10) is strictly less than the total row count (15) and dedup is observable.
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex8_source (
        event_id STRING,
        event_type STRING,
        user_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex8_source VALUES
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-101', 'purchase', 'U-1', 50.00, TIMESTAMP '2026-04-02 09:00:00'),
        ('EV-102', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-02 09:01:00'),
        ('EV-102', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-02 09:01:00'),
        ('EV-103', 'purchase', 'U-3', 120.00, TIMESTAMP '2026-04-02 09:02:00'),
        ('EV-104', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-02 09:03:00'),
        ('EV-104', 'click',    'U-1', 0.00,  TIMESTAMP '2026-04-02 09:03:00'),
        ('EV-105', 'purchase', 'U-4', 200.00, TIMESTAMP '2026-04-02 09:04:00'),
        ('EV-106', 'view',     'U-5', 0.00,  TIMESTAMP '2026-04-02 09:05:00'),
        ('EV-107', 'purchase', 'U-2', 75.50, TIMESTAMP '2026-04-02 09:06:00'),
        ('EV-107', 'purchase', 'U-2', 75.50, TIMESTAMP '2026-04-02 09:06:00'),
        ('EV-108', 'click',    'U-3', 0.00,  TIMESTAMP '2026-04-02 09:07:00'),
        ('EV-109', 'purchase', 'U-4', 45.00, TIMESTAMP '2026-04-02 09:08:00'),
        ('EV-110', 'view',     'U-1', 0.00,  TIMESTAMP '2026-04-02 09:09:00')
""")

# COMMAND ----------

# Change Data Feed is not needed for these exercises; default Delta tables are fine.
# Streaming reads from Delta tables work out of the box on Free Edition.

# COMMAND ----------

print(f"Setup complete for Structured Streaming Basics")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Base table: {CATALOG}.{BASE_SCHEMA}.events_source (35 rows)")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print(f"  Per-exercise sources: ex1_source ... ex8_source")
print(f"  Exercise 2 source: 35 events (18 purchase, 9 view, 8 click)")
print(f"  Exercise 8 source: 15 rows total, 10 unique event_ids")
