# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Checkpointing & Recovery
# MAGIC Creates per-exercise streaming sources, target tables, and checkpoint volumes for the
# MAGIC `checkpointing_recovery` schema. Each exercise has its own isolated source, target, and
# MAGIC checkpoint directory so demonstrations cannot interfere with each other. Source tables
# MAGIC start with a small initial batch; exercises that append more rows do so inside the TODO cell.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "checkpointing_recovery"
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
            ('EV-030', 'view',     'U-2', 0.00,  TIMESTAMP '2026-04-01 10:29:00')
    """)

# COMMAND ----------

# -- Clean previous runs --
# Drop per-exercise sources / targets / observation tables, and wipe every per-exercise
# checkpoint dir. The base table in BASE_SCHEMA is left alone.
for i in range(1, 8):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Extra tables used by specific exercises
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex2_findings")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_observation")

# Wipe every per-exercise checkpoint dir so a re-run starts clean
dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise source tables
# MAGIC Each exercise gets its own isolated source clone of `db_code.streaming.events_source`.
# MAGIC Exercises that append more rows in the TODO cell start from this base of 30 rows.

# COMMAND ----------

# Exercises 1, 2, 3, 4, 6, 7: start with the FIRST 30 rows of the base events source.
# Important: the real `db_code.streaming.events_source` created by `00_Setup.py` has 35
# rows (EV-001..EV-035). The defensive fallback above (when 00_Setup hasn't run) only
# inserts 30 rows (EV-001..EV-030) - enough for this notebook to validate standalone.
# Either way, this notebook is written exclusively against EV-001..EV-030: assertions
# check for 30 / 60 row counts, Exercise 5 appends EV-021..EV-030 to reach 30, and
# Exercise 7 assumes 6 tumbling 5-minute windows spanning 10:00-10:30. Taking the full
# 35-row base table would spill into a 7th window and break Exercise 7's window-count
# assertions; we keep all clones at 30 for consistency.
# Exercise 5 starts with only the first 20 rows so the TODO can append the remaining 10.
for ex in [1, 2, 3, 4, 6, 7]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex{ex}_source
        USING DELTA
        AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
        ORDER BY event_id
        LIMIT 30
    """)

# COMMAND ----------

# Exercise 5: append-then-resume. Source starts with the first 20 events; the TODO cell
# appends 10 more rows (EV-021..EV-030) between two stream runs to verify only the new
# rows land. The INSERT helper in the exercise filters base rows NOT IN the existing 20
# AND restricts to EV-001..EV-030 so the append lands exactly 10 rows (not 15).
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex5_source
    USING DELTA
    AS SELECT * FROM {CATALOG}.{BASE_SCHEMA}.events_source
    ORDER BY event_id
    LIMIT 20
""")

# COMMAND ----------

# Pre-create empty target tables with explicit schemas. Streaming writes will append into them.
# Pre-creating avoids "table does not exist" races and makes schemas explicit for assertions.
EVENT_TARGET_SCHEMA = """
    event_id STRING,
    event_type STRING,
    user_id STRING,
    amount DOUBLE,
    event_ts TIMESTAMP
"""

for ex in [1, 3, 4, 5, 6]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ex{ex}_target (
            {EVENT_TARGET_SCHEMA}
        ) USING DELTA
    """)

# Exercise 7 target is a windowed aggregation: window_start, event_count
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.ex7_target (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        event_count BIGINT
    ) USING DELTA
""")

# COMMAND ----------

print(f"Setup complete for Checkpointing & Recovery")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Base table: {CATALOG}.{BASE_SCHEMA}.events_source (30 rows in standalone fallback; 35 in the real 00_Setup.py)")
print(f"  Checkpoints base: {CHECKPOINT_BASE}")
print(f"  Per-exercise sources: ex1_source ... ex7_source")
print(f"  Exercise 5 source: 20 rows (first 20 events). TODO appends 10 more.")
print(f"  All other exercise sources: 30 rows (full base table).")
