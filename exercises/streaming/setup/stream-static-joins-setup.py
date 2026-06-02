# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Stream-Static Joins
# MAGIC Creates per-exercise streaming source tables (cloned from the shared
# MAGIC `db_code.streaming.events_source` base table) and a shared static customer dimension
# MAGIC table in the `stream_static_joins` schema. Each exercise gets its own isolated
# MAGIC source so streams cannot interfere with each other. Checkpoints live under a
# MAGIC Unity Catalog volume so streams are restartable.
# MAGIC
# MAGIC Run automatically via `%run` from the exercise notebook.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "stream_static_joins"
BASE_SCHEMA = "streaming"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BASE_SCHEMA}")

# Volume for checkpoint locations (one subdir per exercise)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

# -- Defensive fallback: ensure the base streaming events table exists --
# In production this table is created by `00_Setup.py` and lives in `db_code.streaming`.
# Re-create it here only if missing so this notebook can be validated standalone.
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
            ('EV-005', 'purchase', 'U-2', 75.25, TIMESTAMP '2026-04-01 10:04:00')
    """)

# COMMAND ----------

# -- Clean previous runs --
# Drop per-exercise source and target tables, and wipe checkpoints, so the notebook
# is fully idempotent. The base table in BASE_SCHEMA is left alone.
for i in range(1, 8):
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_source")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex{i}_target")

# Exercise 6 (static refresh) has two target tables (one per availableNow run)
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_target_run1")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.ex6_target_run2")

# The static customer dimension - dropped and recreated below so refresh exercise starts clean
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.customers_static")

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static dimension: customers_static
# MAGIC Small dimension table (20 rows). Customers C-001 .. C-020.
# MAGIC Mix of active/inactive customers (for predicate-pushdown exercise).
# MAGIC The events table will reference customer_ids C-001..C-015 plus some NULLs and
# MAGIC unmatched ids (C-991, C-992) so the left-outer exercise has unmatched rows.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.customers_static (
        customer_id STRING,
        name STRING,
        region STRING,
        tier STRING,
        is_active BOOLEAN
    ) USING DELTA
""")

spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.customers_static VALUES
        ('C-001', 'Alice Chen',    'us-east', 'gold',     TRUE),
        ('C-002', 'Bob Patel',     'us-west', 'silver',   TRUE),
        ('C-003', 'Carla Diaz',    'eu',      'gold',     TRUE),
        ('C-004', 'Dan Kim',       'us-east', 'bronze',   FALSE),
        ('C-005', 'Eve Novak',     'eu',      'silver',   TRUE),
        ('C-006', 'Felix Wu',      'apac',    'gold',     TRUE),
        ('C-007', 'Gina Hart',     'us-west', 'bronze',   FALSE),
        ('C-008', 'Hiro Tanaka',   'apac',    'silver',   TRUE),
        ('C-009', 'Ivy Olsen',     'eu',      'bronze',   TRUE),
        ('C-010', 'Jack Reyes',    'us-east', 'gold',     TRUE),
        ('C-011', 'Kira Singh',    'apac',    'silver',   FALSE),
        ('C-012', 'Leo Brandt',    'us-west', 'bronze',   TRUE),
        ('C-013', 'Mona Park',     'eu',      'gold',     TRUE),
        ('C-014', 'Nina Voss',     'us-east', 'silver',   FALSE),
        ('C-015', 'Owen Reed',     'apac',    'bronze',   TRUE),
        ('C-016', 'Pia Ahmed',     'eu',      'gold',     TRUE),
        ('C-017', 'Quinn Lopez',   'us-west', 'silver',   TRUE),
        ('C-018', 'Rita Bauer',    'us-east', 'bronze',   FALSE),
        ('C-019', 'Sami Cohen',    'eu',      'silver',   TRUE),
        ('C-020', 'Tara Mehta',    'apac',    'gold',     TRUE)
""")

# Active count: 14, Inactive count: 6 (used by predicate-pushdown exercise)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-exercise streaming source tables
# MAGIC Each `exN_source` is a Delta table users will read via `spark.readStream.table(...)`.
# MAGIC Events reference a `customer_id` column - a mix of matched, NULL, and unmatched.

# COMMAND ----------

# Shared schema for all event sources: customer_id is the join key.
# Rows: ~50 total. customer_ids spread across C-001..C-015 (matched in static),
# plus 5 NULL customer_ids and 3 unmatched ids (C-991, C-992) -> left-outer exercise gets unmatched rows.
EVENTS_VALUES = """
    ('EV-001', 'purchase', 'C-001', 50.00,  TIMESTAMP '2026-04-01 10:00:00'),
    ('EV-002', 'view',     'C-002', 0.00,   TIMESTAMP '2026-04-01 10:01:00'),
    ('EV-003', 'purchase', 'C-003', 120.50, TIMESTAMP '2026-04-01 10:02:00'),
    ('EV-004', 'click',    'C-001', 0.00,   TIMESTAMP '2026-04-01 10:03:00'),
    ('EV-005', 'purchase', 'C-002', 75.25,  TIMESTAMP '2026-04-01 10:04:00'),
    ('EV-006', 'view',     'C-004', 0.00,   TIMESTAMP '2026-04-01 10:05:00'),
    ('EV-007', 'purchase', 'C-003', 200.00, TIMESTAMP '2026-04-01 10:06:00'),
    ('EV-008', 'click',    'C-005', 0.00,   TIMESTAMP '2026-04-01 10:07:00'),
    ('EV-009', 'purchase', 'C-001', 45.00,  TIMESTAMP '2026-04-01 10:08:00'),
    ('EV-010', 'view',     'C-002', 0.00,   TIMESTAMP '2026-04-01 10:09:00'),
    ('EV-011', 'purchase', 'C-004', 310.75, TIMESTAMP '2026-04-01 10:10:00'),
    ('EV-012', 'click',    'C-003', 0.00,   TIMESTAMP '2026-04-01 10:11:00'),
    ('EV-013', 'purchase', 'C-005', 89.99,  TIMESTAMP '2026-04-01 10:12:00'),
    ('EV-014', 'view',     'C-001', 0.00,   TIMESTAMP '2026-04-01 10:13:00'),
    ('EV-015', 'purchase', 'C-002', 125.00, TIMESTAMP '2026-04-01 10:14:00'),
    ('EV-016', 'click',    'C-006', 0.00,   TIMESTAMP '2026-04-01 10:15:00'),
    ('EV-017', 'purchase', 'C-007', 60.00,  TIMESTAMP '2026-04-01 10:16:00'),
    ('EV-018', 'view',     'C-008', 0.00,   TIMESTAMP '2026-04-01 10:17:00'),
    ('EV-019', 'purchase', 'C-009', 175.50, TIMESTAMP '2026-04-01 10:18:00'),
    ('EV-020', 'click',    'C-010', 0.00,   TIMESTAMP '2026-04-01 10:19:00'),
    ('EV-021', 'purchase', 'C-011', 99.50,  TIMESTAMP '2026-04-01 10:20:00'),
    ('EV-022', 'view',     'C-012', 0.00,   TIMESTAMP '2026-04-01 10:21:00'),
    ('EV-023', 'purchase', 'C-013', 220.00, TIMESTAMP '2026-04-01 10:22:00'),
    ('EV-024', 'click',    'C-014', 0.00,   TIMESTAMP '2026-04-01 10:23:00'),
    ('EV-025', 'purchase', 'C-015', 30.00,  TIMESTAMP '2026-04-01 10:24:00'),
    ('EV-026', 'view',     'C-006', 0.00,   TIMESTAMP '2026-04-01 10:25:00'),
    ('EV-027', 'purchase', 'C-007', 410.00, TIMESTAMP '2026-04-01 10:26:00'),
    ('EV-028', 'click',    'C-008', 0.00,   TIMESTAMP '2026-04-01 10:27:00'),
    ('EV-029', 'purchase', 'C-009', 15.50,  TIMESTAMP '2026-04-01 10:28:00'),
    ('EV-030', 'view',     'C-010', 0.00,   TIMESTAMP '2026-04-01 10:29:00'),
    ('EV-031', 'purchase', 'C-011', 270.00, TIMESTAMP '2026-04-01 10:30:00'),
    ('EV-032', 'click',    'C-012', 0.00,   TIMESTAMP '2026-04-01 10:31:00'),
    ('EV-033', 'purchase', 'C-013', 55.00,  TIMESTAMP '2026-04-01 10:32:00'),
    ('EV-034', 'view',     'C-014', 0.00,   TIMESTAMP '2026-04-01 10:33:00'),
    ('EV-035', 'purchase', 'C-015', 145.75, TIMESTAMP '2026-04-01 10:34:00'),
    ('EV-036', 'purchase', 'C-991', 80.00,  TIMESTAMP '2026-04-01 10:35:00'),
    ('EV-037', 'view',     'C-992', 0.00,   TIMESTAMP '2026-04-01 10:36:00'),
    ('EV-038', 'purchase', 'C-991', 95.00,  TIMESTAMP '2026-04-01 10:37:00'),
    ('EV-039', 'click',    NULL,    0.00,   TIMESTAMP '2026-04-01 10:38:00'),
    ('EV-040', 'view',     NULL,    0.00,   TIMESTAMP '2026-04-01 10:39:00'),
    ('EV-041', 'purchase', NULL,    65.00,  TIMESTAMP '2026-04-01 10:40:00'),
    ('EV-042', 'click',    NULL,    0.00,   TIMESTAMP '2026-04-01 10:41:00'),
    ('EV-043', 'view',     NULL,    0.00,   TIMESTAMP '2026-04-01 10:42:00'),
    ('EV-044', 'purchase', 'C-001', 110.00, TIMESTAMP '2026-04-01 10:43:00'),
    ('EV-045', 'purchase', 'C-002', 35.00,  TIMESTAMP '2026-04-01 10:44:00'),
    ('EV-046', 'purchase', 'C-003', 220.00, TIMESTAMP '2026-04-01 10:45:00'),
    ('EV-047', 'view',     'C-004', 0.00,   TIMESTAMP '2026-04-01 10:46:00'),
    ('EV-048', 'click',    'C-005', 0.00,   TIMESTAMP '2026-04-01 10:47:00'),
    ('EV-049', 'purchase', 'C-006', 75.00,  TIMESTAMP '2026-04-01 10:48:00'),
    ('EV-050', 'purchase', 'C-007', 195.00, TIMESTAMP '2026-04-01 10:49:00')
"""
# Row counts:
#   total events             = 50
#   events with NULL customer_id = 5  (EV-039..EV-043)
#   events with unmatched id  = 3  (EV-036=C-991, EV-037=C-992, EV-038=C-991)
#   events with matched id    = 42

# COMMAND ----------

# Exercises 1, 2, 3, 4, 5, 7: vanilla copy of the full events (50 rows)
for ex in [1, 2, 3, 4, 5, 7]:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex{ex}_source (
            event_id STRING,
            event_type STRING,
            customer_id STRING,
            amount DOUBLE,
            event_ts TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        INSERT INTO {CATALOG}.{SCHEMA}.ex{ex}_source VALUES {EVENTS_VALUES.strip().rstrip(",")}
    """)

# COMMAND ----------

# Exercise 6 (static refresh, hard): two batches of events.
# Run 1 reads ex6_source_run1, joins against the initial customers_static, writes ex6_target_run1.
# User then inserts a new customer into customers_static.
# Run 2 reads ex6_source_run2, joins against the UPDATED static table, writes ex6_target_run2.
# Both source batches contain ONLY the new customer's id (C-999) so the assertion is clean:
#   run 1 -> 0 matched rows (customer doesn't exist yet -> no inner-join output)
#   run 2 -> matched rows after the user inserts C-999

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_source_run1 (
        event_id STRING,
        event_type STRING,
        customer_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex6_source_run1 VALUES
        ('EV-601', 'purchase', 'C-999', 50.00,  TIMESTAMP '2026-04-03 09:00:00'),
        ('EV-602', 'purchase', 'C-999', 75.00,  TIMESTAMP '2026-04-03 09:01:00'),
        ('EV-603', 'view',     'C-999', 0.00,   TIMESTAMP '2026-04-03 09:02:00')
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ex6_source_run2 (
        event_id STRING,
        event_type STRING,
        customer_id STRING,
        amount DOUBLE,
        event_ts TIMESTAMP
    ) USING DELTA
""")
spark.sql(f"""
    INSERT INTO {CATALOG}.{SCHEMA}.ex6_source_run2 VALUES
        ('EV-604', 'purchase', 'C-999', 100.00, TIMESTAMP '2026-04-03 09:10:00'),
        ('EV-605', 'purchase', 'C-999', 200.00, TIMESTAMP '2026-04-03 09:11:00')
""")

# COMMAND ----------

print(f"Setup complete for Stream-Static Joins")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Static dimension: {CATALOG}.{SCHEMA}.customers_static (20 customers, 14 active / 6 inactive)")
print(f"  Checkpoints: {CHECKPOINT_BASE}")
print(f"  Per-exercise event sources: ex1_source-ex5_source and ex7_source (50 rows each; ex6 uses two separate run tables)")
print(f"     - 42 matched customer_ids, 3 unmatched (C-991/C-992), 5 NULL customer_id")
print(f"  Exercise 6 (static refresh): ex6_source_run1 (3 rows, C-999) + ex6_source_run2 (2 rows, C-999)")
