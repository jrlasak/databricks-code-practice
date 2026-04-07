# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00_Setup: Shared Data Generator
# MAGIC **Run this notebook once** before solving any exercise. It creates the `db_code` catalog,
# MAGIC zone schemas, per-topic schemas, and all base tables that exercise notebooks reference.
# MAGIC
# MAGIC **What it creates**:
# MAGIC - Catalog: `db_code`
# MAGIC - Zone schemas: `delta_lake`, `elt`, `streaming`, `production`, `governance`, `performance`, `fundamentals`
# MAGIC - Per-topic schemas: `spark_sql_joins`, `window_functions`, `pyspark_transformations`, `auto_loader`, `batch_ingestion`, `medallion_architecture`, `complex_data_types`
# MAGIC - Base tables (in `elt`): `orders` (~100 rows), `customers` (~50 rows), `products` (~30 rows),
# MAGIC   `events` (~150 rows), `order_items` (~200 rows)
# MAGIC - Volumes: `auto_loader.source_files`, `auto_loader.checkpoints`, `batch_ingestion.source_files`
# MAGIC
# MAGIC **Idempotent**: Safe to run multiple times. Checks if base tables already exist with
# MAGIC correct row counts before recreating. Schema creation is always idempotent via IF NOT EXISTS.
# MAGIC
# MAGIC **Deterministic**: All data is fixed. No randomness. Same data for everyone,
# MAGIC enabling exact-value assertions in exercises.

# COMMAND ----------

# -- Catalog and Schemas --

CATALOG = "db_code"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

# Zone schemas
for schema in [
    "delta_lake",
    "elt",
    "streaming",
    "production",
    "governance",
    "performance",
    "fundamentals",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# Per-topic schemas for ELT zone (exercise tables go here, base tables stay in elt)
for schema in [
    "spark_sql_joins",
    "window_functions",
    "pyspark_transformations",
    "auto_loader",
    "batch_ingestion",
    "medallion_architecture",
    "complex_data_types",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# COMMAND ----------

# -- Idempotency Check --
# If base tables already exist with correct row counts, skip recreation.
# This avoids redundant writes on repeated runs and respects schema quotas.

_SETUP_SKIP = False
try:
    _orders_ct = spark.table(f"{CATALOG}.elt.orders").count()
    _customers_ct = spark.table(f"{CATALOG}.elt.customers").count()
    _products_ct = spark.table(f"{CATALOG}.elt.products").count()
    _events_ct = spark.table(f"{CATALOG}.elt.events").count()
    _items_ct = spark.table(f"{CATALOG}.elt.order_items").count()
    if _orders_ct >= 100 and _customers_ct >= 50 and _products_ct >= 30 and _events_ct >= 150 and _items_ct >= 200:
        _SETUP_SKIP = True
        print("=" * 60)
        print(f"  Base tables already exist with correct row counts. Skipping creation.")
        print(f"  orders={_orders_ct}, customers={_customers_ct}, products={_products_ct}, events={_events_ct}, order_items={_items_ct}")
        print("=" * 60)
except Exception:
    _SETUP_SKIP = False

# COMMAND ----------

# -- Orders Table --
# ~100 rows. Primary table referenced by most exercises.
#
# Edge cases built in:
#   - Duplicates: ORD-042 and ORD-067 appear twice with different updated_at
#   - Null customer_id: ORD-015, ORD-038, ORD-071
#   - $0 amounts: ORD-022, ORD-058
#   - Future dates: ORD-091 through ORD-095 have order_date in 2027
#   - Mixed statuses: pending, completed, shipped, cancelled
#   - amount < 100: ORD-003, ORD-005, ORD-009, ORD-013, ORD-022, ORD-038, ORD-058 (and others)
#   - amount >= 100: ORD-001, ORD-002, ORD-004, ORD-006, ORD-007, ORD-008 (and others)
#
# ORD-001 through ORD-005 are the most referenced subset across exercises.
# ORD-001 through ORD-020 must exist for OPTIMIZE/liquid-clustering setup (LIMIT 20 queries).

if not _SETUP_SKIP:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.elt.orders (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        amount DOUBLE,
        status STRING,
        order_date DATE,
        updated_at TIMESTAMP
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.elt.orders
    SELECT * FROM VALUES
    -- ORD-001 through ORD-005: core subset used by most setup notebooks
    ('ORD-001', 'CUST-001', 'PROD-001', 109.50, 'completed', DATE '2025-06-15', TIMESTAMP '2025-06-15 09:30:00'),
    ('ORD-002', 'CUST-002', 'PROD-003', 175.00, 'completed', DATE '2025-06-16', TIMESTAMP '2025-06-16 14:15:00'),
    ('ORD-003', 'CUST-003', 'PROD-002', 45.00,  'pending',   DATE '2025-06-17', TIMESTAMP '2025-06-17 11:00:00'),
    ('ORD-004', 'CUST-004', 'PROD-005', 320.00, 'cancelled',  DATE '2025-06-18', TIMESTAMP '2025-06-18 16:45:00'),
    ('ORD-005', 'CUST-005', 'PROD-001', 89.99,  'pending',   DATE '2025-06-19', TIMESTAMP '2025-06-19 08:20:00'),

    -- ORD-006 through ORD-010
    ('ORD-006', 'CUST-006', 'PROD-004', 210.00, 'completed', DATE '2025-06-20', TIMESTAMP '2025-06-20 10:00:00'),
    ('ORD-007', 'CUST-007', 'PROD-002', 155.50, 'shipped',   DATE '2025-06-21', TIMESTAMP '2025-06-21 13:30:00'),
    ('ORD-008', 'CUST-001', 'PROD-003', 249.99, 'completed', DATE '2025-06-22', TIMESTAMP '2025-06-22 09:45:00'),
    ('ORD-009', 'CUST-008', 'PROD-006', 35.00,  'cancelled', DATE '2025-06-23', TIMESTAMP '2025-06-23 17:10:00'),
    ('ORD-010', 'CUST-009', 'PROD-001', 109.50, 'pending',   DATE '2025-06-24', TIMESTAMP '2025-06-24 11:30:00'),

    -- ORD-011 through ORD-020
    ('ORD-011', 'CUST-010', 'PROD-005', 450.00, 'completed', DATE '2025-07-01', TIMESTAMP '2025-07-01 08:00:00'),
    ('ORD-012', 'CUST-011', 'PROD-002', 130.00, 'shipped',   DATE '2025-07-02', TIMESTAMP '2025-07-02 14:20:00'),
    ('ORD-013', 'CUST-012', 'PROD-007', 67.50,  'pending',   DATE '2025-07-03', TIMESTAMP '2025-07-03 10:15:00'),
    ('ORD-014', 'CUST-003', 'PROD-003', 175.00, 'completed', DATE '2025-07-04', TIMESTAMP '2025-07-04 16:00:00'),
    ('ORD-015', NULL,        'PROD-001', 89.99,  'shipped',   DATE '2025-07-05', TIMESTAMP '2025-07-05 09:30:00'),
    ('ORD-016', 'CUST-014', 'PROD-008', 199.00, 'completed', DATE '2025-07-06', TIMESTAMP '2025-07-06 11:45:00'),
    ('ORD-017', 'CUST-015', 'PROD-004', 210.00, 'pending',   DATE '2025-07-07', TIMESTAMP '2025-07-07 15:00:00'),
    ('ORD-018', 'CUST-016', 'PROD-009', 525.00, 'shipped',   DATE '2025-07-08', TIMESTAMP '2025-07-08 08:30:00'),
    ('ORD-019', 'CUST-002', 'PROD-005', 320.00, 'completed', DATE '2025-07-09', TIMESTAMP '2025-07-09 12:00:00'),
    ('ORD-020', 'CUST-017', 'PROD-010', 112.00, 'cancelled', DATE '2025-07-10', TIMESTAMP '2025-07-10 17:30:00'),

    -- ORD-021 through ORD-030
    ('ORD-021', 'CUST-018', 'PROD-001', 109.50, 'completed', DATE '2025-07-11', TIMESTAMP '2025-07-11 09:00:00'),
    ('ORD-022', 'CUST-019', 'PROD-003', 0.00,   'completed', DATE '2025-07-12', TIMESTAMP '2025-07-12 10:30:00'),
    ('ORD-023', 'CUST-020', 'PROD-006', 35.00,  'shipped',   DATE '2025-07-13', TIMESTAMP '2025-07-13 14:00:00'),
    ('ORD-024', 'CUST-001', 'PROD-002', 45.00,  'pending',   DATE '2025-07-14', TIMESTAMP '2025-07-14 11:15:00'),
    ('ORD-025', 'CUST-021', 'PROD-008', 199.00, 'completed', DATE '2025-07-15', TIMESTAMP '2025-07-15 16:30:00'),
    ('ORD-026', 'CUST-022', 'PROD-004', 275.00, 'shipped',   DATE '2025-07-16', TIMESTAMP '2025-07-16 08:45:00'),
    ('ORD-027', 'CUST-023', 'PROD-010', 112.00, 'cancelled', DATE '2025-07-17', TIMESTAMP '2025-07-17 13:00:00'),
    ('ORD-028', 'CUST-024', 'PROD-005', 320.00, 'completed', DATE '2025-07-18', TIMESTAMP '2025-07-18 10:00:00'),
    ('ORD-029', 'CUST-025', 'PROD-001', 109.50, 'pending',   DATE '2025-07-19', TIMESTAMP '2025-07-19 15:30:00'),
    ('ORD-030', 'CUST-013', 'PROD-007', 67.50,  'shipped',   DATE '2025-07-20', TIMESTAMP '2025-07-20 09:15:00'),

    -- ORD-031 through ORD-040 (includes null customer_id edge case)
    ('ORD-031', 'CUST-026', 'PROD-009', 525.00, 'completed', DATE '2025-08-01', TIMESTAMP '2025-08-01 08:00:00'),
    ('ORD-032', 'CUST-027', 'PROD-002', 130.00, 'shipped',   DATE '2025-08-02', TIMESTAMP '2025-08-02 14:30:00'),
    ('ORD-033', 'CUST-028', 'PROD-003', 175.00, 'pending',   DATE '2025-08-03', TIMESTAMP '2025-08-03 11:00:00'),
    ('ORD-034', 'CUST-005', 'PROD-006', 35.00,  'completed', DATE '2025-08-04', TIMESTAMP '2025-08-04 16:20:00'),
    ('ORD-035', 'CUST-029', 'PROD-001', 109.50, 'shipped',   DATE '2025-08-05', TIMESTAMP '2025-08-05 09:45:00'),
    ('ORD-036', 'CUST-030', 'PROD-008', 199.00, 'completed', DATE '2025-08-06', TIMESTAMP '2025-08-06 13:15:00'),
    ('ORD-037', 'CUST-004', 'PROD-004', 275.00, 'cancelled', DATE '2025-08-07', TIMESTAMP '2025-08-07 10:30:00'),
    ('ORD-038', NULL,        'PROD-010', 55.00,  'pending',   DATE '2025-08-08', TIMESTAMP '2025-08-08 15:00:00'),
    ('ORD-039', 'CUST-031', 'PROD-005', 320.00, 'completed', DATE '2025-08-09', TIMESTAMP '2025-08-09 08:30:00'),
    ('ORD-040', 'CUST-032', 'PROD-002', 130.00, 'shipped',   DATE '2025-08-10', TIMESTAMP '2025-08-10 12:00:00'),

    -- ORD-041 through ORD-050 (includes duplicate edge case: ORD-042 appears twice)
    ('ORD-041', 'CUST-033', 'PROD-007', 67.50,  'completed', DATE '2025-08-11', TIMESTAMP '2025-08-11 09:00:00'),
    ('ORD-042', 'CUST-034', 'PROD-003', 175.00, 'pending',   DATE '2025-08-12', TIMESTAMP '2025-08-12 10:00:00'),
    ('ORD-042', 'CUST-034', 'PROD-003', 175.00, 'shipped',   DATE '2025-08-12', TIMESTAMP '2025-08-13 14:30:00'),
    ('ORD-043', 'CUST-035', 'PROD-009', 525.00, 'completed', DATE '2025-08-13', TIMESTAMP '2025-08-13 11:30:00'),
    ('ORD-044', 'CUST-006', 'PROD-001', 109.50, 'shipped',   DATE '2025-08-14', TIMESTAMP '2025-08-14 16:00:00'),
    ('ORD-045', 'CUST-036', 'PROD-006', 35.00,  'cancelled', DATE '2025-08-15', TIMESTAMP '2025-08-15 08:15:00'),
    ('ORD-046', 'CUST-037', 'PROD-004', 210.00, 'completed', DATE '2025-08-16', TIMESTAMP '2025-08-16 13:45:00'),
    ('ORD-047', 'CUST-038', 'PROD-008', 199.00, 'pending',   DATE '2025-08-17', TIMESTAMP '2025-08-17 10:00:00'),
    ('ORD-048', 'CUST-039', 'PROD-005', 320.00, 'shipped',   DATE '2025-08-18', TIMESTAMP '2025-08-18 15:30:00'),
    ('ORD-049', 'CUST-040', 'PROD-002', 130.00, 'completed', DATE '2025-08-19', TIMESTAMP '2025-08-19 09:00:00'),
    ('ORD-050', 'CUST-007', 'PROD-010', 112.00, 'pending',   DATE '2025-08-20', TIMESTAMP '2025-08-20 14:15:00'),

    -- ORD-051 through ORD-060 (includes $0 amount edge case)
    ('ORD-051', 'CUST-041', 'PROD-003', 175.00, 'completed', DATE '2025-09-01', TIMESTAMP '2025-09-01 08:00:00'),
    ('ORD-052', 'CUST-042', 'PROD-001', 109.50, 'shipped',   DATE '2025-09-02', TIMESTAMP '2025-09-02 10:30:00'),
    ('ORD-053', 'CUST-043', 'PROD-007', 67.50,  'pending',   DATE '2025-09-03', TIMESTAMP '2025-09-03 14:00:00'),
    ('ORD-054', 'CUST-008', 'PROD-004', 275.00, 'completed', DATE '2025-09-04', TIMESTAMP '2025-09-04 11:15:00'),
    ('ORD-055', 'CUST-044', 'PROD-009', 525.00, 'shipped',   DATE '2025-09-05', TIMESTAMP '2025-09-05 16:30:00'),
    ('ORD-056', 'CUST-045', 'PROD-006', 35.00,  'completed', DATE '2025-09-06', TIMESTAMP '2025-09-06 09:00:00'),
    ('ORD-057', 'CUST-046', 'PROD-005', 320.00, 'cancelled', DATE '2025-09-07', TIMESTAMP '2025-09-07 13:00:00'),
    ('ORD-058', 'CUST-047', 'PROD-002', 0.00,   'completed', DATE '2025-09-08', TIMESTAMP '2025-09-08 10:45:00'),
    ('ORD-059', 'CUST-048', 'PROD-008', 199.00, 'pending',   DATE '2025-09-09', TIMESTAMP '2025-09-09 15:00:00'),
    ('ORD-060', 'CUST-009', 'PROD-010', 112.00, 'shipped',   DATE '2025-09-10', TIMESTAMP '2025-09-10 08:30:00'),

    -- ORD-061 through ORD-070 (includes duplicate edge case: ORD-067 appears twice)
    ('ORD-061', 'CUST-049', 'PROD-001', 109.50, 'completed', DATE '2025-09-11', TIMESTAMP '2025-09-11 10:00:00'),
    ('ORD-062', 'CUST-050', 'PROD-004', 210.00, 'pending',   DATE '2025-09-12', TIMESTAMP '2025-09-12 14:30:00'),
    ('ORD-063', 'CUST-010', 'PROD-003', 175.00, 'shipped',   DATE '2025-09-13', TIMESTAMP '2025-09-13 11:00:00'),
    ('ORD-064', 'CUST-011', 'PROD-007', 67.50,  'completed', DATE '2025-09-14', TIMESTAMP '2025-09-14 16:45:00'),
    ('ORD-065', 'CUST-012', 'PROD-009', 525.00, 'completed', DATE '2025-09-15', TIMESTAMP '2025-09-15 09:15:00'),
    ('ORD-066', 'CUST-013', 'PROD-002', 130.00, 'cancelled', DATE '2025-09-16', TIMESTAMP '2025-09-16 13:30:00'),
    ('ORD-067', 'CUST-014', 'PROD-005', 320.00, 'shipped',   DATE '2025-09-17', TIMESTAMP '2025-09-17 10:00:00'),
    ('ORD-067', 'CUST-014', 'PROD-005', 340.00, 'completed', DATE '2025-09-17', TIMESTAMP '2025-09-18 08:00:00'),
    ('ORD-068', 'CUST-015', 'PROD-006', 35.00,  'pending',   DATE '2025-09-18', TIMESTAMP '2025-09-18 15:00:00'),
    ('ORD-069', 'CUST-016', 'PROD-008', 199.00, 'completed', DATE '2025-09-19', TIMESTAMP '2025-09-19 09:30:00'),
    ('ORD-070', 'CUST-017', 'PROD-001', 109.50, 'shipped',   DATE '2025-09-20', TIMESTAMP '2025-09-20 14:00:00'),

    -- ORD-071 through ORD-080 (includes null customer_id edge case)
    ('ORD-071', NULL,        'PROD-004', 210.00, 'completed', DATE '2025-10-01', TIMESTAMP '2025-10-01 08:00:00'),
    ('ORD-072', 'CUST-018', 'PROD-003', 175.00, 'pending',   DATE '2025-10-02', TIMESTAMP '2025-10-02 10:30:00'),
    ('ORD-073', 'CUST-019', 'PROD-010', 112.00, 'shipped',   DATE '2025-10-03', TIMESTAMP '2025-10-03 14:15:00'),
    ('ORD-074', 'CUST-020', 'PROD-002', 130.00, 'completed', DATE '2025-10-04', TIMESTAMP '2025-10-04 11:00:00'),
    ('ORD-075', 'CUST-021', 'PROD-007', 67.50,  'cancelled', DATE '2025-10-05', TIMESTAMP '2025-10-05 16:30:00'),
    ('ORD-076', 'CUST-022', 'PROD-005', 320.00, 'completed', DATE '2025-10-06', TIMESTAMP '2025-10-06 09:00:00'),
    ('ORD-077', 'CUST-023', 'PROD-009', 525.00, 'shipped',   DATE '2025-10-07', TIMESTAMP '2025-10-07 13:00:00'),
    ('ORD-078', 'CUST-024', 'PROD-006', 35.00,  'pending',   DATE '2025-10-08', TIMESTAMP '2025-10-08 10:15:00'),
    ('ORD-079', 'CUST-025', 'PROD-001', 109.50, 'completed', DATE '2025-10-09', TIMESTAMP '2025-10-09 15:45:00'),
    ('ORD-080', 'CUST-026', 'PROD-008', 199.00, 'shipped',   DATE '2025-10-10', TIMESTAMP '2025-10-10 08:30:00'),

    -- ORD-081 through ORD-090
    ('ORD-081', 'CUST-027', 'PROD-004', 275.00, 'completed', DATE '2025-10-11', TIMESTAMP '2025-10-11 12:00:00'),
    ('ORD-082', 'CUST-028', 'PROD-002', 45.00,  'pending',   DATE '2025-10-12', TIMESTAMP '2025-10-12 09:00:00'),
    ('ORD-083', 'CUST-029', 'PROD-003', 175.00, 'shipped',   DATE '2025-10-13', TIMESTAMP '2025-10-13 14:30:00'),
    ('ORD-084', 'CUST-030', 'PROD-010', 112.00, 'completed', DATE '2025-10-14', TIMESTAMP '2025-10-14 11:15:00'),
    ('ORD-085', 'CUST-031', 'PROD-005', 320.00, 'cancelled', DATE '2025-10-15', TIMESTAMP '2025-10-15 16:00:00'),
    ('ORD-086', 'CUST-032', 'PROD-007', 67.50,  'completed', DATE '2025-10-16', TIMESTAMP '2025-10-16 09:30:00'),
    ('ORD-087', 'CUST-033', 'PROD-001', 109.50, 'pending',   DATE '2025-10-17', TIMESTAMP '2025-10-17 13:45:00'),
    ('ORD-088', 'CUST-034', 'PROD-009', 525.00, 'shipped',   DATE '2025-10-18', TIMESTAMP '2025-10-18 10:00:00'),
    ('ORD-089', 'CUST-035', 'PROD-006', 35.00,  'completed', DATE '2025-10-19', TIMESTAMP '2025-10-19 15:00:00'),
    ('ORD-090', 'CUST-036', 'PROD-008', 199.00, 'completed', DATE '2025-10-20', TIMESTAMP '2025-10-20 08:00:00'),

    -- ORD-091 through ORD-095: future dates (edge case)
    ('ORD-091', 'CUST-037', 'PROD-001', 109.50, 'pending',   DATE '2027-01-15', TIMESTAMP '2027-01-15 09:00:00'),
    ('ORD-092', 'CUST-038', 'PROD-003', 175.00, 'pending',   DATE '2027-02-20', TIMESTAMP '2027-02-20 10:30:00'),
    ('ORD-093', 'CUST-039', 'PROD-005', 320.00, 'pending',   DATE '2027-03-10', TIMESTAMP '2027-03-10 14:00:00'),
    ('ORD-094', 'CUST-040', 'PROD-002', 45.00,  'pending',   DATE '2027-04-05', TIMESTAMP '2027-04-05 11:00:00'),
    ('ORD-095', 'CUST-041', 'PROD-004', 210.00, 'pending',   DATE '2027-05-01', TIMESTAMP '2027-05-01 16:00:00'),

    -- ORD-096 through ORD-100: recent orders
    ('ORD-096', 'CUST-042', 'PROD-010', 112.00, 'completed', DATE '2026-01-05', TIMESTAMP '2026-01-05 09:30:00'),
    ('ORD-097', 'CUST-043', 'PROD-007', 67.50,  'shipped',   DATE '2026-01-10', TIMESTAMP '2026-01-10 14:00:00'),
    ('ORD-098', 'CUST-044', 'PROD-009', 525.00, 'completed', DATE '2026-01-15', TIMESTAMP '2026-01-15 10:00:00'),
    ('ORD-099', 'CUST-045', 'PROD-006', 35.00,  'pending',   DATE '2026-01-20', TIMESTAMP '2026-01-20 15:30:00'),
    ('ORD-100', 'CUST-046', 'PROD-008', 199.00, 'completed', DATE '2026-01-25', TIMESTAMP '2026-01-25 08:45:00')

    AS t(order_id, customer_id, product_id, amount, status, order_date, updated_at)
    """)

# COMMAND ----------

# -- Customers Table --
# ~50 rows. Used by MERGE SCD Type 2 exercises and governance exercises.
#
# Edge cases built in:
#   - Null emails: CUST-015, CUST-033, CUST-047
#   - Duplicate names: CUST-008 and CUST-028 both named "James Wilson"
#   - Inactive customers (tier='inactive'): CUST-025, CUST-049
#   - All regions: US-East, US-West, EU-West, EU-East, APAC

if not _SETUP_SKIP:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.elt.customers (
        customer_id STRING,
        name STRING,
        email STRING,
        region STRING,
        tier STRING,
        signup_date DATE
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.elt.customers
    SELECT * FROM VALUES
    ('CUST-001', 'Alice Smith',      'alice@example.com',       'US-East',  'gold',     DATE '2024-01-15'),
    ('CUST-002', 'Bob Johnson',      'bob@example.com',         'US-West',  'silver',   DATE '2024-02-20'),
    ('CUST-003', 'Carol Lee',        'carol@example.com',       'US-West',  'gold',     DATE '2024-03-10'),
    ('CUST-004', 'David Chen',       'david.chen@example.com',  'APAC',     'platinum', DATE '2024-01-05'),
    ('CUST-005', 'Emma Garcia',      'emma.g@example.com',      'EU-West',  'silver',   DATE '2024-04-01'),
    ('CUST-006', 'Frank Miller',     'frank@example.com',       'US-East',  'bronze',   DATE '2024-05-12'),
    ('CUST-007', 'Grace Kim',        'grace.kim@example.com',   'APAC',     'gold',     DATE '2024-02-28'),
    ('CUST-008', 'James Wilson',     'james.w@example.com',     'US-East',  'silver',   DATE '2024-06-15'),
    ('CUST-009', 'Iris Patel',       'iris@example.com',        'EU-East',  'bronze',   DATE '2024-03-22'),
    ('CUST-010', 'Kevin Brown',      'kevin.b@example.com',     'US-West',  'gold',     DATE '2024-07-01'),
    ('CUST-011', 'Laura Martinez',   'laura.m@example.com',     'EU-West',  'silver',   DATE '2024-04-18'),
    ('CUST-012', 'Michael Davis',    'michael.d@example.com',   'US-East',  'bronze',   DATE '2024-08-05'),
    ('CUST-013', 'Nina Thompson',    'nina.t@example.com',      'APAC',     'gold',     DATE '2024-05-30'),
    ('CUST-014', 'Oscar Rivera',     'oscar@example.com',       'US-West',  'platinum', DATE '2024-01-20'),
    ('CUST-015', 'Patricia Wang',     NULL,                      'APAC',     'silver',   DATE '2024-06-25'),
    ('CUST-016', 'Quinn Foster',     'quinn.f@example.com',     'EU-East',  'bronze',   DATE '2024-09-01'),
    ('CUST-017', 'Rachel Adams',     'rachel@example.com',      'US-East',  'gold',     DATE '2024-02-14'),
    ('CUST-018', 'Samuel Clark',     'sam.c@example.com',       'EU-West',  'silver',   DATE '2024-07-20'),
    ('CUST-019', 'Tanya Nguyen',     'tanya.n@example.com',     'APAC',     'bronze',   DATE '2024-10-10'),
    ('CUST-020', 'Umar Hassan',      'umar@example.com',        'EU-East',  'gold',     DATE '2024-03-05'),
    ('CUST-021', 'Victoria Scott',   'victoria@example.com',    'US-West',  'silver',   DATE '2024-08-15'),
    ('CUST-022', 'William Torres',   'will.t@example.com',      'US-East',  'platinum', DATE '2024-04-22'),
    ('CUST-023', 'Xena Lopez',       'xena@example.com',        'EU-West',  'gold',     DATE '2024-11-01'),
    ('CUST-024', 'Yusuf Ali',        'yusuf.a@example.com',     'APAC',     'bronze',   DATE '2024-05-08'),
    ('CUST-025', 'Zara Mitchell',    'zara@example.com',        'US-East',  'inactive', DATE '2023-06-15'),
    ('CUST-026', 'Aaron Hughes',     'aaron.h@example.com',     'US-West',  'silver',   DATE '2024-12-01'),
    ('CUST-027', 'Bianca Romano',    'bianca@example.com',      'EU-East',  'gold',     DATE '2024-06-10'),
    ('CUST-028', 'James Wilson',     'jwilson2@example.com',    'US-West',  'bronze',   DATE '2024-09-20'),
    ('CUST-029', 'Diana Cho',        'diana.c@example.com',     'APAC',     'silver',   DATE '2024-07-15'),
    ('CUST-030', 'Ethan Brooks',     'ethan@example.com',       'EU-West',  'gold',     DATE '2024-01-30'),
    ('CUST-031', 'Fiona O''Brien',   'fiona@example.com',       'US-East',  'platinum', DATE '2024-10-25'),
    ('CUST-032', 'George Tanaka',    'george.t@example.com',    'APAC',     'silver',   DATE '2024-08-08'),
    ('CUST-033', 'Hannah Reed',       NULL,                      'US-West',  'bronze',   DATE '2024-11-15'),
    ('CUST-034', 'Ivan Petrov',      'ivan.p@example.com',      'EU-East',  'gold',     DATE '2024-02-05'),
    ('CUST-035', 'Julia Santos',     'julia.s@example.com',     'EU-West',  'silver',   DATE '2024-12-20'),
    ('CUST-036', 'Kyle Murphy',      'kyle.m@example.com',      'US-East',  'bronze',   DATE '2024-03-18'),
    ('CUST-037', 'Leila Amari',      'leila@example.com',       'EU-West',  'gold',     DATE '2024-09-05'),
    ('CUST-038', 'Marco Bellini',    'marco.b@example.com',     'EU-East',  'silver',   DATE '2024-04-12'),
    ('CUST-039', 'Naomi Sato',       'naomi.s@example.com',     'APAC',     'platinum', DATE '2024-07-28'),
    ('CUST-040', 'Owen Price',       'owen.p@example.com',      'US-West',  'bronze',   DATE '2024-10-02'),
    ('CUST-041', 'Priya Sharma',     'priya@example.com',       'APAC',     'gold',     DATE '2024-05-20'),
    ('CUST-042', 'Ryan Fletcher',    'ryan.f@example.com',      'US-East',  'silver',   DATE '2024-11-08'),
    ('CUST-043', 'Sofia Herrera',    'sofia.h@example.com',     'EU-West',  'bronze',   DATE '2024-06-03'),
    ('CUST-044', 'Tyler Ross',       'tyler.r@example.com',     'US-West',  'gold',     DATE '2024-12-15'),
    ('CUST-045', 'Uma Reddy',        'uma@example.com',         'APAC',     'silver',   DATE '2024-01-25'),
    ('CUST-046', 'Victor Chang',     'victor.c@example.com',    'EU-East',  'bronze',   DATE '2024-08-30'),
    ('CUST-047', 'Wendy Park',        NULL,                      'APAC',     'gold',     DATE '2024-03-12'),
    ('CUST-048', 'Xavier Moreau',    'xavier@example.com',      'EU-West',  'platinum', DATE '2024-09-18'),
    ('CUST-049', 'Yara Okafor',      'yara.o@example.com',      'US-East',  'inactive', DATE '2023-04-20'),
    ('CUST-050', 'Zach Bennett',     'zach.b@example.com',      'US-West',  'silver',   DATE '2024-10-30')
    AS t(customer_id, name, email, region, tier, signup_date)
    """)

# COMMAND ----------

# -- Products Table --
# ~30 rows. Used by schema enforcement exercises (product_id, name, category, price).
#
# Edge cases built in:
#   - $0 price: PROD-022
#   - Inactive products: PROD-015, PROD-028
#   - Null categories: PROD-019, PROD-030

if not _SETUP_SKIP:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.elt.products (
        product_id STRING,
        name STRING,
        category STRING,
        price DOUBLE,
        is_active BOOLEAN
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.elt.products
    SELECT * FROM VALUES
    ('PROD-001', 'Delta Lake Essentials Guide',     'books',        109.50, true),
    ('PROD-002', 'Spark SQL Quick Reference',       'books',        45.00,  true),
    ('PROD-003', 'Databricks Certification Prep',   'courses',      175.00, true),
    ('PROD-004', 'Unity Catalog Workshop',          'courses',      210.00, true),
    ('PROD-005', 'Production Pipeline Templates',   'templates',    320.00, true),
    ('PROD-006', 'Cluster Config Cheat Sheet',      'cheat-sheets', 35.00,  true),
    ('PROD-007', 'Spark Tuning Pocket Guide',       'books',        67.50,  true),
    ('PROD-008', 'Streaming Workshop Bundle',       'courses',      199.00, true),
    ('PROD-009', 'Enterprise Architecture Guide',   'books',        525.00, true),
    ('PROD-010', 'Job Orchestration Playbook',      'templates',    112.00, true),
    ('PROD-011', 'Delta Lake Deep Dive',            'courses',      249.99, true),
    ('PROD-012', 'PySpark Cookbook',                 'books',        89.00,  true),
    ('PROD-013', 'MLflow Integration Guide',        'books',        95.00,  true),
    ('PROD-014', 'Data Quality Framework',          'templates',    155.00, true),
    ('PROD-015', 'Legacy Hive Migration Guide',     'books',        75.00,  false),
    ('PROD-016', 'Medallion Architecture Kit',      'templates',    185.00, true),
    ('PROD-017', 'Auto Loader Mastery',             'courses',      145.00, true),
    ('PROD-018', 'Cost Optimization Playbook',      'templates',    130.00, true),
    ('PROD-019', 'Workspace Admin Toolkit',          NULL,          99.00,  true),
    ('PROD-020', 'CI/CD for Databricks',            'courses',      165.00, true),
    ('PROD-021', 'Spark Internals Deep Dive',       'courses',      225.00, true),
    ('PROD-022', 'Free Sample Notebook',            'samples',      0.00,   true),
    ('PROD-023', 'Governance Best Practices',       'books',        85.00,  true),
    ('PROD-024', 'Performance Tuning Lab',          'courses',      195.00, true),
    ('PROD-025', 'DLT Pipeline Starter',            'templates',    140.00, true),
    ('PROD-026', 'SQL Analytics Dashboard Kit',     'templates',    110.00, true),
    ('PROD-027', 'Databricks REST API Guide',       'books',        55.00,  true),
    ('PROD-028', 'Deprecated Connector Pack',       'templates',    65.00,  false),
    ('PROD-029', 'Interview Prep Bundle',           'courses',      299.00, true),
    ('PROD-030', 'Experimental Feature Preview',     NULL,          49.99,  true)
    AS t(product_id, name, category, price, is_active)
    """)

# COMMAND ----------

# -- Events Table --
# ~150 rows. Used by streaming, ELT, and performance exercises.
#
# Edge cases built in:
#   - Late arrivals: EVT-031 through EVT-035 have event_ts 30+ days before their neighbors
#   - Null payloads: EVT-011, EVT-044, EVT-077, EVT-110, EVT-143
#   - Duplicate event_ids: EVT-050 and EVT-100 each appear twice
#   - Event types: page_view, click, purchase, signup, logout, error, search

if not _SETUP_SKIP:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.elt.events (
        event_id STRING,
        user_id STRING,
        event_type STRING,
        event_ts TIMESTAMP,
        payload STRING
    ) USING DELTA
    """)

    # Build events programmatically for 150 rows (deterministic, no randomness)
    _event_types = ['page_view', 'click', 'purchase', 'signup', 'logout', 'error', 'search']
    _payloads = [
        '{"page": "/home"}',
        '{"button": "buy_now"}',
        '{"product_id": "PROD-001", "amount": 109.50}',
        '{"source": "google"}',
        '{"session_duration": 1200}',
        '{"code": 500, "message": "timeout"}',
        '{"query": "delta lake merge"}',
    ]
    _null_payload_ids = {11, 44, 77, 110, 143}
    _late_arrival_ids = {31, 32, 33, 34, 35}
    _duplicate_ids = {50, 100}

    _event_values = []
    for i in range(1, 151):
        eid = f"EVT-{i:03d}"
        uid = f"CUST-{((i - 1) % 50) + 1:03d}"
        etype = _event_types[(i - 1) % 7]

        # Base timestamp: 2025-06-01 + ~1 row per 3 hours across ~19 days
        day_offset = (i - 1) // 8
        hour_offset = ((i - 1) % 8) * 3
        if i in _late_arrival_ids:
            # Late arrivals: timestamp 45 days before expected
            ts = f"2025-{5 + (day_offset // 30):02d}-{((day_offset % 30) + 1):02d} {hour_offset:02d}:00:00"
        else:
            month = 6 + (day_offset // 30)
            day = (day_offset % 30) + 1
            if month > 12:
                month = 12
                day = min(day, 28)
            ts = f"2025-{month:02d}-{day:02d} {hour_offset:02d}:00:00"

        if i in _null_payload_ids:
            payload = "NULL"
        else:
            payload = f"'{_payloads[(i - 1) % 7]}'"

        _event_values.append(f"('{eid}', '{uid}', '{etype}', TIMESTAMP '{ts}', {payload})")

        # Add duplicate row for specific IDs
        if i in _duplicate_ids:
            # Duplicate with slightly different timestamp (simulates double-send)
            ts2 = ts.replace(":00:00", ":00:01")
            _event_values.append(f"('{eid}', '{uid}', '{etype}', TIMESTAMP '{ts2}', {payload})")

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.elt.events
    SELECT * FROM VALUES
    {','.join(_event_values)}
    AS t(event_id, user_id, event_type, event_ts, payload)
    """)

# COMMAND ----------

# -- Order Items Table --
# ~200 rows. Used by ELT join exercises and aggregation problems.
#
# Edge cases built in:
#   - Orphan order_ids (no matching order): ORD-201 through ORD-205
#   - quantity=0: rows 50, 100, 150
#   - Multiple items per order (most have 2, some have 3)

if not _SETUP_SKIP:
    _item_values = []
    _item_counter = 0

    # 2 items per order for ORD-001 through ORD-090 (180 rows), with edge cases
    for i in range(1, 91):
        oid = f"ORD-{i:03d}"
        for j in range(2):
            _item_counter += 1
            pid = f"PROD-{((i + j - 1) % 30) + 1:03d}"
            qty = 1 + ((_item_counter * 3) % 5)  # Deterministic: cycles through 1-5
            uprice = [109.50, 45.00, 175.00, 210.00, 320.00, 35.00, 67.50, 199.00, 525.00, 112.00][(_item_counter - 1) % 10]

            # Edge case: quantity=0 for specific items
            if _item_counter in (50, 100, 150):
                qty = 0

            _item_values.append(f"('{oid}', '{pid}', {qty}, {uprice})")

    # 3 items per order for ORD-091 through ORD-095 (15 rows - larger orders)
    for i in range(91, 96):
        oid = f"ORD-{i:03d}"
        for j in range(3):
            _item_counter += 1
            pid = f"PROD-{((i + j) % 30) + 1:03d}"
            qty = 1 + ((_item_counter * 3) % 5)
            uprice = [109.50, 45.00, 175.00, 210.00, 320.00, 35.00, 67.50, 199.00, 525.00, 112.00][(_item_counter - 1) % 10]
            _item_values.append(f"('{oid}', '{pid}', {qty}, {uprice})")

    # 5 orphan order_ids (no matching order in orders table): 10 rows
    for i in range(201, 206):
        oid = f"ORD-{i:03d}"
        for j in range(2):
            _item_counter += 1
            pid = f"PROD-{((i + j) % 30) + 1:03d}"
            qty = 1 + ((_item_counter * 3) % 5)
            uprice = [109.50, 45.00, 175.00, 210.00, 320.00][(_item_counter - 1) % 5]
            _item_values.append(f"('{oid}', '{pid}', {qty}, {uprice})")

    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.elt.order_items (
        order_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE
    ) USING DELTA
    """)

    spark.sql(f"""
    INSERT OVERWRITE {CATALOG}.elt.order_items
    SELECT * FROM VALUES
    {','.join(_item_values)}
    AS t(order_id, product_id, quantity, unit_price)
    """)

# COMMAND ----------

# -- Volumes for Ingestion Exercises --
# Creates volumes for Auto Loader and batch ingestion exercises.
# The per-exercise source files are created by each topic's setup notebook, not here.

# Auto Loader volumes (used by auto-loader-setup.py)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.auto_loader.source_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.auto_loader.checkpoints")

# Batch ingestion volume (used by batch-ingestion-setup.py)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.batch_ingestion.source_files")

# COMMAND ----------

# -- Confirmation --
if not _SETUP_SKIP:
    _orders_count = spark.table(f"{CATALOG}.elt.orders").count()
    _customers_count = spark.table(f"{CATALOG}.elt.customers").count()
    _products_count = spark.table(f"{CATALOG}.elt.products").count()
    _events_count = spark.table(f"{CATALOG}.elt.events").count()
    _items_count = spark.table(f"{CATALOG}.elt.order_items").count()

    print("=" * 60)
    print(f"  00_Setup complete. Catalog: {CATALOG}")
    print("=" * 60)
    print(f"  orders:      {_orders_count} rows")
    print(f"  customers:   {_customers_count} rows")
    print(f"  products:    {_products_count} rows")
    print(f"  events:      {_events_count} rows")
    print(f"  order_items: {_items_count} rows")
    print("=" * 60)
    print(f"  Zone schemas: delta_lake, elt, streaming, production, governance, performance, fundamentals")
    print(f"  Topic schemas: spark_sql_joins, window_functions, pyspark_transformations, auto_loader, batch_ingestion, medallion_architecture, complex_data_types")
    print(f"  Volumes: auto_loader.source_files, auto_loader.checkpoints, batch_ingestion.source_files")
    print("=" * 60)
