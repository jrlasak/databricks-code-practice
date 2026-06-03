# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Setup Functions
# MAGIC Creates the 3 Unity Catalog functions Genie calls when SQL alone is the wrong tool. These are what
# MAGIC let Genie **Agent mode** act like an analyst with tools instead of guessing SQL:
# MAGIC
# MAGIC - `simulate_discount(product_id, discount_pct)` - a **what-if**: projects units, revenue, and margin
# MAGIC   if you discount a product. Can't be done from historical SQL alone, so it must be a function.
# MAGIC - `top_drivers(region)` - reads the precomputed `performance_insights` table and returns the ranked
# MAGIC   drivers of a region's gap to target (scoped to the latest precomputed month automatically).
# MAGIC - `explain_driver(region, rank)` - a one-sentence natural-language rationale for one driver.
# MAGIC
# MAGIC Each function's `COMMENT` is the prompt Genie reads to decide whether to call it. The what/how/avoid
# MAGIC structure below is deliberate - tune it here and re-run if Genie ever writes raw SQL instead of calling.
# MAGIC
# MAGIC **Prerequisites**: notebooks 01 and 02 must have run. `top_drivers` / `explain_driver` return empty
# MAGIC until notebook 04 populates `performance_insights` - that is expected, not an error.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## simulate_discount - what-if margin impact (the Agent hero)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.simulate_discount(
    p_product_id STRING    COMMENT 'Product code to discount, e.g. SKU-1042 (Summit 600 Down Jacket).',
    p_discount_pct DOUBLE  COMMENT 'Discount percent to apply, e.g. 15 for a 15 percent markdown.'
)
RETURNS TABLE(
    product_id STRING,
    product_name STRING,
    list_price DOUBLE,
    discount_pct DOUBLE,
    discounted_price DOUBLE,
    baseline_monthly_units DOUBLE,
    projected_monthly_units DOUBLE,
    baseline_monthly_revenue DOUBLE,
    projected_monthly_revenue DOUBLE,
    baseline_monthly_margin DOUBLE,
    projected_monthly_margin DOUBLE,
    projected_margin_pct DOUBLE,
    margin_safe BOOLEAN
)
COMMENT 'Projects the revenue and margin impact of marking a product down by p_discount_pct, using a fixed price-elasticity assumption (a 10 percent discount lifts units ~15 percent) against the product''s 12-month average monthly volume. Returns one row: current vs discounted price, baseline vs projected monthly units/revenue/margin, projected margin percent, and a margin_safe flag (TRUE only if the discounted price still clears unit cost and projected margin stays at or above 15 percent). HOW TO USE: call when the user asks "what if we discount X by Y percent", "what would a markdown do to margin", or any pricing what-if. CALL SYNTAX: positional arguments only - SELECT * FROM {FQ}.simulate_discount(''SKU-1042'', 15). WHAT TO AVOID: do NOT use named-parameter syntax (p_product_id => ...) - Spark SQL does not support named parameters for UC table functions; do NOT answer pricing what-ifs with raw SQL over historical sales - always call this function so the elasticity and the margin-safety check are applied.'
RETURN
WITH baseline AS (
  SELECT
    SUM(quantity) / 12.0       AS baseline_monthly_units,
    SUM(net_sales) / 12.0      AS baseline_monthly_revenue,
    SUM(gross_margin) / 12.0   AS baseline_monthly_margin
  FROM {FACT_SALES} f
  JOIN {DIM_PRODUCT} p ON f.product_key = p.product_key
  WHERE p.product_id = p_product_id
),
prod AS (
  SELECT product_id, product_name, list_price, unit_cost
  FROM {DIM_PRODUCT} WHERE product_id = p_product_id
),
calc AS (
  SELECT
    prod.product_id,
    prod.product_name,
    prod.list_price,
    p_discount_pct AS discount_pct,
    ROUND(prod.list_price * (1 - p_discount_pct / 100.0), 2) AS discounted_price,
    prod.unit_cost,
    baseline.baseline_monthly_units,
    baseline.baseline_monthly_revenue,
    baseline.baseline_monthly_margin,
    -- elasticity: +1.5 percent units per 1 percent discount
    baseline.baseline_monthly_units * (1 + 1.5 * p_discount_pct / 100.0) AS projected_monthly_units
  FROM prod CROSS JOIN baseline
)
SELECT
  product_id,
  product_name,
  ROUND(list_price, 2)                                          AS list_price,
  discount_pct,
  discounted_price,
  ROUND(baseline_monthly_units, 0)                              AS baseline_monthly_units,
  ROUND(projected_monthly_units, 0)                             AS projected_monthly_units,
  ROUND(baseline_monthly_revenue, 0)                            AS baseline_monthly_revenue,
  ROUND(projected_monthly_units * discounted_price, 0)          AS projected_monthly_revenue,
  ROUND(baseline_monthly_margin, 0)                             AS baseline_monthly_margin,
  ROUND(projected_monthly_units * (discounted_price - unit_cost), 0) AS projected_monthly_margin,
  ROUND(100.0 * (discounted_price - unit_cost) / NULLIF(discounted_price, 0), 1) AS projected_margin_pct,
  (discounted_price > unit_cost
   AND (100.0 * (discounted_price - unit_cost) / NULLIF(discounted_price, 0)) >= 15.0) AS margin_safe
FROM calc
""")
print("Created simulate_discount")

# COMMAND ----------

# MAGIC %md
# MAGIC ## top_drivers - read precomputed ranked gap drivers

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.top_drivers(
    p_region STRING      COMMENT 'Region to analyze, e.g. West.'
)
RETURNS TABLE(
    rank INT,
    driver_type STRING,
    dimension_value STRING,
    revenue_current DOUBLE,
    revenue_prior DOUBLE,
    revenue_delta DOUBLE,
    contribution_to_gap_pct DOUBLE,
    insight STRING
)
COMMENT 'Returns the ranked drivers (rank 1 = biggest) of a region''s revenue decline for the most recent precomputed month, from the performance_insights table. The function scopes to the current (latest) month automatically, so NO month argument is needed. HOW TO USE: call when the user asks "what is driving the gap in [region]", "what is dragging down [region] this month", "top detractors for [region]", or "why is [region] behind". CALL SYNTAX: positional argument only - SELECT * FROM {FQ}.top_drivers(''West'') ORDER BY rank. WHAT TO AVOID: do NOT use named-parameter syntax; do NOT recompute this with raw SQL - call the function so the ranking and gap-contribution logic stay governed and consistent. Returns empty if notebook 04 has not been run yet (that is expected, not an error). (Production note: a real system would add a year_month parameter and keep monthly history; this lab precomputes only the current month.)'
RETURN
SELECT rank, driver_type, dimension_value, revenue_current, revenue_prior,
       revenue_delta, contribution_to_gap_pct, insight
FROM {PERFORMANCE_INSIGHTS}
WHERE region = p_region
  AND year_month = (SELECT MAX(year_month) FROM {PERFORMANCE_INSIGHTS})
ORDER BY rank ASC
""")
print("Created top_drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## explain_driver - natural-language rationale for one driver

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.explain_driver(
    p_region STRING      COMMENT 'Region, e.g. West.',
    p_rank INT           COMMENT 'Which driver to explain (1 is the biggest).'
)
RETURNS TABLE(explanation STRING)
COMMENT 'Returns a one-sentence natural-language explanation of a single ranked gap driver for a region, scoped to the most recent precomputed month (no month argument needed). HOW TO USE: call when the user asks "why" or "explain" about a specific driver already surfaced by top_drivers; pass the region and the rank (1, 2, 3...). CALL SYNTAX: positional arguments only - SELECT * FROM {FQ}.explain_driver(''West'', 1). WHAT TO AVOID: do NOT use named-parameter syntax; do NOT call before top_drivers has returned rows for the same region - the result will be empty.'
RETURN
SELECT MAX(insight) AS explanation
FROM {PERFORMANCE_INSIGHTS}
WHERE region = p_region AND rank = p_rank
  AND year_month = (SELECT MAX(year_month) FROM {PERFORMANCE_INSIGHTS})
""")
print("Created explain_driver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smoke test - simulate_discount works standalone (it reads no precomputed table)

# COMMAND ----------

print(f"simulate_discount test - 15% markdown on {HERO_DISCOUNT_PRODUCT_ID} (Summit 600 Down Jacket):")
spark.sql(f"SELECT * FROM {FQ}.simulate_discount('{HERO_DISCOUNT_PRODUCT_ID}', 15)").show(truncate=False)
print("Expected: discounted_price ~ $186, projected units up ~22%, margin_safe = true.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all 3 functions exist

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"SHOW USER FUNCTIONS IN {SCHEMA}").show(truncate=False)
print("Next: notebook 04 populates performance_insights so top_drivers / explain_driver return rows.")
