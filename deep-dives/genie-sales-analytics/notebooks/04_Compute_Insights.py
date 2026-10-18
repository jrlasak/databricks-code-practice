# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Compute Insights (the "daily job")
# MAGIC This is the notebook you would schedule as a daily job. For each region it decomposes the
# MAGIC **month-over-month revenue change** for the current month into ranked drivers (which categories and
# MAGIC products moved the most) and writes them to `performance_insights`. The `top_drivers()` and
# MAGIC `explain_driver()` UC functions read this table.
# MAGIC
# MAGIC In production that schedule is a **Lakeflow Job** (e.g. daily 06:00 on serverless). For the lab, `00_Run_All` already ran it once; re-run it only after you regenerate data.
# MAGIC Re-running is safe (`INSERT OVERWRITE`).
# MAGIC
# MAGIC **Prerequisites**: notebooks 01, 02, 03 must have run.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

import pandas as pd

# prior month relative to the anchor
_prev_m = CURRENT_MONTH - 1 if CURRENT_MONTH > 1 else 12
_prev_y = CURRENT_YEAR if CURRENT_MONTH > 1 else CURRENT_YEAR - 1
PRIOR_YEAR_MONTH = f"{_prev_y}-{_prev_m:02d}"
print(f"Analyzing {CURRENT_YEAR_MONTH} vs prior month {PRIOR_YEAR_MONTH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull region × category and region × product revenue for both months

# COMMAND ----------

cat_df = spark.sql(f"""
SELECT s.region, p.category AS dim_value, d.year_month, SUM(f.net_sales) AS rev
FROM {FACT_SALES} f
JOIN {DIM_STORE} s   ON f.store_key = s.store_key
JOIN {DIM_PRODUCT} p ON f.product_key = p.product_key
JOIN {DIM_DATE} d    ON f.date_key = d.date_key
WHERE d.year_month IN ('{CURRENT_YEAR_MONTH}', '{PRIOR_YEAR_MONTH}')
GROUP BY s.region, p.category, d.year_month
""").toPandas()

prod_df = spark.sql(f"""
SELECT s.region, p.product_name AS dim_value, p.category, d.year_month, SUM(f.net_sales) AS rev
FROM {FACT_SALES} f
JOIN {DIM_STORE} s   ON f.store_key = s.store_key
JOIN {DIM_PRODUCT} p ON f.product_key = p.product_key
JOIN {DIM_DATE} d    ON f.date_key = d.date_key
WHERE d.year_month IN ('{CURRENT_YEAR_MONTH}', '{PRIOR_YEAR_MONTH}')
GROUP BY s.region, p.product_name, p.category, d.year_month
""").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decompose into ranked drivers per region

# COMMAND ----------

def pivot_delta(df, keep_category=False):
    cols = ["region", "dim_value"] + (["category"] if keep_category else [])
    cur = df[df.year_month == CURRENT_YEAR_MONTH].groupby(cols)["rev"].sum().rename("revenue_current")
    pri = df[df.year_month == PRIOR_YEAR_MONTH].groupby(cols)["rev"].sum().rename("revenue_prior")
    out = pd.concat([cur, pri], axis=1).fillna(0.0).reset_index()
    out["revenue_delta"] = out["revenue_current"] - out["revenue_prior"]
    return out

cat = pivot_delta(cat_df)
prod = pivot_delta(prod_df, keep_category=True)

rows = []
for region in REGIONS:
    cregion = cat[cat.region == region].copy()
    # region's total decline = sum of negative category deltas (a negative number)
    total_decline = cregion.loc[cregion.revenue_delta < 0, "revenue_delta"].sum()
    total_decline = total_decline if total_decline < 0 else -1.0  # guard against /0

    # CATEGORY drivers
    cat_rows = []
    for _, r in cregion.iterrows():
        if r.revenue_delta < 0:
            contrib = round(100.0 * r.revenue_delta / total_decline, 1)
            insight = (f"{r.dim_value} in {region} fell from ${round(r.revenue_prior):,} to "
                       f"${round(r.revenue_current):,} (-${abs(round(r.revenue_delta)):,} month over month, "
                       f"{contrib:.0f}% of the region's category declines).")
        else:
            # a growing category offsets the other categories' declines; it is not part of them
            contrib = 0.0
            insight = (f"{r.dim_value} in {region} grew from ${round(r.revenue_prior):,} to "
                       f"${round(r.revenue_current):,} (+${abs(round(r.revenue_delta)):,} month over month, "
                       f"offsetting part of the other categories' declines).")
        cat_rows.append({
            "region": region, "year_month": CURRENT_YEAR_MONTH, "driver_type": "CATEGORY",
            "dimension_value": r.dim_value,
            "revenue_current": round(r.revenue_current), "revenue_prior": round(r.revenue_prior),
            "revenue_delta": round(r.revenue_delta),
            "contribution_to_gap_pct": contrib,
            "insight": insight,
        })

    # PRODUCT drivers - drill into the single worst category for this region
    worst_cat = cregion.sort_values("revenue_delta").iloc[0]["dim_value"]
    pregion = prod[(prod.region == region) & (prod.category == worst_cat)].copy()
    prod_rows = []
    for _, r in pregion.sort_values("revenue_delta").head(3).iterrows():
        if r.revenue_delta >= 0:
            continue
        contrib = round(100.0 * r.revenue_delta / total_decline, 1)
        prod_rows.append({
            "region": region, "year_month": CURRENT_YEAR_MONTH, "driver_type": "PRODUCT",
            "dimension_value": r.dim_value,
            "revenue_current": round(r.revenue_current), "revenue_prior": round(r.revenue_prior),
            "revenue_delta": round(r.revenue_delta),
            "contribution_to_gap_pct": contrib,
            "insight": (f"{r.dim_value} ({worst_cat}) in {region} fell from ${round(r.revenue_prior):,} "
                        f"to ${round(r.revenue_current):,} (-${abs(round(r.revenue_delta)):,} MoM). "
                        f"Part of the {worst_cat} decline, not additional to it."),
        })

    # combine, rank by most negative delta first, keep top 8
    combined = pd.DataFrame(cat_rows + prod_rows).sort_values("revenue_delta").head(8).reset_index(drop=True)
    combined["rank"] = combined.index + 1
    rows.append(combined)

insights = pd.concat(rows, ignore_index=True)[[
    "region", "year_month", "rank", "driver_type", "dimension_value",
    "revenue_current", "revenue_prior", "revenue_delta", "contribution_to_gap_pct", "insight"]]
print(f"performance_insights rows: {len(insights)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to performance_insights

# COMMAND ----------

spark.createDataFrame(insights).createOrReplaceTempView("v_insights")
spark.sql(f"""
INSERT OVERWRITE TABLE {PERFORMANCE_INSIGHTS}
SELECT region, year_month, CAST(rank AS INT), driver_type, dimension_value,
       CAST(revenue_current AS DOUBLE), CAST(revenue_prior AS DOUBLE), CAST(revenue_delta AS DOUBLE),
       CAST(contribution_to_gap_pct AS DOUBLE), insight
FROM v_insights
""")
print(f"{PERFORMANCE_INSIGHTS}: {spark.table(PERFORMANCE_INSIGHTS).count()} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spot-check the West and the UC functions

# COMMAND ----------

print(f"== top_drivers('{HERO_REGION}') ==")
spark.sql(f"SELECT * FROM {FQ}.top_drivers('{HERO_REGION}') ORDER BY rank").show(truncate=False)

print(f"== explain_driver('{HERO_REGION}', 1) ==")
spark.sql(f"SELECT * FROM {FQ}.explain_driver('{HERO_REGION}', 1)").show(truncate=False)
print(f"Expected: rank 1 is '{HERO_DETRACTOR_CATEGORY}' as the biggest detractor in the {HERO_REGION}.")
