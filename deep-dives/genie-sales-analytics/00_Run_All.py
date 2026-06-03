# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Run All
# MAGIC One-click setup for the whole data layer. Runs notebooks 01 → 04 in order:
# MAGIC
# MAGIC 1. **01_Setup_Tables** - catalog `aurora_retail`, schema `sales`, 7 star-schema tables (with comments).
# MAGIC 2. **02_Generate_Data** - deterministic synthetic sales with the baked-in storyline.
# MAGIC 3. **03_Setup_Functions** - the 3 Unity Catalog functions Genie calls.
# MAGIC 4. **04_Compute_Insights** - fills `performance_insights` (the "daily job").
# MAGIC
# MAGIC Re-running is safe - every step uses `CREATE OR REPLACE` / `IF NOT EXISTS` / `INSERT OVERWRITE`.
# MAGIC Expected runtime: ~3-6 min on serverless (notebook 02 is the slow one).
# MAGIC
# MAGIC > Prefer a real orchestrator? Create a **Databricks Workflow** with four notebook tasks
# MAGIC > (01 → 02 → 03 → 04) chained by dependency, on serverless compute. This run-all notebook does
# MAGIC > the same thing in one click for the lab.

# COMMAND ----------

# MAGIC %run ./notebooks/01_Setup_Tables

# COMMAND ----------

# MAGIC %run ./notebooks/02_Generate_Data

# COMMAND ----------

# MAGIC %run ./notebooks/03_Setup_Functions

# COMMAND ----------

# MAGIC %run ./notebooks/04_Compute_Insights

# COMMAND ----------

# MAGIC %run ./notebooks/variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final verification

# COMMAND ----------

print("Tables in the schema:")
spark.sql(f"""
SELECT table_name FROM {CATALOG}.information_schema.tables
WHERE table_schema = '{SCHEMA}' ORDER BY table_name
""").show(truncate=False)

print("Row counts:")
for t in [DIM_DATE, DIM_PRODUCT, DIM_CUSTOMER, DIM_STORE, FACT_SALES, SALES_TARGETS, PERFORMANCE_INSIGHTS]:
    print(f"  {t}: {spark.table(t).count():,}")

print("\nUC functions:")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"SHOW USER FUNCTIONS IN {SCHEMA}").show(truncate=False)

print("\n✅ Data layer ready. Next: open genie-setup/01_Genie_Space_Setup.md and build the Genie space.")
