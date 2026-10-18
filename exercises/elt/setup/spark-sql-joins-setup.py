# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Setup: Spark SQL Joins & Aggregations
# MAGIC Creates the `db_code.spark_sql_joins` schema, sets the expected row counts the checks use,
# MAGIC and drops output tables from earlier runs. The exercise notebook runs it for you with `%run`.

# COMMAND ----------

CATALOG = "db_code"
SCHEMA = "spark_sql_joins"
BASE_SCHEMA = "elt"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Expected results for the checks (base data from 00_Setup.py is fixed).
JOINS_EX1_EXPECTED_COUNT = 103
JOINS_EX3_EXPECTED_COUNT = 10
JOINS_EX4_EXPECTED_COUNT = 9
JOINS_EX5_EXPECTED_COUNT = 5
JOINS_EX6_EXPECTED_COUNT = 92
JOINS_EX7_EXPECTED_COUNT = 14
JOINS_EX8_EXPECTED_GRAND_COUNT = 42

# COMMAND ----------

# Clean up any previous exercise output tables (idempotent)
for tbl in [
    "joins_ex1_output",
    "joins_ex2_output",
    "joins_ex3_output",
    "joins_ex4_output",
    "joins_ex5_output",
    "joins_ex6_output",
    "joins_ex7_output",
    "joins_ex8_rollup",
    "joins_ex9_cube",
]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{tbl}")

# COMMAND ----------

print(f"Setup complete. Exercise output tables go in {CATALOG}.{SCHEMA}.")
print(f"Expected counts: Ex1={JOINS_EX1_EXPECTED_COUNT}, Ex3={JOINS_EX3_EXPECTED_COUNT}, Ex4={JOINS_EX4_EXPECTED_COUNT}, Ex5={JOINS_EX5_EXPECTED_COUNT}, Ex6={JOINS_EX6_EXPECTED_COUNT}, Ex7={JOINS_EX7_EXPECTED_COUNT}, Ex8 grand total={JOINS_EX8_EXPECTED_GRAND_COUNT}")
