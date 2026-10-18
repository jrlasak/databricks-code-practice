# Databricks notebook source
# MAGIC %md
# MAGIC # Shared names and helpers
# MAGIC Every notebook in this lab loads this one with `%run ./variables`: the catalog and schema names, the table registry, the `sales_raw` column definitions, the two lookups every layout is measured on (`benchmark()` with `country`, `benchmark_any_country()` without it) and the imports.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col

CATALOG_NAME = "delta_optimization_project"
SCHEMA_NAME = "sales_data"

# Fully qualified name of every table the lab builds, by short key: tbl('raw')
TABLES = {
    "raw": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_raw",
    "partitioned": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_partitioned",
    "raw_zorder": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_raw_zorder",
    "to_compact": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_to_compact",
    "auto_compact": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_auto_compact",
    "liquid_clustered": f"{CATALOG_NAME}.{SCHEMA_NAME}.sales_liquid_clustered",
}


def tbl(key: str) -> str:
    """Return the fully qualified table name for a short key."""
    return TABLES[key]


# Column definitions of sales_raw, reused by the Z-Order and Liquid Clustering copies
SALES_COLUMNS = """
  seq BIGINT, transaction_id STRING, customer_id INT, product_id INT,
  sale_date DATE, quantity INT, unit_price DOUBLE, country STRING
"""


# The account-support lookup every layout is measured on:
# one account, one product, and that account's country
BENCH_CUSTOMER_ID = 1500
BENCH_PRODUCT_ID = 250
BENCH_COUNTRY = "United States"


def benchmark(key: str):
    """The benchmark lookup against one table, as a DataFrame."""
    return spark.read.table(tbl(key)).where(
        (col("customer_id") == BENCH_CUSTOMER_ID)
        & (col("product_id") == BENCH_PRODUCT_ID)
        & (col("country") == BENCH_COUNTRY)
    )


def benchmark_any_country(key: str):
    """The same lookup without the country filter: one account and one product only."""
    return spark.read.table(tbl(key)).where(
        (col("customer_id") == BENCH_CUSTOMER_ID)
        & (col("product_id") == BENCH_PRODUCT_ID)
    )
