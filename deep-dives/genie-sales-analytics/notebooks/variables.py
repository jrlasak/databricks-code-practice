# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration
# MAGIC Centralized names and constants. Every other notebook pulls these in with `%run ./variables`.
# MAGIC Change `CATALOG` / `SCHEMA` here once and the whole lab follows.

# COMMAND ----------

# --- Unity Catalog namespace ---
CATALOG = "bramblepeak_retail"          # created idempotently by 01_Setup_Tables
SCHEMA = "sales"                   # the gold star schema Genie connects to
FQ = f"{CATALOG}.{SCHEMA}"         # fully-qualified prefix, e.g. bramblepeak_retail.sales

# --- Star schema tables ---
DIM_DATE = f"{FQ}.dim_date"
DIM_PRODUCT = f"{FQ}.dim_product"
DIM_CUSTOMER = f"{FQ}.dim_customer"
DIM_STORE = f"{FQ}.dim_store"
FACT_SALES = f"{FQ}.fact_sales"
SALES_TARGETS = f"{FQ}.sales_targets"
PERFORMANCE_INSIGHTS = f"{FQ}.performance_insights"

# --- Time anchor (derived from TODAY so the lab never goes stale) ---
# "This month" = the actual current calendar month. The data spans the trailing 12 months ending
# with it, and the baked-in storyline is always planted in the current month. So whenever a learner
# runs this lab, "this month" is now - and the Genie guides self-anchor on MAX(year_month), so they
# never need a hardcoded date either.
import calendar as _calendar
from datetime import date as _date

_today = _date.today()
CURRENT_YEAR = _today.year
CURRENT_MONTH = _today.month
CURRENT_YEAR_MONTH = _today.strftime("%Y-%m")   # the "today" anchor used throughout the demo


def _add_months(year, month, delta):
    idx = (year * 12 + (month - 1)) + delta
    return idx // 12, idx % 12 + 1


_sy, _sm = _add_months(CURRENT_YEAR, CURRENT_MONTH, -11)     # 11 months back = 12-month window
START_DATE = _date(_sy, _sm, 1).isoformat()
_last_day = _calendar.monthrange(CURRENT_YEAR, CURRENT_MONTH)[1]
END_DATE = _date(CURRENT_YEAR, CURRENT_MONTH, _last_day).isoformat()

# --- Determinism ---
RANDOM_SEED = 42

# --- Categorical domains (kept small on purpose) ---
REGIONS = ["West", "Midwest", "Northeast", "South"]
CHANNELS = ["Retail", "Online"]
LOYALTY_TIERS = ["Standard", "Plus", "VIP"]
CATEGORIES = [
    "Outdoor & Camping",
    "Apparel",
    "Footwear",
    "Electronics",
    "Home & Kitchen",
    "Sports & Fitness",
    "Travel & Bags",
    "Accessories",
]

# --- Baked-in narrative anchors (the "hero" storylines, always planted in the current month) ---
# H1 on-track: company is BEHIND target this month.
# H2 why: the West region is dragging; Outdoor & Camping collapsed there.
# H3 driver: Outdoor & Camping is the #1 detractor in the West.
# H4/H5 what-if: discount the hero product to see margin impact.
HERO_REGION = "West"                       # the region that misses target
HERO_DETRACTOR_CATEGORY = "Outdoor & Camping"
HERO_DISCOUNT_PRODUCT_ID = "SKU-1042"      # "Summit 600 Down Jacket" - the discount what-if anchor
HERO_SURGE_PRODUCT_ID = "SKU-1009"         # "Aera Wireless Earbuds Pro" - online surge bright spot

print(f"Config loaded. Target namespace: {FQ}")
print(f"Current month anchor: {CURRENT_YEAR_MONTH}  (data spans {START_DATE} to {END_DATE})")
print(f"In the Genie guides, 'this month' = the latest month in the data: "
      f"SELECT MAX(year_month) FROM {DIM_DATE}  -> {CURRENT_YEAR_MONTH}")
