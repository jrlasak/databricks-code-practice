# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Data
# MAGIC Generates deterministic synthetic data (seed=42) for Aurora Outfitters and writes it into the
# MAGIC 7 tables from notebook 01. The data has a **baked-in storyline** so every demo question lands:
# MAGIC
# MAGIC - **The company misses its sales target this month.** (drives "are we on track this month?")
# MAGIC - **The West region is the drag.** (drives "why are we behind?")
# MAGIC - **Outdoor & Camping collapsed in the West.** (drives "what's the biggest detractor?")
# MAGIC - **Aera Wireless Earbuds Pro (SKU-1009) surged online.** (a bright spot)
# MAGIC - **Summit 600 Down Jacket (SKU-1042)** has steady, healthy-margin volume - the discount what-if anchor.
# MAGIC
# MAGIC Re-running is safe: every table is `INSERT OVERWRITE`-d, so the DDL/comments from notebook 01 stay intact.

# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import date

np.random.seed(RANDOM_SEED)
print(f"Generating data for {FQ} | anchor month {CURRENT_YEAR_MONTH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------

dates = pd.date_range(START_DATE, END_DATE, freq="D")
dim_date = pd.DataFrame({
    "date_key": dates.strftime("%Y%m%d").astype(int),
    "date": [d.date() for d in dates],
    "day_name": dates.strftime("%A"),
    "week_of_year": dates.isocalendar().week.astype(int).values,
    "month": dates.month,
    "month_name": dates.strftime("%B"),
    "year_month": dates.strftime("%Y-%m"),
    "quarter": dates.quarter,
    "year": dates.year,
    "is_weekend": dates.dayofweek >= 5,
})
ALL_MONTHS = sorted(dim_date["year_month"].unique())
print(f"dim_date: {len(dim_date)} days across {len(ALL_MONTHS)} months ({ALL_MONTHS[0]}..{ALL_MONTHS[-1]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_product (60 SKUs, 2 hand-placed heroes)

# COMMAND ----------

CAT_PRICE = {  # (list_price low, high) per category
    "Outdoor & Camping": (20, 340), "Apparel": (25, 240), "Footwear": (45, 185),
    "Electronics": (40, 400), "Home & Kitchen": (15, 200), "Sports & Fitness": (12, 150),
    "Travel & Bags": (30, 220), "Accessories": (8, 90),
}
CAT_SUBS = {
    "Outdoor & Camping": ["Tents", "Sleeping Bags", "Backpacks", "Cookware"],
    "Apparel": ["Outerwear", "Tops", "Bottoms", "Base Layers"],
    "Footwear": ["Hiking Boots", "Trail Runners", "Sandals", "Casual"],
    "Electronics": ["Audio", "Wearables", "Action Cams", "Power"],
    "Home & Kitchen": ["Drinkware", "Cookware", "Storage", "Lighting"],
    "Sports & Fitness": ["Yoga", "Strength", "Cycling", "Recovery"],
    "Travel & Bags": ["Duffels", "Daypacks", "Luggage", "Slings"],
    "Accessories": ["Headwear", "Gloves", "Socks", "Belts"],
}
BRANDS = ["Summit", "Aera", "Northbound", "Trailhead", "Cascade", "Vertex", "Drift", "Outpost"]
NOUNS = {
    "Outdoor & Camping": ["Ridge Tent", "Alpine Sleeping Bag", "65L Backpack", "Camp Stove", "Trail Lantern"],
    "Apparel": ["Fleece Pullover", "Rain Shell", "Hiking Pants", "Merino Tee", "Insulated Vest"],
    "Footwear": ["Trail Runner", "Hiking Boot", "Trekking Sandal", "Approach Shoe", "Camp Slip-On"],
    "Electronics": ["Wireless Earbuds", "GPS Watch", "Action Cam", "Power Bank", "Headlamp"],
    "Home & Kitchen": ["Insulated Bottle", "Camp Mug", "Storage Crate", "Cast Pan", "LED Lantern"],
    "Sports & Fitness": ["Yoga Mat", "Kettlebell", "Cycling Bottle", "Foam Roller", "Resistance Set"],
    "Travel & Bags": ["Travel Duffel", "Daypack", "Carry-On", "Sling Bag", "Packing Cubes"],
    "Accessories": ["Trail Cap", "Liner Gloves", "Wool Socks", "Utility Belt", "Beanie"],
}

products = []
for i in range(60):
    cat = CATEGORIES[i % len(CATEGORIES)]
    sub = NOUNS[cat][i % len(NOUNS[cat])]
    brand = BRANDS[i % len(BRANDS)]
    lo, hi = CAT_PRICE[cat]
    list_price = round(float(np.random.uniform(lo, hi)), 2)
    unit_cost = round(list_price * float(np.random.uniform(0.42, 0.58)), 2)
    pop = float(np.random.gamma(2.0, 1.0)) + 0.3
    products.append({
        "product_id": f"SKU-{1001 + i}",
        "product_name": f"{brand} {sub}",
        "category": cat,
        "subcategory": CAT_SUBS[cat][i % len(CAT_SUBS[cat])],
        "brand": brand,
        "unit_cost": unit_cost,
        "list_price": list_price,
        "_pop": pop,
    })

# Hero overrides - keep their auto-assigned ids (index 8 -> SKU-1009, index 41 -> SKU-1042)
products[8].update({
    "product_id": HERO_SURGE_PRODUCT_ID, "product_name": "Aera Wireless Earbuds Pro",
    "category": "Electronics", "subcategory": "Audio", "brand": "Aera",
    "unit_cost": 38.0, "list_price": 129.0, "_pop": 6.0,
})
products[41].update({
    "product_id": HERO_DISCOUNT_PRODUCT_ID, "product_name": "Summit 600 Down Jacket",
    "category": "Apparel", "subcategory": "Outerwear", "brand": "Summit",
    "unit_cost": 92.0, "list_price": 219.0, "_pop": 5.5,
})

dim_product = pd.DataFrame(products)
dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))
PROD_POP = dim_product["_pop"].values
print(f"dim_product: {len(dim_product)} SKUs. Heroes: {HERO_SURGE_PRODUCT_ID} (surge), {HERO_DISCOUNT_PRODUCT_ID} (discount what-if).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer (500)

# COMMAND ----------

FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David",
         "Elizabeth", "William", "Barbara", "Maria", "Carlos", "Aisha", "Wei", "Sofia", "Liam",
         "Noah", "Olivia", "Emma", "Ava", "Diego", "Priya", "Chen", "Fatima", "Lucas", "Mia"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
        "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Lee",
        "Nguyen", "Kim", "Patel", "Chen", "Okafor", "Ali", "Reyes", "Murphy", "Bauer"]
REGION_W = [0.30, 0.22, 0.26, 0.22]   # West, Midwest, Northeast, South

n_cust = 500
cust_region = np.random.choice(REGIONS, size=n_cust, p=REGION_W)
dim_customer = pd.DataFrame({
    "customer_key": range(1, n_cust + 1),
    "customer_id": [f"CUST-{100000 + i}" for i in range(n_cust)],
    "customer_name": [f"{np.random.choice(FIRST)} {np.random.choice(LAST)}" for _ in range(n_cust)],
    "loyalty_tier": np.random.choice(LOYALTY_TIERS, size=n_cust, p=[0.55, 0.30, 0.15]),
    "region": cust_region,
    "country": "USA",
    "signup_date": [date(2022, 1, 1) + pd.Timedelta(days=int(np.random.randint(0, 1100))) for _ in range(n_cust)],
})
# region -> list of customer_keys (for regional sampling in the fact)
CUST_BY_REGION = {r: dim_customer.loc[dim_customer.region == r, "customer_key"].values for r in REGIONS}
print(f"dim_customer: {len(dim_customer)} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_store (4 retail + 1 online per region = 20)

# COMMAND ----------

REGION_CITIES = {
    "West": ["Denver", "Seattle", "Phoenix", "Portland"],
    "Midwest": ["Chicago", "Minneapolis", "Columbus", "Kansas City"],
    "Northeast": ["Boston", "New York", "Philadelphia", "Pittsburgh"],
    "South": ["Austin", "Atlanta", "Nashville", "Miami"],
}
store_rows = []
skey = 1
for region in REGIONS:
    cities = REGION_CITIES[region]
    for j, city in enumerate(cities):
        name = f"{city} Flagship" if j == 0 else f"{city} Store"
        store_rows.append({"store_key": skey, "store_id": f"STR-{skey:02d}", "store_name": name,
                           "channel": "Retail", "city": city, "region": region, "country": "USA"})
        skey += 1
    # one online store per region (hub city = first city)
    store_rows.append({"store_key": skey, "store_id": f"STR-{skey:02d}", "store_name": f"Online - {region}",
                       "channel": "Online", "city": cities[0], "region": region, "country": "USA"})
    skey += 1
dim_store = pd.DataFrame(store_rows)
# (region, channel) -> store_keys
STORE_BY_RC = {(r, c): dim_store.loc[(dim_store.region == r) & (dim_store.channel == c), "store_key"].values
               for r in REGIONS for c in CHANNELS}
print(f"dim_store: {len(dim_store)} stores ({dim_store.channel.value_counts().to_dict()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales (~120k order lines) with seasonality + planted storyline

# COMMAND ----------

CAT_W = {"Outdoor & Camping": 0.14, "Apparel": 0.18, "Footwear": 0.12, "Electronics": 0.15,
         "Home & Kitchen": 0.13, "Sports & Fitness": 0.11, "Travel & Bags": 0.09, "Accessories": 0.08}
CHANNEL_W = [0.62, 0.38]   # Retail, Online
# product_keys grouped by category, with within-category popularity
PK_BY_CAT, POP_BY_CAT = {}, {}
for cat in CATEGORIES:
    sub = dim_product[dim_product.category == cat]
    PK_BY_CAT[cat] = sub["product_key"].values
    w = sub["_pop"].values
    POP_BY_CAT[cat] = w / w.sum()

# Gentle volume drift by POSITION in the window (not calendar month) so the storyline lands the same
# way no matter which month "today" is. The last two positions are equal, so the current month and the
# prior month sit at the same baseline - that keeps the month-over-month driver decomposition (notebook
# 04) clean: the only big mover is the planted West collapse, not a seasonal swing.
POS_FACTORS = [0.96, 0.99, 1.04, 1.07, 1.05, 1.00, 0.97, 1.02, 1.05, 1.03, 1.00, 1.00]
SEASON = {ym: POS_FACTORS[i] for i, ym in enumerate(ALL_MONTHS)}
BASE_LINES = 9500   # per "normal" month
prod_lookup = dim_product.set_index("product_key")[["list_price", "unit_cost"]].to_dict("index")

rows = []
sale_seq = 0
for ym in ALL_MONTHS:
    n = int(BASE_LINES * SEASON[ym])
    days_in_month = (dim_date.year_month == ym).sum()
    region_arr = np.random.choice(REGIONS, size=n, p=REGION_W)
    channel_arr = np.random.choice(CHANNELS, size=n, p=CHANNEL_W)
    cat_arr = np.random.choice(CATEGORIES, size=n, p=[CAT_W[c] for c in CATEGORIES])
    day_arr = np.random.randint(1, days_in_month + 1, size=n)
    qty_arr = np.random.choice([1, 2, 3, 4], size=n, p=[0.55, 0.30, 0.10, 0.05])
    # promo discount pct per line (mostly 0)
    disc_arr = np.random.choice([0, 0, 0, 5, 10, 15, 20], size=n)
    y, mo = int(ym.split("-")[0]), int(ym.split("-")[1])
    first_key = int(f"{y}{mo:02d}01")
    for i in range(n):
        region, channel, cat = region_arr[i], channel_arr[i], cat_arr[i]
        pk = int(np.random.choice(PK_BY_CAT[cat], p=POP_BY_CAT[cat]))
        # customer: 70% same region as store, else any
        if np.random.rand() < 0.70:
            ck = int(np.random.choice(CUST_BY_REGION[region]))
        else:
            ck = int(np.random.choice(dim_customer["customer_key"].values))
        store_keys = STORE_BY_RC[(region, channel)]
        sk = int(np.random.choice(store_keys))
        dkey = int(f"{y}{mo:02d}{day_arr[i]:02d}")
        qty = int(qty_arr[i])
        lp = prod_lookup[pk]["list_price"]
        uc = prod_lookup[pk]["unit_cost"]
        disc_pct = int(disc_arr[i])
        gross = qty * lp
        discount_amount = round(gross * disc_pct / 100.0, 2)
        net = round(gross - discount_amount, 2)
        cost = round(qty * uc, 2)
        sale_seq += 1
        rows.append((f"SL-{sale_seq:08d}", f"ORD-{dkey}-{ck:05d}", dkey, pk, ck, sk,
                     qty, round(lp, 2), discount_amount, net, cost, round(net - cost, 2)))

fact = pd.DataFrame(rows, columns=[
    "sale_id", "order_id", "date_key", "product_key", "customer_key", "store_key",
    "quantity", "unit_price", "discount_amount", "net_sales", "cost_amount", "gross_margin"])
print(f"fact_sales (pre-storyline): {len(fact):,} lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plant the storyline (deterministic edits on the current month only)

# COMMAND ----------

# Helpers to identify current-month rows by store region / product category
store_region = dim_store.set_index("store_key")["region"].to_dict()
prod_cat = dim_product.set_index("product_key")["category"].to_dict()
prod_id = dim_product.set_index("product_key")["product_id"].to_dict()
fact["_region"] = fact["store_key"].map(store_region)
fact["_cat"] = fact["product_key"].map(prod_cat)
fact["_pid"] = fact["product_key"].map(prod_id)
fact["_ym"] = (fact["date_key"] // 100).astype(str).str.replace(r"(\d{4})(\d{2})", r"\1-\2", regex=True)

# Capture each region's current-month revenue BEFORE the collapse - this "expected" level is what we
# set targets against, so healthy regions land on target and only the collapsed West misses.
R_PRE = fact[fact["_ym"] == CURRENT_YEAR_MONTH].groupby("_region")["net_sales"].sum().to_dict()

cur = fact["_ym"] == CURRENT_YEAR_MONTH

# 1) West x Outdoor & Camping collapses: drop ~78% of those lines in the current month.
mask_collapse = cur & (fact["_region"] == HERO_REGION) & (fact["_cat"] == HERO_DETRACTOR_CATEGORY)
drop_collapse = fact[mask_collapse].sample(frac=0.78, random_state=RANDOM_SEED).index
# 2) West soft elsewhere: drop ~22% of the rest of West's current-month lines.
mask_west_other = cur & (fact["_region"] == HERO_REGION) & (fact["_cat"] != HERO_DETRACTOR_CATEGORY)
drop_west = fact[mask_west_other].sample(frac=0.22, random_state=RANDOM_SEED + 1).index
fact = fact.drop(index=drop_collapse.union(drop_west))

# 3) Bright spot: the surge earbuds (online) roughly double their current-month lines.
mask_surge = (fact["_ym"] == CURRENT_YEAR_MONTH) & (fact["_pid"] == HERO_SURGE_PRODUCT_ID)
surge_dupes = fact[mask_surge].sample(frac=1.0, random_state=RANDOM_SEED + 2).copy()
if len(surge_dupes):
    surge_dupes["sale_id"] = [f"SL-9{i:07d}" for i in range(len(surge_dupes))]
    fact = pd.concat([fact, surge_dupes], ignore_index=True)

fact = fact.drop(columns=["_region", "_cat", "_pid", "_ym"]).reset_index(drop=True)
print(f"fact_sales (post-storyline): {len(fact):,} lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sales_targets - per-region target = the current month's pre-collapse revenue × 0.98
# MAGIC
# MAGIC Setting the target just under what each region *would* have done this month makes the healthy
# MAGIC regions land comfortably on target while the collapsed West misses clearly - and it works no
# MAGIC matter which calendar month "today" is. The same flat target is written for every month so the
# MAGIC dashboard trend has a clean target line.

# COMMAND ----------

# region revenue per month from the finished fact (for verification + the trend line)
fr = fact.merge(dim_store[["store_key", "region"]], on="store_key")
fr["_ym"] = (fr["date_key"] // 100).astype(str).str.replace(r"(\d{4})(\d{2})", r"\1-\2", regex=True)
region_month_rev = fr.groupby(["region", "_ym"])["net_sales"].sum().reset_index()

target_rows = []
for region in REGIONS:
    tgt = round(R_PRE[region] * 0.98 / 1000.0) * 1000.0   # just under the expected level, nearest $1k
    for ym in ALL_MONTHS:
        target_rows.append({"region": region, "year_month": ym, "revenue_target": tgt})
sales_targets = pd.DataFrame(target_rows)
print("sales_targets: per-region target = this month's pre-collapse revenue x 0.98 (flat across months).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storyline check - confirm the demo lands before we write

# COMMAND ----------

print(f"== Actual vs target for {CURRENT_YEAR_MONTH} ==")
for region in REGIONS:
    act = region_month_rev[(region_month_rev.region == region) & (region_month_rev._ym == CURRENT_YEAR_MONTH)]["net_sales"].sum()
    tgt = sales_targets[(sales_targets.region == region) & (sales_targets.year_month == CURRENT_YEAR_MONTH)]["revenue_target"].iloc[0]
    pct = 100 * act / tgt if tgt else 0
    flag = "OFF TRACK" if act < tgt else "on track"
    print(f"  {region:10s} actual ${act:>12,.0f}  target ${tgt:>12,.0f}  ({pct:5.1f}%)  {flag}")
tot_act = region_month_rev[region_month_rev._ym == CURRENT_YEAR_MONTH]["net_sales"].sum()
tot_tgt = sales_targets[sales_targets.year_month == CURRENT_YEAR_MONTH]["revenue_target"].sum()
print(f"  {'COMPANY':10s} actual ${tot_act:>12,.0f}  target ${tot_tgt:>12,.0f}  ({100*tot_act/tot_tgt:5.1f}%)")
print("Expected: West ~76% OFF TRACK (the only region off track); Midwest/Northeast/South on track "
      "(~101-106%); COMPANY ~96% OFF TRACK because of the West.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write all tables (INSERT OVERWRITE preserves the DDL comments from notebook 01)

# COMMAND ----------

def overwrite(pdf, table, select_sql):
    """Register a temp view and INSERT OVERWRITE the target table with explicit casts."""
    view = "v_" + table.split(".")[-1]
    spark.createDataFrame(pdf).createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE TABLE {table} SELECT {select_sql} FROM {view}")
    cnt = spark.table(table).count()
    print(f"  {table}: {cnt:,} rows")

overwrite(dim_date, DIM_DATE,
          "CAST(date_key AS INT), CAST(date AS DATE), day_name, CAST(week_of_year AS INT), "
          "CAST(month AS INT), month_name, year_month, CAST(quarter AS INT), CAST(year AS INT), CAST(is_weekend AS BOOLEAN)")

overwrite(dim_product.drop(columns=["_pop"]), DIM_PRODUCT,
          "CAST(product_key AS INT), product_id, product_name, category, subcategory, brand, "
          "CAST(unit_cost AS DOUBLE), CAST(list_price AS DOUBLE)")

overwrite(dim_customer, DIM_CUSTOMER,
          "CAST(customer_key AS INT), customer_id, customer_name, loyalty_tier, region, country, CAST(signup_date AS DATE)")

overwrite(dim_store, DIM_STORE,
          "CAST(store_key AS INT), store_id, store_name, channel, city, region, country")

overwrite(fact, FACT_SALES,
          "sale_id, order_id, CAST(date_key AS INT), CAST(product_key AS INT), CAST(customer_key AS INT), "
          "CAST(store_key AS INT), CAST(quantity AS INT), CAST(unit_price AS DOUBLE), CAST(discount_amount AS DOUBLE), "
          "CAST(net_sales AS DOUBLE), CAST(cost_amount AS DOUBLE), CAST(gross_margin AS DOUBLE)")

overwrite(sales_targets, SALES_TARGETS,
          "region, year_month, CAST(revenue_target AS DOUBLE)")

print("All tables written. performance_insights stays empty until notebook 04.")
