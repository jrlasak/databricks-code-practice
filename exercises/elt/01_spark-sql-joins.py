# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Spark SQL Joins & Aggregations
# MAGIC **Topic**: ELT | **Exercises**: 9 | **Total Time**: ~95 min
# MAGIC
# MAGIC Practice core SQL join patterns and aggregation techniques on Delta tables:
# MAGIC inner joins, left joins with null handling, anti-joins, semi-joins, self-joins,
# MAGIC multi-table join chains, GROUP BY with HAVING, ROLLUP, and CUBE. Every exercise
# MAGIC reads the base tables in `db_code.elt` and writes its result as a Delta table to the **Target** it names.
# MAGIC
# MAGIC **Solutions**: If stuck, see `solutions/spark-sql-joins-solutions.py` for hints and answers.
# MAGIC
# MAGIC **Tables used** (from `00_Setup.py`):
# MAGIC - `db_code.elt.orders` - order records (106 rows)
# MAGIC - `db_code.elt.customers` - customer dimension (50 rows)
# MAGIC - `db_code.elt.products` - product catalog (30 rows)
# MAGIC - `db_code.elt.order_items` - line items per order (205 rows)
# MAGIC
# MAGIC **Schema** (`orders`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | Order key (ORD-042 and ORD-067 appear twice, with different `updated_at`) |
# MAGIC | customer_id | STRING | FK to customers (some nulls) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | amount | DOUBLE | Order total in USD (includes $0) |
# MAGIC | status | STRING | completed, pending, shipped, cancelled |
# MAGIC | order_date | DATE | Order placement date |
# MAGIC | updated_at | TIMESTAMP | Last modification time |
# MAGIC
# MAGIC **Schema** (`customers`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | customer_id | STRING | Primary key |
# MAGIC | name | STRING | Customer name (some duplicates) |
# MAGIC | email | STRING | Email (some nulls) |
# MAGIC | region | STRING | US-East, US-West, EU-West, EU-East, APAC |
# MAGIC | tier | STRING | bronze, silver, gold, platinum, inactive |
# MAGIC | signup_date | DATE | Registration date |
# MAGIC
# MAGIC **Schema** (`products`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | product_id | STRING | Primary key |
# MAGIC | name | STRING | Product name |
# MAGIC | category | STRING | Category (some nulls) |
# MAGIC | price | DOUBLE | List price (includes $0) |
# MAGIC | is_active | BOOLEAN | Active flag |
# MAGIC
# MAGIC **Schema** (`order_items`):
# MAGIC | Column | Type | Notes |
# MAGIC |--------|------|-------|
# MAGIC | order_id | STRING | FK to orders (has orphans) |
# MAGIC | product_id | STRING | FK to products |
# MAGIC | quantity | INT | Quantity (includes 0) |
# MAGIC | unit_price | DOUBLE | Price at time of order |

# COMMAND ----------

# MAGIC %run ./00_Setup

# COMMAND ----------

# MAGIC %run ./setup/spark-sql-joins-setup

# COMMAND ----------
# MAGIC %md
# MAGIC Your output tables go in `db_code.spark_sql_joins`; the base tables are in `db_code.elt`.
# MAGIC
# MAGIC The setup notebook sets the Python variables `CATALOG` = `db_code`, `SCHEMA` = `spark_sql_joins` and `BASE_SCHEMA` = `elt`. The TODO cells are Python: run SQL with `spark.sql(f"...")`.
# MAGIC
# MAGIC - Ex 1 (easy): inner join orders + customers
# MAGIC - Ex 2 (easy): left join with COALESCE null handling
# MAGIC - Ex 3 (medium): anti-join to find orphan order_items
# MAGIC - Ex 4 (medium): semi-join to find customers with a cancelled order
# MAGIC - Ex 5 (medium): self-join for same-day orders by the same customer
# MAGIC - Ex 6 (medium): join chain across 4 tables with QUALIFY
# MAGIC - Ex 7 (medium): GROUP BY with HAVING for high-value customers
# MAGIC - Ex 8 (hard): ROLLUP for hierarchical subtotals
# MAGIC - Ex 9 (hard): CUBE for all-combination subtotals

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 1: Inner Join Orders with Customers
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Join orders with customers to get enriched order records. Only include orders that
# MAGIC have a matching customer.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` (106 rows, some with NULL customer_id) and `db_code.elt.customers` (50 rows)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex1_output`. Expect 103 rows (the 3 orders with NULL `customer_id` drop out).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner join `orders` with `customers` on `customer_id`
# MAGIC 2. Select: `order_id`, `customer_id`, `amount`, `status`, `order_date`, customer `name`, customer `region`, customer `tier`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Orders with NULL `customer_id` should be excluded (inner join handles this)
# MAGIC - Use the Unity Catalog three-level namespace for all table references

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex1
# TODO: Inner join orders with customers and write to Delta table

# Your code here


# COMMAND ----------

# Validate Exercise 1
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex1_output")

# Row count: inner join drops orders with NULL customer_id
assert result.count() == JOINS_EX1_EXPECTED_COUNT, \
    f"Expected {JOINS_EX1_EXPECTED_COUNT} rows, got {result.count()}"
# Must have customer columns
assert "name" in result.columns, "Missing 'name' column from customers table"
assert "region" in result.columns, "Missing 'region' column from customers table"
assert "tier" in result.columns, "Missing 'tier' column from customers table"
# No NULL customer_ids (inner join excludes them)
assert result.filter("customer_id IS NULL").count() == 0, \
    "Inner join should exclude orders with NULL customer_id"

print("Exercise 1 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 2: Left Join with COALESCE for Null Handling
# MAGIC **Difficulty**: Easy | **Time**: ~5 min
# MAGIC
# MAGIC Left join orders with customers so ALL orders are preserved, even those without a
# MAGIC matching customer. Replace NULL customer names with 'Unknown'.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` (106 rows) and `db_code.elt.customers` (50 rows)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex2_output`. Should have the same row count as orders.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Left join `orders` with `customers` on `customer_id`
# MAGIC 2. Select: `order_id`, `customer_id`, `amount`, `status`, `order_date`, customer `name` (COALESCE to 'Unknown' if NULL), customer `region`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - ALL orders must appear in the output (including those with NULL customer_id)
# MAGIC - No NULL values in the `name` column of the output

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex2
# TODO: Left join orders with customers, COALESCE nulls, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 2
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex2_output")

# Row count: left join preserves all orders
total_orders = spark.table(f"{CATALOG}.{BASE_SCHEMA}.orders").count()
assert result.count() == total_orders, \
    f"Expected {total_orders} rows (all orders), got {result.count()}"
# No NULL names (COALESCE should handle them)
null_names = result.filter("name IS NULL").count()
assert null_names == 0, f"Found {null_names} NULL names - use COALESCE to replace with 'Unknown'"
# Verify 'Unknown' values exist (orders with NULL customer_id)
unknown_count = result.filter("name = 'Unknown'").count()
assert unknown_count > 0, "Expected some 'Unknown' names for orders with NULL customer_id"

print("Exercise 2 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 3: Anti-Join to Find Orphan Order Items
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Find line items in `order_items` that reference order_ids NOT present in the
# MAGIC `orders` table (orphan records). This is a common data quality check in production
# MAGIC pipelines. Write the orphans as a Delta table for downstream investigation.
# MAGIC
# MAGIC **Source**: `db_code.elt.order_items` (205 rows, has orphan order_ids) and `db_code.elt.orders` (106 rows)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex3_output`. Contains only orphan line items. Expect 10 rows: two line items each for `ORD-201` through `ORD-205`.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Find all rows in `order_items` where `order_id` does NOT exist in `orders`
# MAGIC 2. Select all columns from `order_items` for orphan records
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use LEFT ANTI JOIN (Databricks/Spark SQL extension, not available in standard SQL)
# MAGIC - Every row in the output must have an order_id that does NOT exist in orders

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex3
# TODO: Anti-join to find orphan line items, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 3
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex3_output")

# Must have orphan rows
assert result.count() == JOINS_EX3_EXPECTED_COUNT, \
    f"Expected {JOINS_EX3_EXPECTED_COUNT} orphan rows, got {result.count()}"
# Verify every row is truly an orphan (not in orders)
orders_ids = set(
    row.order_id for row in spark.table(f"{CATALOG}.{BASE_SCHEMA}.orders")
    .select("order_id").distinct().collect()
)
orphan_ids = set(row.order_id for row in result.select("order_id").distinct().collect())
overlap = orphan_ids & orders_ids
assert len(overlap) == 0, f"Found non-orphan order_ids in output: {overlap}"
# Must have order_items columns
assert "quantity" in result.columns, "Missing 'quantity' column from order_items"
assert "unit_price" in result.columns, "Missing 'unit_price' column from order_items"

print("Exercise 3 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 4: Semi-Join to Find Customers with a Cancelled Order
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Find all customers who have at least one cancelled order. Unlike INNER JOIN,
# MAGIC a semi-join returns only columns from the left table and never duplicates rows,
# MAGIC even when a customer has several matching orders (CUST-004 has two cancelled orders).
# MAGIC
# MAGIC **Source**: `db_code.elt.customers` (50 rows) and `db_code.elt.orders` (106 rows)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex4_output`. Expect 9 rows, one per customer with a cancelled order (CUST-004, CUST-008, CUST-013, CUST-017, CUST-021, CUST-023, CUST-031, CUST-036, CUST-046).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Find customers who have at least one order with `status = 'cancelled'` in `orders` (match on `customer_id`)
# MAGIC 2. Select all columns from `customers` only
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use LEFT SEMI JOIN (Databricks/Spark SQL extension)
# MAGIC - Output must contain ONLY customer columns (no order columns)
# MAGIC - No duplicate customer rows (semi-join guarantees this)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex4
# TODO: Semi-join customers against their cancelled orders, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 4
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex4_output")

# Row count: customers who have at least one cancelled order
assert result.count() == JOINS_EX4_EXPECTED_COUNT, \
    f"Expected {JOINS_EX4_EXPECTED_COUNT} rows, got {result.count()}"
# Must have customer columns only
assert "customer_id" in result.columns, "Missing 'customer_id' column"
assert "name" in result.columns, "Missing 'name' column"
assert "region" in result.columns, "Missing 'region' column"
assert "tier" in result.columns, "Missing 'tier' column"
# Must NOT have order columns (semi-join returns left table only)
assert "order_id" not in result.columns, "Semi-join should not include order columns"
assert "amount" not in result.columns, "Semi-join should not include order columns"
# No duplicate customers
assert result.count() == result.select("customer_id").distinct().count(), \
    "Semi-join should produce exactly one row per customer"

print("Exercise 4 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 5: Self-Join to Find Same-Day Orders by Same Customer
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Self-join the orders table to find pairs of orders where the same customer
# MAGIC placed multiple orders on the same day. Each pair should appear once (not twice).
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` (106 rows). Same-day repeat orders belong to
# MAGIC CUST-003 (2025-07-04), CUST-047 (2025-09-08) and CUST-050 (2025-09-12, three orders).
# MAGIC Note that the duplicate rows for ORD-042 and ORD-067 do NOT form pairs - both rows
# MAGIC share a single `order_id`, so `o1.order_id < o2.order_id` is never true for them.
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex5_output`. One row per order pair. Expect 5 pairs: one each for CUST-003 and CUST-047, three for CUST-050.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Self-join `orders` to itself on `customer_id` AND `order_date`
# MAGIC 2. Avoid pairing an order with itself and avoid duplicate pairs by requiring `o1.order_id < o2.order_id`
# MAGIC 3. Exclude rows where `customer_id` IS NULL
# MAGIC 4. Select: `o1.customer_id`, `o1.order_date`, `o1.order_id` AS `order_id_1`, `o2.order_id` AS `order_id_2`, `o1.amount` AS `amount_1`, `o2.amount` AS `amount_2`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Each pair appears exactly once (order_id_1 < order_id_2)
# MAGIC - No rows with NULL customer_id

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex5
# TODO: Self-join orders to find same-day pairs, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 5
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex5_output")

# Non-empty: same-day pairs exist in the base data (CUST-003, CUST-047, CUST-050)
assert result.count() > 0, \
    "Output is empty - the base data has same-day repeat orders, so a correct self-join returns rows"
# Row count
assert result.count() == JOINS_EX5_EXPECTED_COUNT, \
    f"Expected {JOINS_EX5_EXPECTED_COUNT} pairs, got {result.count()}"
# Must have the correct columns
assert "customer_id" in result.columns, "Missing 'customer_id' column"
assert "order_id_1" in result.columns, "Missing 'order_id_1' column"
assert "order_id_2" in result.columns, "Missing 'order_id_2' column"
assert "order_date" in result.columns, "Missing 'order_date' column"
# No NULL customer_ids
assert result.filter("customer_id IS NULL").count() == 0, \
    "Self-join should exclude rows with NULL customer_id"
# order_id_1 < order_id_2 for all rows (no duplicate pairs)
bad_pairs = result.filter("order_id_1 >= order_id_2").count()
assert bad_pairs == 0, f"Found {bad_pairs} rows where order_id_1 >= order_id_2 - pairs should be deduplicated"

print("Exercise 5 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 6: Multi-Table Join Chain (Orders + Customers + Products + Order Items)
# MAGIC **Difficulty**: Medium | **Time**: ~15 min
# MAGIC
# MAGIC Build a denormalized order detail table by joining 4 tables: orders, customers,
# MAGIC order_items, and products. Use QUALIFY (Databricks SQL extension) to keep only the
# MAGIC highest-quantity line item per order.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders`, `db_code.elt.customers`, `db_code.elt.order_items`, `db_code.elt.products`
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex6_output`. One row per order (top line item only). Expect 92 rows; `ORD-001` keeps its quantity-4 line item (`Delta Lake Essentials Guide`).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Join `orders` with `customers` on `customer_id` (inner join)
# MAGIC 2. Join with `order_items` on `order_id` (inner join)
# MAGIC 3. Join with `products` on `order_items.product_id` (inner join)
# MAGIC 4. Select: `order_id`, customer `name` AS `customer_name`, customer `region`,
# MAGIC    `order_items.quantity`, `order_items.unit_price`, product `name` AS `product_name`,
# MAGIC    product `category`, orders `status`
# MAGIC 5. Use QUALIFY with ROW_NUMBER() to keep only the line item with the highest quantity per order_id (partition by order_id, order by quantity DESC, then updated_at DESC so the latest version of a duplicated order wins)
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use QUALIFY (Databricks SQL window filter, not standard SQL)
# MAGIC - One row per order_id in the output
# MAGIC - Only orders that have matching customers, line items, AND products appear

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex6
# TODO: Multi-table join with QUALIFY, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 6
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex6_output")

# One row per order_id
assert result.count() == result.select("order_id").distinct().count(), \
    "Output should have exactly one row per order_id (QUALIFY should deduplicate)"
assert result.count() == JOINS_EX6_EXPECTED_COUNT, \
    f"Expected {JOINS_EX6_EXPECTED_COUNT} rows, got {result.count()}"
# Must have columns from all 4 tables
assert "customer_name" in result.columns, "Missing 'customer_name' (aliased from customers.name)"
assert "product_name" in result.columns, "Missing 'product_name' (aliased from products.name)"
assert "region" in result.columns, "Missing 'region' from customers"
assert "category" in result.columns, "Missing 'category' from products"
assert "quantity" in result.columns, "Missing 'quantity' from order_items"
# QUALIFY must keep the highest-quantity line item per order
row = result.filter("order_id = 'ORD-001'").first()
assert row is not None, "ORD-001 is missing from the output"
assert row.quantity == 4, f"ORD-001 should keep its highest-quantity line item, got quantity {row.quantity}"

print("Exercise 6 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 7: GROUP BY with HAVING to Find High-Value Customers
# MAGIC **Difficulty**: Medium | **Time**: ~10 min
# MAGIC
# MAGIC Aggregate orders per customer and filter to only high-value customers using HAVING.
# MAGIC This is a core reporting pattern: aggregate first, filter the groups second.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` (106 rows) and `db_code.elt.customers` (50 rows)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex7_output`. One row per qualifying customer. Expect 14 customers; `CUST-044` has `order_count` 2 and `total_spent` 1050.0. Count every order row as it is (do not deduplicate ORD-042 or ORD-067), so `CUST-034` has `order_count` 3.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Exclude rows where `customer_id` IS NULL
# MAGIC 2. Group `orders` by `customer_id`
# MAGIC 3. Calculate: COUNT(*) AS `order_count`, SUM(amount) AS `total_spent`, AVG(amount) AS `avg_order_value`
# MAGIC 4. Use HAVING to keep only customers with `total_spent > 500`
# MAGIC 5. Inner join the result with `customers` on `customer_id` and select `customer_id`, `name`, `region`, `order_count`, `total_spent`, `avg_order_value`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - Use HAVING (not WHERE) to filter on the aggregated total
# MAGIC - One row per customer_id in the output
# MAGIC - All customers in the output must have total_spent > 500

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex7
# TODO: GROUP BY with HAVING for high-value customers, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 7
result = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex7_output")

# Row count
assert result.count() == JOINS_EX7_EXPECTED_COUNT, \
    f"Expected {JOINS_EX7_EXPECTED_COUNT} high-value customers, got {result.count()}"
# Must have required columns
assert "customer_id" in result.columns, "Missing 'customer_id' column"
assert "order_count" in result.columns, "Missing 'order_count' column"
assert "total_spent" in result.columns, "Missing 'total_spent' column"
assert "avg_order_value" in result.columns, "Missing 'avg_order_value' column"
assert "name" in result.columns, "Missing 'name' column from customers"
assert "region" in result.columns, "Missing 'region' column from customers"
# All customers must have total_spent > 500
below_threshold = result.filter("total_spent <= 500").count()
assert below_threshold == 0, f"Found {below_threshold} rows with total_spent <= 500 - HAVING should filter these"
# No NULL customer_ids
assert result.filter("customer_id IS NULL").count() == 0, \
    "NULL customer_ids should be excluded"
# One row per customer
assert result.count() == result.select("customer_id").distinct().count(), \
    "Output should have exactly one row per customer_id"

print("Exercise 7 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 8: ROLLUP for Hierarchical Subtotals by Region and Tier
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Generate hierarchical revenue summaries using GROUP BY ROLLUP. ROLLUP produces
# MAGIC subtotals for each level in the grouping hierarchy (left-to-right), plus a grand
# MAGIC total. This is essential for building gold-layer aggregation tables in a medallion
# MAGIC architecture.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` and `db_code.elt.customers` (joined on customer_id)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex8_rollup`. The grand total row has `order_count` 42.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner join `orders` with `customers` on `customer_id`
# MAGIC 2. Filter to only `status = 'completed'` orders
# MAGIC 3. GROUP BY ROLLUP(`region`, `tier`) to get:
# MAGIC    - Per region+tier totals
# MAGIC    - Per region subtotals (tier = NULL)
# MAGIC    - Grand total (region = NULL, tier = NULL)
# MAGIC 4. Select: `region`, `tier`, COUNT(*) AS `order_count`, SUM(amount) AS `total_revenue`,
# MAGIC    AVG(amount) AS `avg_order_value`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - ROLLUP produces hierarchical subtotals (left-to-right only)
# MAGIC - Must have exactly 1 grand total row where both region and tier are NULL
# MAGIC - Must have per-region subtotals where tier is NULL but region is NOT NULL

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex8
# TODO: Build ROLLUP aggregation table, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 8
rollup = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex8_rollup")

# Must have required columns
assert "region" in rollup.columns, "Missing 'region' column"
assert "tier" in rollup.columns, "Missing 'tier' column"
assert "order_count" in rollup.columns, "Missing 'order_count' column"
assert "total_revenue" in rollup.columns, "Missing 'total_revenue' column"
assert "avg_order_value" in rollup.columns, "Missing 'avg_order_value' column"
# ROLLUP: has exactly 1 grand total row (both region and tier are NULL)
grand_total = rollup.filter("region IS NULL AND tier IS NULL")
assert grand_total.count() == 1, \
    "ROLLUP should have exactly 1 grand total row (region=NULL, tier=NULL)"
# ROLLUP: has per-region subtotals (region NOT NULL, tier NULL)
region_subtotals = rollup.filter("region IS NOT NULL AND tier IS NULL")
assert region_subtotals.count() > 0, \
    "ROLLUP should have per-region subtotals (region=value, tier=NULL)"
# ROLLUP should NOT have per-tier-only subtotals (region=NULL, tier=value)
tier_only = rollup.filter("region IS NULL AND tier IS NOT NULL")
assert tier_only.count() == 0, \
    "ROLLUP should NOT have per-tier subtotals (that is what CUBE adds)"
# Grand total counts completed orders only
grand_count = grand_total.select("order_count").collect()[0][0]
assert grand_count == JOINS_EX8_EXPECTED_GRAND_COUNT, \
    f"Grand total order_count should be {JOINS_EX8_EXPECTED_GRAND_COUNT} (completed orders only), got {grand_count}"

print("Exercise 8 passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exercise 9: CUBE for All-Combination Subtotals Across Region and Status
# MAGIC **Difficulty**: Hard | **Time**: ~15 min
# MAGIC
# MAGIC Generate all-combination revenue summaries using GROUP BY CUBE. Unlike ROLLUP,
# MAGIC CUBE produces subtotals for EVERY possible combination of the grouping columns.
# MAGIC This gives you per-region, per-status, per-region+status, and grand total rows
# MAGIC in a single query.
# MAGIC
# MAGIC **Source**: `db_code.elt.orders` and `db_code.elt.customers` (joined on customer_id)
# MAGIC
# MAGIC **Target**: Write to `db_code.spark_sql_joins.joins_ex9_cube`. The grand total row has `order_count` 103: every order row with a matching customer, both versions of ORD-042 and ORD-067 included (do not deduplicate).
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Inner join `orders` with `customers` on `customer_id`
# MAGIC 2. GROUP BY CUBE(`region`, `status`) to get all combinations:
# MAGIC    - Per region+status, per region only, per status only, and grand total
# MAGIC 3. Select: `region`, `status`, COUNT(*) AS `order_count`, SUM(amount) AS `total_revenue`,
# MAGIC    AVG(amount) AS `avg_order_value`
# MAGIC
# MAGIC **Constraints**:
# MAGIC - CUBE produces ALL dimension combinations (more rows than ROLLUP would)
# MAGIC - Must have per-status subtotals (region=NULL, status=value) - this is what CUBE adds over ROLLUP
# MAGIC - Must have per-region subtotals (region=value, status=NULL)
# MAGIC - Must have exactly 1 grand total row (both NULL)

# COMMAND ----------

# EXERCISE_KEY: spark_sql_joins_ex9
# TODO: Build CUBE aggregation table, write to Delta

# Your code here


# COMMAND ----------

# Validate Exercise 9
cube = spark.table(f"{CATALOG}.{SCHEMA}.joins_ex9_cube")

# Must have required columns
assert "region" in cube.columns, "Missing 'region' column"
assert "status" in cube.columns, "Missing 'status' column"
assert "order_count" in cube.columns, "Missing 'order_count' column"
assert "total_revenue" in cube.columns, "Missing 'total_revenue' column"
assert "avg_order_value" in cube.columns, "Missing 'avg_order_value' column"
# CUBE: has grand total row
grand_total = cube.filter("region IS NULL AND status IS NULL")
assert grand_total.count() == 1, \
    "CUBE should have exactly 1 grand total row (region=NULL, status=NULL)"
# CUBE: has per-status subtotals (region=NULL, status NOT NULL)
status_only = cube.filter("region IS NULL AND status IS NOT NULL")
assert status_only.count() > 0, \
    "CUBE should have per-status subtotals (region=NULL, status=value)"
# CUBE: has per-region subtotals (region NOT NULL, status=NULL)
region_only = cube.filter("region IS NOT NULL AND status IS NULL")
assert region_only.count() > 0, \
    "CUBE should have per-region subtotals (region=value, status=NULL)"
# CUBE: has detail rows (both NOT NULL)
detail_rows = cube.filter("region IS NOT NULL AND status IS NOT NULL")
assert detail_rows.count() > 0, "CUBE should have detail rows (both dimensions specified)"
# Grand total covers every order with a matching customer (no status filter)
grand_count = grand_total.select("order_count").collect()[0][0]
assert grand_count == JOINS_EX1_EXPECTED_COUNT, \
    f"Grand total order_count should be {JOINS_EX1_EXPECTED_COUNT} (all statuses), got {grand_count}"

print("Exercise 9 passed!")
