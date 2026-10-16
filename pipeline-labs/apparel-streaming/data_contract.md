# Data Contract: Apparel Retail 360 Pipeline

This is what your finished Lakeflow pipeline must produce: every table and view, the schema it lands in, its type, its key and the columns it outputs. Names and paths come from `dlt/variables.py`; everything lands in the `apparel_store` catalog. The four source streams (sales, customers, products, stores) and their raw schemas are documented in [SyntheticDataGenerator.md](SyntheticDataGenerator.md#synthetic-data-generator-table-schemas), so they are not repeated here.

Expectation actions used below: **fail** stops the update (`expect_or_fail`), **drop** removes the row (`expect_or_drop`), **warn** keeps the row and records the violation in the pipeline metrics (`expect`).

## Overview

| Name | Schema | Type | Reads from | Key |
|---|---|---|---|---|
| `bronze_sales` | `01_bronze` | Streaming table | `/Volumes/apparel_store/00_landing/streaming/sales` | `transaction_id` |
| `bronze_customers` | `01_bronze` | Streaming table | `/Volumes/apparel_store/00_landing/streaming/customers` | `customer_id` |
| `bronze_products` | `01_bronze` | Streaming table | `/Volumes/apparel_store/00_landing/streaming/items` | `product_id` |
| `bronze_stores` | `01_bronze` | Streaming table | `/Volumes/apparel_store/00_landing/streaming/stores` | `store_id` |
| `customers_cleaned_stream` | pipeline only | View | `bronze_customers` | `customer_id` |
| `products_cleaned_stream` | pipeline only | View | `bronze_products` | `product_id` |
| `stores_cleaned_stream` | pipeline only | View | `bronze_stores` | `store_id` |
| `sales_cleaned_stream` | pipeline only | View | `bronze_sales` | `transaction_id` |
| `silver_customers` | `02_silver` | Streaming table (SCD Type 2) | `customers_cleaned_stream` | `customer_id` + `__START_AT` |
| `silver_products` | `02_silver` | Streaming table (SCD Type 2) | `products_cleaned_stream` | `product_id` + `__START_AT` |
| `silver_stores` | `02_silver` | Streaming table (SCD Type 2) | `stores_cleaned_stream` | `store_id` + `__START_AT` |
| `silver_sales_transactions` | `02_silver` | Streaming table | `sales_cleaned_stream` | `transaction_id` |
| `silver_returns_transactions` | `02_silver` | Streaming table | `sales_cleaned_stream` | `transaction_id` |
| `silver_customers_current` | pipeline only | View | `silver_customers` | `customer_id` |
| `silver_products_current` | pipeline only | View | `silver_products` | `product_id` |
| `silver_stores_current` | pipeline only | View | `silver_stores` | `store_id` |
| `denormalized_sales_facts` | `03_gold` | Streaming table | `silver_sales_transactions` + the three current views | `transaction_id` |
| `gold_daily_sales_by_store` | `03_gold` | Materialized view | `denormalized_sales_facts` + `silver_stores_current` | `sale_date` + `store_id` |
| `gold_product_performance` | `03_gold` | Materialized view | `denormalized_sales_facts` + `silver_products_current` | `product_id` |
| `gold_customer_lifetime_value` | `03_gold` | Materialized view | `denormalized_sales_facts` + `silver_customers_current` | `customer_id` |

"Pipeline only" views exist inside the pipeline run and are not published to Unity Catalog, so you will not find them in Catalog Explorer. The streaming tables and materialized views are published: query them as, for example, `` apparel_store.`01_bronze`.bronze_sales ``.

## Bronze (`01_bronze`)

Each bronze table reads its landing folder as a Delta stream, casts every source column to the type below and adds two lineage columns:

| Column | Type | Meaning |
|---|---|---|
| `ingest_timestamp` | timestamp | When the micro-batch that loaded the row was processed (`current_timestamp()`) |
| `source_file_path` | string | The landing file the row came from (`_metadata.file_path`) |

| Table | Column types after the cast | Expectation |
|---|---|---|
| `bronze_sales` | `transaction_id`, `store_id`, `customer_id`, `product_id`, `quantity`: int; `event_time`: timestamp; `unit_price`, `total_amount`, `discount_applied`, `tax_amount`: double; `payment_method`: string | fail: `transaction_id is not null` |
| `bronze_customers` | `customer_id`, `loyalty_points`, `age`: int; `join_date`, `last_update_time`: timestamp; `name`, `email`, `address`, `phone_number`, `gender`: string | fail: `customer_id is not null` |
| `bronze_products` | `product_id`, `stock_quantity`: int; `price`: double; `last_update_time`: timestamp; `name`, `category`, `brand`, `size`, `color`, `description`: string | fail: `product_id is not null` |
| `bronze_stores` | `store_id`: int; `open_date`, `last_update_time`: timestamp; `name`, `address`, `manager`, `status`, `phone_number`: string | fail: `store_id is not null` |

## Silver cleaned streams (pipeline-only views)

These views pass the bronze columns through unchanged except where noted, and carry the quality checks for the silver tables built on them.

| View | Changes | Expectations |
|---|---|---|
| `customers_cleaned_stream` | none | drop: `customer_id IS NOT NULL`; warn: `email IS NULL OR email LIKE '%@%.%'`, `age >= 18 AND age <= 100`, `loyalty_points >= 0`, `gender IN ('Male', 'Female', 'Other')` |
| `products_cleaned_stream` | none | drop: `product_id IS NOT NULL`, `price > 0 AND price < 1000`; warn: `stock_quantity >= 0`, `category IN ('Casual Wear', 'Formal Wear', 'Sportswear', 'Accessories', 'Footwear')`, `brand IS NOT NULL` |
| `stores_cleaned_stream` | a NULL `manager` becomes `'Unknown'` | drop: `store_id IS NOT NULL`; warn: `manager <> 'Unknown'` (counts vacancies after the fill), `status IN ('Open', 'Under Renovation')` |
| `sales_cleaned_stream` | adds `date` (date, from `event_time`); 10-minute watermark on `event_time` | warn: `payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card')`, `discount_applied >= 0` |

## Silver dimensions (`02_silver`, SCD Type 2)

Built with `create_auto_cdc_flow`, keyed on the business key, sequenced by `last_update_time`, with NULL updates ignored (a NULL in an incoming change keeps the stored value). Each table keeps every bronze column except `last_update_time`, `ingest_timestamp` and `source_file_path`, plus `__START_AT` and `__END_AT`. A change to a tracked column closes the current version (sets its `__END_AT`) and opens a new one; a change to any other column updates the current version in place. The current version of a key is the row where `__END_AT IS NULL`. A store's `manager` is filled with `'Unknown'` before the flow, so a vacancy is a tracked change that opens a new version, not a NULL the flow ignores.

| Table | Key | History tracked on | Other columns (updated in place) |
|---|---|---|---|
| `silver_customers` | `customer_id` | `name`, `email`, `address`, `phone_number`, `gender` | `join_date`, `loyalty_points`, `age` |
| `silver_products` | `product_id` | `name`, `category`, `brand`, `price`, `description`, `color`, `size` | `stock_quantity` |
| `silver_stores` | `store_id` | `name`, `address`, `manager`, `status` | `open_date`, `phone_number` |

## Silver facts (`02_silver`)

Both tables read `sales_cleaned_stream` and split it by the sign of `quantity`. A row with `quantity = 0` lands in neither.

| Table | Rows | Columns | Expectations |
|---|---|---|---|
| `silver_sales_transactions` | `quantity > 0` | every `sales_cleaned_stream` column (bronze sales columns, the two lineage columns and `date`) | drop: `discount_applied >= 0`, `store_id IS NOT NULL`, `customer_id IS NOT NULL`, `product_id IS NOT NULL` |
| `silver_returns_transactions` | `quantity < 0` | as above, but `quantity` and `total_amount` are replaced by `returned_quantity` (int) and `returned_amount` (double), both positive (absolute values); `tax_amount` keeps the source sign, negative on a return (refunded tax is `-tax_amount`) | same four drops as above |

## Silver current views (pipeline only)

`silver_customers_current`, `silver_products_current` and `silver_stores_current` return the rows of their SCD2 table where `__END_AT IS NULL`: one row per key, the latest version. Gold joins against these.

## Gold (`03_gold`)

### `denormalized_sales_facts` (streaming table)

One row per `silver_sales_transactions` row, left-joined to the current store, customer and product. A sale whose dimension has not arrived yet keeps its row with NULL names, and the names stay as they were at join time. Returns are not included.

| Column | Type | From |
|---|---|---|
| `transaction_id` | int | sales |
| `event_time` | timestamp | sales |
| `sale_date` | date | sales `date` (from Task 8) |
| `customer_id` | int | sales |
| `customer_name` | string | `silver_customers_current.name` |
| `product_id` | int | sales |
| `product_name` | string | `silver_products_current.name` |
| `product_category` | string | `silver_products_current.category` |
| `store_id` | int | sales |
| `store_name` | string | `silver_stores_current.name` |
| `quantity` | int | sales |
| `unit_price` | double | sales |
| `total_amount` | double | sales |
| `payment_method` | string | sales |
| `discount_applied` | double | sales |
| `tax_amount` | double | sales |

The three gold aggregates below group by the ID only. Name columns come from the current dimension view, so one ID is one row. Revenue and spend are `total_amount` sums: after discounts, before returns.

### `gold_daily_sales_by_store` (materialized view)

One row per `sale_date` and store.

| Column | Type | Meaning |
|---|---|---|
| `sale_date` | date | Sale date |
| `store_id` | int | Store |
| `store_name` | string | Current store name (`silver_stores_current.name`) |
| `total_revenue` | double | Sum of `total_amount` (after discounts, before returns), rounded to 2 decimals |
| `total_transactions` | bigint | Count of `transaction_id` |
| `total_items_sold` | bigint | Sum of `quantity` |
| `unique_customers` | bigint | Distinct `customer_id` |

### `gold_product_performance` (materialized view)

One row per product.

| Column | Type | Meaning |
|---|---|---|
| `product_id` | int | Product |
| `product_name` | string | Current product name (`silver_products_current.name`) |
| `product_category` | string | Current product category (`silver_products_current.category`) |
| `total_revenue` | double | Sum of `total_amount` (after discounts, before returns), rounded to 2 decimals |
| `total_quantity_sold` | bigint | Sum of `quantity` |
| `total_orders` | bigint | Count of `transaction_id` |

### `gold_customer_lifetime_value` (materialized view)

One row per customer.

| Column | Type | Meaning |
|---|---|---|
| `customer_id` | int | Customer |
| `customer_name` | string | Current customer name (`silver_customers_current.name`) |
| `total_spend` | double | Sum of `total_amount` (after discounts, before returns), rounded to 2 decimals |
| `total_orders` | bigint | Distinct `transaction_id` |
| `first_purchase_date` | date | Earliest `event_time` |
| `last_purchase_date` | date | Latest `event_time` |
| `avg_order_value` | double | Average `total_amount`, rounded to 2 decimals |
