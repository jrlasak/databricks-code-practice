# Genie Space Setup - Aurora Sales Assistant

Step-by-step build of a production-grade Genie space on top of the star schema you just created.
Pick up here once `00_Run_All` finished and all 7 tables are populated.

> This is a **manual, click-through** build inside the Databricks UI. Genie configuration lives in
> the workspace, not in code - so this guide is the artifact, the same way a runbook is. Work through
> it top to bottom; most of your iteration time is in **§ 4 (Instructions Text)**.

**What you'll configure:** About → Data → Instructions (Text, Joins, SQL Expressions, SQL Queries) →
register the 3 UC functions → Common Questions. Then benchmark it in `02_Benchmarks.md`, diagnose +
iterate using `03_Test_Workflows.md`, and surface it in a dashboard via `04_Dashboard_And_Embedded_Genie.md`.

Everything is compatible with **Databricks Free Edition** (Genie, UC functions, and AI/BI dashboards
all run on the Serverless Starter Warehouse).

---

## 0. The data you're pointing Genie at

| Table                  | Grain             | Key columns                                                                                     |
| ---------------------- | ----------------- | ----------------------------------------------------------------------------------------------- |
| `fact_sales`           | one order line    | `date_key`, `product_key`, `customer_key`, `store_key`, `net_sales`, `gross_margin`, `quantity` |
| `dim_date`             | one day           | `date_key`, `year_month`, `month_name`, `quarter`                                               |
| `dim_product`          | one SKU           | `product_key`, `product_id`, `product_name`, `category`, `unit_cost`, `list_price`              |
| `dim_customer`         | one customer      | `customer_key`, `loyalty_tier`, `region`                                                        |
| `dim_store`            | one store/channel | `store_key`, `channel`, `region`                                                                |
| `sales_targets`        | region × month    | `region`, `year_month`, `revenue_target`                                                        |
| `performance_insights` | ranked driver     | `region`, `year_month`, `rank`, `driver_type`, `insight`                                        |

Full namespace prefix: `aurora_retail.sales.` - column names are clean `snake_case`, so **no backticks anywhere**.

---

## 1. Create the Genie space

Free Edition leads with **Connect your data** before you name the space.

1. In the Databricks left sidebar, click **Genie Spaces**.
2. Click **New** in the top-right corner.
3. The **Connect your data** dialog opens. Navigate the breadcrumb: **All catalogs → `aurora_retail` → `sales`**.
   You should see all 7 tables:
   - `dim_customer`
   - `dim_date`
   - `dim_product`
   - `dim_store`
   - `fact_sales` ⬅ the fact
   - `performance_insights`
   - `sales_targets`

   Add all of them - the easiest way is to click the **+** icon on the right of the breadcrumb row to add the whole `sales` schema in one click. (You can also click each table individually if you'd rather pick them.)

4. Click **Create**.

---

## 2. Open the settings panel

After creating the space, the top toolbar shows **Chat / Monitor / Benchmark** on the left and action
icons on the right. Click **Configure** (gear icon) in the top-right to open the settings panel.

The panel exposes 3 tabs:

| Tab              | What lives here                                                       |
| ---------------- | --------------------------------------------------------------------- |
| **About**        | Name, tags, thumbnail, description, common questions                  |
| **Data**         | Connected tables (drill into each for column-level fine-tuning)       |
| **Instructions** | 4 sub-tabs: **Text**, **Joins**, **SQL Expressions**, **SQL Queries** |

> Free Edition uses the Serverless Starter Warehouse automatically - there's no warehouse selector to set.

---

## 3. About tab - name, description, common questions

The About tab has fields for **Name**, **Tags**, **Thumbnail**, **Description**, and **Common questions**.
Genie pre-fills a suggested description and several suggested common questions; you can **Accept** each
suggestion or click the **pencil** icon to edit. Tags and Thumbnail are optional - skip them.

- **Name**: `Aurora Sales Assistant`
- **Description**: Genie's auto-suggestion is usually close (something like "this space enables analysis
  of retail sales performance by region, product, category, and channel…"). If you'd rather pin it down,
  click the pencil and replace with:

  ```text
  AI assistant for Aurora Outfitters retail sales. Answers questions about revenue, margin, target attainment, regional and channel performance, top products, and pricing what-ifs.
  ```
- **Common questions**: come back to this at **§ 8** below - once SQL Queries and functions exist, the
  curated set there is what we want. For now you can Accept any of Genie's auto-suggestions that look
  useful, or skip them entirely; we'll add a curated set later.

---

## 4. Instructions → Text - the system prompt (HIGHEST-LEVERAGE STEP)

**Instructions** tab → **Text** sub-tab. You'll see a **General Instructions** textarea with placeholder
example text ("MCA stands for...", "Countries in the country_code column...", etc.) - that's just a
prompt template, clear it. Paste the block below in its place and click **Save** (bottom-right).

This is the single most important artifact in the whole setup. Iterate it whenever Genie misreads a question.

```
You are a sales analyst assistant for Aurora Outfitters, a US omnichannel outdoor and lifestyle
retailer. Answer questions about revenue, margin, target attainment, regional and channel performance,
top products, and pricing what-ifs.

CURRENT MONTH: "this month" = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date). Never
hardcode a year-month string. "Last month" = the second-most-recent month.

REGION: for sales / revenue / channel reporting, region means dim_store.region (the store's region).
Use dim_customer.region only when the question is explicitly about where customers live.

PRODUCT-NAME RESOLUTION: when a user names a product, resolve to dim_product.product_id via
dim_product.product_name before calling a function. Quick anchors:
- "Summit 600 Down Jacket" / "the down jacket" -> SKU-1042
- "Aera Wireless Earbuds Pro" / "the earbuds" -> SKU-1009
For any other product: SELECT product_id FROM dim_product WHERE LOWER(product_name) LIKE LOWER('%<fragment>%') LIMIT 1.

OUTPUT: round dollars to the nearest dollar; percentages to 1 decimal. Tables for multi-row answers.
Always show product_name with product_id and region/category labels next to numbers.
```

Short on purpose. Everything else (measures, joins, certified queries, function call syntax) is encoded
in its own surface and Genie reads it natively - duplicating it here would just bloat the prompt without
making the answers better. The system prompt holds only what those surfaces can't carry: business
context, the current-month resolver, the store-vs-customer-region pin, product-name anchors for function
calls, and output preferences.

After pasting, click **Save**.

---

## 5. Instructions → Joins - explicit join paths

**Instructions** tab → **Joins** sub-tab. The list view has three columns (Left table / Right table /
Condition) and starts at "No joins". Click **+ Add** (top-right) for each row below - the dialog asks
for Left Table / Right Table / Join condition / Relationship Type / Instructions.

| #   | Left         | Left col       | Right          | Right col      | Relationship | Instructions (paste)                                                                                                                                         |
| --- | ------------ | -------------- | -------------- | -------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `fact_sales` | `date_key`     | `dim_date`     | `date_key`     | Many to One  | `Join sales lines to the date dimension. Required to filter or group by month (dim_date.year_month), quarter, or year.`                                      |
| 2   | `fact_sales` | `product_key`  | `dim_product`  | `product_key`  | Many to One  | `Join sales lines to the product dimension for product_name, category, subcategory, brand, unit_cost, and list_price.`                                       |
| 3   | `fact_sales` | `store_key`    | `dim_store`    | `store_key`    | Many to One  | `Join sales lines to the store dimension. Use dim_store.region for ALL regional sales reporting and dim_store.channel for Retail vs Online.`                 |
| 4   | `fact_sales` | `customer_key` | `dim_customer` | `customer_key` | Many to One  | `Join sales lines to the customer dimension for loyalty_tier. Use dim_customer.region only when the user asks where customers live, not for regional sales.` |

`sales_targets` and `performance_insights` are deliberately **not** declared here. They don't have FK
relationships to the star (they share `region` and `year_month` as _attributes_, not keys, so any join
would be Many-to-Many on a non-key column with row-multiplication risk). The certified queries in §7
already show the canonical pattern: aggregate `fact_sales` to `(region, year_month)` first, then join
the aggregate to `sales_targets` on those two columns. The Instructions text in §4 tells Genie the same.
Declaring extra M:M attribute joins would clutter the Joins surface with patterns no real question needs,
and would push Genie toward joins that risk multiplying rows. `performance_insights` is read almost
exclusively via the `top_drivers` function, so an entry buys even less.

---

## 6. Instructions → SQL Expressions - reusable measures & filters

**Instructions** tab → **SQL Expressions** sub-tab. The section header reads **Common SQL Expressions**
("Define business concepts as SQL expressions") with columns Type / Name / Tables. Click **+ Add** for
each entry below - the dialog exposes 3 sub-types: **Measure**, **Filter**, **Field**. All of these are
anchored on `fact_sales`, so qualify every column with `fact_sales.` (the validator rejects bare names).
Fill **Code**, **Synonyms**, and **Instructions** for each.

### Measures (5)

**1. `total_revenue`**

- Code: `SUM(fact_sales.net_sales)`
- Synonyms: `revenue, sales, total sales, net sales, top line`
- Instructions: `The primary revenue measure. Use for any "how much did we sell / what's our revenue" question. Do NOT sum unit_price or list_price for revenue - only net_sales.`

**2. `total_margin`**

- Code: `SUM(fact_sales.gross_margin)`
- Synonyms: `margin, gross margin, profit, gross profit`
- Instructions: `Total gross profit in dollars. Use for margin/profit questions. Pair with total_revenue for margin percent.`

**3. `margin_pct`**

- Code: `SUM(fact_sales.gross_margin) / NULLIF(SUM(fact_sales.net_sales), 0) * 100`
- Synonyms: `margin percent, margin rate, profitability, gross margin percentage`
- Instructions: `Gross margin as a percent of revenue. Use when the user asks about margin rate or profitability rather than absolute dollars.`

**4. `units_sold`**

- Code: `SUM(fact_sales.quantity)`
- Synonyms: `units, quantity, units sold, volume`
- Instructions: `Total units sold. Use for volume questions, distinct from revenue.`

**5. `avg_order_value`**

- Code: `SUM(fact_sales.net_sales) / NULLIF(COUNT(DISTINCT fact_sales.order_id), 0)`
- Synonyms: `AOV, average order value, average basket, average ticket`
- Instructions: `Average revenue per order. Use for AOV / basket-size questions.`

### Filters (1)

**6. `is_discounted`**

- Code: `fact_sales.discount_amount > 0`
- Synonyms: `discounted, on promotion, marked down, promo lines`
- Instructions: `Filters to order lines that had a discount applied. Use for promo-analysis questions.`

> **No "current month" filter expression on purpose.** A SQL Expression is anchored to a single table
> (`fact_sales`), which has `date_key` but not `year_month` - so it can't self-anchor on the latest month
> without hardcoding a date range that would go stale. "This month" is handled instead by joining
> `dim_date` and filtering `year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)`,
> which every certified query below already does. Keeps the lab evergreen.

---

## 7. Instructions → SQL Queries - certified queries + register the functions

**Instructions** tab → **SQL Queries** sub-tab. The section header reads **SQL queries & functions**
("Add example queries that Genie can learn from"). Click the dropdown caret next to **+ Add**; it
opens with two options - **Example query** (certified queries) and **SQL function** (links to UC
functions). Do the example queries first, then register the functions.

### 7a. Example queries

Each has 4 fields: **Question** (NL label Genie matches intent against), **SQL**, **Parameters**
(`:name` slots), **Usage Guidance** (what/how/avoid).

#### Query 1 - On-track status this month

- **Question**: `How are sales pacing against our monthly target?`
- **Parameters**: _(none)_
- **SQL**:

```sql
WITH actual AS (
  SELECT s.region, SUM(f.net_sales) AS revenue
  FROM aurora_retail.sales.fact_sales f
  JOIN aurora_retail.sales.dim_store s ON f.store_key = s.store_key
  JOIN aurora_retail.sales.dim_date d  ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
  GROUP BY s.region
)
SELECT
  ROUND(SUM(a.revenue))                                    AS company_actual_revenue,
  ROUND(SUM(t.revenue_target))                             AS company_target,
  ROUND(SUM(a.revenue) - SUM(t.revenue_target))            AS gap_to_target,
  ROUND(100 * SUM(a.revenue) / SUM(t.revenue_target), 1)   AS pct_to_target,
  CASE WHEN SUM(a.revenue) >= SUM(t.revenue_target) THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM aurora_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date);
```

- **Usage Guidance**: `Use when the user asks "are we hitting target", "how are sales tracking this month", or any company-level status question. Returns one row: company actual revenue, target, dollar gap, percent-to-target, and ON/OFF TRACK. Do NOT list regions or products here - if the user wants the regional breakdown, the per-region certified query handles that. Anchored on the current month (the latest month in the data).`

#### Query 2 - Regional pacing vs target

- **Question**: `Which regions are missing target this month, worst first?`
- **Parameters**: _(none)_
- **SQL**:

```sql
WITH actual AS (
  SELECT s.region, SUM(f.net_sales) AS revenue
  FROM aurora_retail.sales.fact_sales f
  JOIN aurora_retail.sales.dim_store s ON f.store_key = s.store_key
  JOIN aurora_retail.sales.dim_date d  ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
  GROUP BY s.region
)
SELECT
  t.region,
  ROUND(a.revenue)                              AS actual_revenue,
  ROUND(t.revenue_target)                       AS revenue_target,
  ROUND(a.revenue - t.revenue_target)           AS gap_to_target,
  ROUND(100 * a.revenue / t.revenue_target, 1)  AS pct_to_target,
  CASE WHEN a.revenue >= t.revenue_target THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM aurora_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
ORDER BY pct_to_target ASC;
```

- **Usage Guidance**: `Use when the user asks "why are we behind", "which regions are missing target", "break it down by region", or any per-region pacing question. Returns each region's actual vs target sorted worst-first, so the worst-performing region lands at the top. To drill from a region into WHY it's down, call the top_drivers function.`

#### Query 3 - Top products by revenue this month

- **Question**: `What are our top products by revenue this month?`
- **Parameters**: `limit_n` (Integer, default `10`)
- **SQL**:

```sql
SELECT
  p.product_id, p.product_name, p.category,
  ROUND(SUM(f.net_sales))    AS revenue,
  SUM(f.quantity)            AS units,
  ROUND(SUM(f.gross_margin)) AS margin
FROM aurora_retail.sales.fact_sales f
JOIN aurora_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN aurora_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT :limit_n;
```

- **Usage Guidance**: `Use for "top products", "best sellers", "what's selling" questions for the current month. Returns revenue, units, and margin per product, ranked by revenue. The Aera Wireless Earbuds Pro should rank high - it surged online this month.`

#### Query 4 - Channel performance this month (Online vs Retail)

- **Question**: `How did Online compare to Retail this month?`
- **Parameters**: _(none)_
- **SQL**:

```sql
SELECT
  s.channel,
  ROUND(SUM(f.net_sales))                                            AS revenue,
  SUM(f.quantity)                                                    AS units,
  ROUND(SUM(f.net_sales) / COUNT(DISTINCT f.order_id), 2)            AS avg_order_value,
  ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1)             AS margin_pct
FROM aurora_retail.sales.fact_sales f
JOIN aurora_retail.sales.dim_store s ON f.store_key = s.store_key
JOIN aurora_retail.sales.dim_date d  ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
GROUP BY s.channel
ORDER BY revenue DESC;
```

- **Usage Guidance**: `Use for Online-vs-Retail or channel-mix questions. Returns revenue, units, AOV, and margin percent per channel for the current month.`

#### Query 5 - Monthly revenue trend

- **Question**: `Show me the monthly revenue trend.`
- **Parameters**: _(none)_
- **SQL**:

```sql
SELECT d.year_month, ROUND(SUM(f.net_sales)) AS revenue, ROUND(SUM(f.gross_margin)) AS margin
FROM aurora_retail.sales.fact_sales f
JOIN aurora_retail.sales.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month;
```

- **Usage Guidance**: `Use for trend / time-series / "over time" questions. Returns revenue and margin per month across the full 12 months. Good for a line chart.`

#### Query 6 - Revenue & margin by category this month

- **Question**: `What's our revenue and margin by category this month?`
- **Parameters**: _(none)_
- **SQL**:

```sql
SELECT
  p.category,
  ROUND(SUM(f.net_sales))                                  AS revenue,
  ROUND(SUM(f.gross_margin))                               AS margin,
  ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1)   AS margin_pct
FROM aurora_retail.sales.fact_sales f
JOIN aurora_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN aurora_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM aurora_retail.sales.dim_date)
GROUP BY p.category
ORDER BY revenue DESC;
```

- **Usage Guidance**: `Use for category-mix and category-margin questions for the current month. Outdoor & Camping will look weak - it collapsed in the West this month.`

#### Query 7 - Discount what-if (wraps simulate_discount)

- **Question**: `What if we discount product :product_id by :discount_pct percent?`
- **Parameters**: `product_id` (String, e.g. `SKU-1042`), `discount_pct` (Integer, e.g. `15`)
- **SQL**:

```sql
SELECT * FROM aurora_retail.sales.simulate_discount(:product_id, :discount_pct);
```

- **Usage Guidance**: `Use for any pricing what-if ("what if we mark down X by Y percent", "impact of discounting X"). Resolve a product NAME to its product_id via dim_product first, then pass it. Returns projected units, revenue, margin, margin percent, and a margin_safe flag. CALL SYNTAX: positional arguments only. Having this as a certified query gives Genie a worked example so it reliably routes to the function instead of writing historical SQL.`

#### Query 8 - Top gap drivers for a region (wraps top_drivers)

- **Question**: `What's driving the gap in region :region this month?`
- **Parameters**: `region` (String, e.g. `West`)
- **SQL**:

```sql
SELECT * FROM aurora_retail.sales.top_drivers(:region) ORDER BY rank;
```

- **Usage Guidance**: `Use when the user asks "what's dragging down [region]", "what's driving the gap in [region]", or "top detractors for [region]". The function scopes to the current month automatically - no month argument needed. Returns the precomputed ranked drivers (rank 1 = biggest). For the West, rank 1 is Outdoor & Camping. CALL SYNTAX: positional argument only.`

### 7b. Register the 3 UC functions

Stay in **Instructions → SQL Queries**. Click **+ Add → SQL function**. The dialog has 3 dropdowns
(Catalog / Schema / Function) and **no Usage Guidance field** - Genie reads the function's UC `COMMENT`
automatically. The notebooks already attached rich what/how/avoid comments.

| Catalog         | Schema  | Function            |
| --------------- | ------- | ------------------- |
| `aurora_retail` | `sales` | `simulate_discount` |
| `aurora_retail` | `sales` | `top_drivers`       |
| `aurora_retail` | `sales` | `explain_driver`    |

Click **Save** after each.

> To tune what Genie tells itself about a function, edit the `COMMENT` in
> `notebooks/03_Setup_Functions.py` and re-run that notebook - `CREATE OR REPLACE FUNCTION` is
> idempotent and just updates the comment. There is no comment override in the Genie UI.

---

## 8. About tab - Common Questions

Back in **About** → **Edit common questions**. These show as suggested prompts in new chats. Each has a
per-question **Agent** toggle: **ON** lets Genie call UC functions and reason in multiple steps (needed
for function calls); **OFF** is single-shot NL→SQL (faster).

Pick any subset that fits. These span the schema and exercise both certified queries and functions - 
no prescribed order, learners can ask them however they want:

| Question                                                    | Mode                            |
| ----------------------------------------------------------- | ------------------------------- |
| `How are sales pacing against our monthly target?`          | Standard                        |
| `Which regions are missing target this month, worst first?` | Standard                        |
| `What are our top 10 products by revenue this month?`       | Standard                        |
| `How did Online compare to Retail this month?`              | Standard                        |
| `What's our revenue and margin by category this month?`     | Standard                        |
| `Show me the monthly revenue trend.`                        | Standard                        |
| `Which loyalty tier drives the most revenue this month?`    | Standard                        |
| `What's driving the gap in the West?`                       | **Agent** (`top_drivers`)       |
| `What if we discount the Summit 600 Down Jacket by 15%?`    | **Agent** (`simulate_discount`) |

Click **Save**. The two Agent-mode rows are the only ones that need the toggle on.

---

## 9. Next

Work the guides in numeric order:

1. **Benchmark it** (`02_Benchmarks.md`) - paste in the 10 ground-truth questions + Evaluation notes,
   set the run-wide mode to **Agent**, hit **▶ Run all benchmarks**, get a hard accuracy number.
2. **Test & iterate** (`03_Test_Workflows.md`) - learn how to read Genie's Thinking trace, run a
   multi-turn conversation, and use the diagnostic toolkit to close whatever the benchmark surfaced.
   Then re-run the benchmark and watch the score move. **This loop is the lab.**
3. **Surface it** (`04_Dashboard_And_Embedded_Genie.md`) - build the AI/BI dashboard with embedded Genie.

When Genie misreads something, the fix order is: **Instructions Text → the relevant Join instruction →
SQL Expression → certified Query**. For wrong function routing, fix the function `COMMENT` in notebook 03.
