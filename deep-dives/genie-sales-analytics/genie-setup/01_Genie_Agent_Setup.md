# Genie Agent Setup - Bramblepeak Sales Assistant

Time: ~1.5 hours.

Step-by-step build of a production-grade Genie Agent on top of the star schema you just created. Start here once `00_Run_All` has finished and all 7 tables are populated.

> This is a **manual, click-through** build inside the Databricks UI. Genie configuration lives in the workspace, not in code - so this guide is the artifact, the same way a runbook is. Work through it top to bottom.

**What you'll configure:** Settings → Text (general instructions) → Examples (joins, SQL expressions, example queries, the 3 UC functions) → Common questions. Then benchmark it in `02_Benchmarks.md`, diagnose + iterate using `03_Test_Workflows.md`, and surface it in a dashboard via `04_Dashboard_And_Embedded_Genie.md`.

Everything is compatible with **Databricks Free Edition** (Genie, UC functions, and AI/BI dashboards all run on the Serverless Starter Warehouse).

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

Full namespace prefix: `bramblepeak_retail.sales.` - column names are clean `snake_case`, so **no backticks anywhere**.

---

## 1. Create the Genie Agent

Free Edition leads with **Connect your data** before you name the agent.

1. In the Databricks left sidebar, click **Genie Agents**.
2. Click **New** in the top-right corner.
3. The **Connect your data** dialog opens. Navigate the breadcrumb: **All catalogs → `bramblepeak_retail` → `sales`**. You should see all 7 tables:
   - `dim_customer`
   - `dim_date`
   - `dim_product`
   - `dim_store`
   - `fact_sales` ⬅ the fact
   - `performance_insights`
   - `sales_targets`

   Add all of them - the easiest way is to click the **+** icon on the right of the breadcrumb row to add the whole `sales` schema in one click. (You can also click each table individually if you'd rather pick them.)

4. Click **Create**.
5. Genie Code opens and suggests table descriptions and example queries. Accept the table descriptions if they read correctly (they only add context). Dismiss the suggested example queries: this guide supplies the curated set in § 7.

**Check:** the agent opens on the Chat tab, and **Configure** (gear icon, top right; § 2 walks through it) → **Sources** lists 7 tables.

---

## 2. Open the Configure panel

After creating the agent, the top toolbar shows **Chat / Monitor / Benchmarks** on the left and action icons on the right. Click **Configure** (gear icon) in the top-right to open the settings panel.

The Configure panel has these tabs:

| Tab          | What lives here                                                                                   |
| ------------ | ------------------------------------------------------------------------------------------------- |
| **Settings** | Title, default warehouse, tags, thumbnail, description, common questions                          |
| **Text**     | General instructions (the system prompt)                                                          |
| **Examples** | Example SQL queries, SQL functions, measures, filters, fields and joins, in one searchable list   |
| **Sources**  | Connected tables; click one for its columns, descriptions and synonyms                            |

> On Free Edition the default warehouse is the Serverless Starter Warehouse; leave it as is.

---

## 3. Settings - name, description, common questions

**Configure → Settings** has fields for **Title**, **Tags**, **Thumbnail**, **Description**, and **Common questions**. Genie pre-fills a suggested description and several suggested common questions; you can **Accept** each suggestion or click the **pencil** icon to edit. Tags and Thumbnail are optional - skip them.

- **Title**: `Bramblepeak Sales Assistant`
- **Description**: Genie's auto-suggestion is usually close (something like "this agent enables analysis of retail sales performance by region, product, category, and channel…"). If you'd rather pin it down, click the pencil and replace with:

  ```text
  AI assistant for Bramblepeak Outfitters retail sales. Answers questions about revenue, margin, target attainment, regional and channel performance, top products, and pricing what-ifs.
  ```
- **Common questions**: leave these for **§ 8**. Once the example queries and functions exist, you'll replace them with a curated set. For now, Accept any of Genie's auto-suggestions that look useful, or skip them.

---

## 4. Configure → Text - the system prompt

Open **Configure → Text**. You'll see a **General instructions** textarea with placeholder example text ("MCA stands for...", "Countries in the country_code column...", etc.) - that's just a prompt template, clear it. Paste the block below in its place and click **Save** (bottom-right).

```
You are a sales analyst assistant for Bramblepeak Outfitters, a US omnichannel outdoor and lifestyle
retailer. Answer questions about revenue, margin, target attainment, regional and channel performance,
top products, and pricing what-ifs.

CURRENT MONTH: "this month" = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date). Never
hardcode a year-month string. "Last month" = the second-most-recent month. "This year" = the year of
that month, never YEAR(current_date()).

RELATIVE DATES: the latest month in the data is a complete month. Anchor "today", "yesterday",
"last N days" and "month to date" on (SELECT MAX(date) FROM bramblepeak_retail.sales.dim_date),
never current_date().

REGION: for sales / revenue / channel reporting, region means dim_store.region (the store's region).
Use dim_customer.region only when the question is explicitly about where customers live.

PRODUCT-NAME RESOLUTION: when a user names a product, resolve to dim_product.product_id via
dim_product.product_name before calling a function. Quick anchors:
- "Summit 600 Down Jacket" / "the down jacket" -> SKU-1042
- "Aera Wireless Earbuds Pro" / "the earbuds" -> SKU-1009
For any other product: prefer an exact match (LOWER(product_name) = LOWER('<name>')); otherwise search with LOWER(product_name) LIKE LOWER('%<fragment>%'). If more than one product matches, list them with product_id and ask which one; never pick one silently.

OUTPUT: round dollars to the nearest dollar; percentages to 1 decimal. Tables for multi-row answers.
Always show product_name with product_id and region/category labels next to numbers.
```

Keep it to context that applies to every question. Databricks' guidance is that anything narrower belongs in its own surface: measures, joins, example queries and function syntax already live there, so this prompt holds only business context, the current-month and relative-date resolvers, the store-vs-customer region pin, product-name anchors and output preferences.

**Check:** in Chat, ask `What is this month?`. Expand Thinking: the SQL uses `MAX(year_month)` from `dim_date`, not a hardcoded date.

---

## 5. Configure → Examples → Joins - explicit join paths

Open **Configure → Examples → Add → Joins** for each row below. Pick the left and right table, enter the **Join condition** (e.g. `fact_sales.date_key = dim_date.date_key`) and choose the **Relationship Type** (Many to one). The store-vs-customer region rule lives in the general instructions (§ 4), not on the join.

| #   | Left         | Left col       | Right          | Right col      | Relationship |
| --- | ------------ | -------------- | -------------- | -------------- | ------------ |
| 1   | `fact_sales` | `date_key`     | `dim_date`     | `date_key`     | Many to one  |
| 2   | `fact_sales` | `product_key`  | `dim_product`  | `product_key`  | Many to one  |
| 3   | `fact_sales` | `store_key`    | `dim_store`    | `store_key`    | Many to one  |
| 4   | `fact_sales` | `customer_key` | `dim_customer` | `customer_key` | Many to one  |

Don't declare joins for `sales_targets` or `performance_insights`: they share `region` and `year_month` only as attributes, so a join would be many-to-many and multiply rows. The example queries in § 7 aggregate `fact_sales` to `(region, year_month)` first, then join `sales_targets`; `performance_insights` is read through `top_drivers`.

**Check:** the Examples list shows 4 joins, all with `fact_sales` on the left.

---

## 6. Configure → Examples → SQL expressions - reusable measures & filters

Go to **Configure → Examples → Add**, then pick **Measure** or **Filter** for each entry below. All of these are anchored on `fact_sales`; qualify every column with `fact_sales.` so the expression stays unambiguous across the 7 tables. Fill **Name** (the bold name, e.g. `total_revenue`), **Code**, **Synonyms**, and **Instructions** for each.

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

> **There is no "current month" filter expression.** A SQL expression is anchored to `fact_sales`, which has `date_key` but not `year_month`, so it can't find the latest month without a hardcoded date range. Every current-month example query joins `dim_date` and filters `year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)` instead.

**Check:** the Examples list shows 5 measures and 1 filter. In Chat, ask `What is our total revenue this month?`: the SQL sums `net_sales`.

---

## 7. Configure → Examples → example queries + the functions

Go to **Configure → Examples → Add**, then pick the example query entry for each query below. Do the example queries first, then attach the functions.

Queries with parameters (3, 7 and 8) and the three functions are *trusted assets*. When a Chat-mode answer uses the exact text of a parameterized query, or Genie answers with one of the functions, it marks the answer as verified.

### 7a. Example queries

Each has 4 fields: **Question** (NL label Genie matches intent against), **SQL**, **Parameters** (`:name` slots), **Usage Guidance** (what/how/avoid).

When the SQL contains a `:name` marker (or you click **Add parameter**), Genie adds it as a parameter. Click the gear icon next to it to open **Parameter details**, set the **Data type** this guide gives (the default is String) and add a **Comment** describing the allowed values, then test-run the query before you save it.

#### Query 1 - On-track status this month

- **Question**: `How are sales pacing against our monthly target?`
- **Parameters**: _(none)_
- **SQL**:

```sql
WITH actual AS (
  SELECT s.region, SUM(f.net_sales) AS revenue
  FROM bramblepeak_retail.sales.fact_sales f
  JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
  JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
  GROUP BY s.region
)
SELECT
  ROUND(SUM(a.revenue))                                    AS company_actual_revenue,
  ROUND(SUM(t.revenue_target))                             AS company_target,
  ROUND(SUM(a.revenue) - SUM(t.revenue_target))            AS gap_to_target,
  ROUND(100 * SUM(a.revenue) / SUM(t.revenue_target), 1)   AS pct_to_target,
  CASE WHEN SUM(a.revenue) >= SUM(t.revenue_target) THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM bramblepeak_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date);
```

- **Usage Guidance**: `Use when the user asks "are we hitting target", "how are sales tracking this month", or any company-level status question. Returns one row: company actual revenue, target, dollar gap, percent-to-target, and ON/OFF TRACK. Do NOT list regions or products here - if the user wants the regional breakdown, the per-region example query handles that. Anchored on the current month (the latest month in the data).`

#### Query 2 - Regional pacing vs target

- **Question**: `Which regions are missing target this month, worst first?`
- **Parameters**: _(none)_
- **SQL**:

```sql
WITH actual AS (
  SELECT s.region, SUM(f.net_sales) AS revenue
  FROM bramblepeak_retail.sales.fact_sales f
  JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
  JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
  GROUP BY s.region
)
SELECT
  t.region,
  ROUND(a.revenue)                              AS actual_revenue,
  ROUND(t.revenue_target)                       AS revenue_target,
  ROUND(a.revenue - t.revenue_target)           AS gap_to_target,
  ROUND(100 * a.revenue / t.revenue_target, 1)  AS pct_to_target,
  CASE WHEN a.revenue >= t.revenue_target THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM bramblepeak_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
ORDER BY pct_to_target ASC;
```

- **Usage Guidance**: `Use when the user asks "why are we behind", "which regions are missing target", "break it down by region", or any per-region pacing question. Returns each region's actual vs target sorted worst-first, so the worst-performing region lands at the top. To drill from a region into WHY it's down, call the top_drivers function.`

#### Query 3 - Top products by revenue this month

- **Question**: `What are our top products by revenue this month?`
- **Parameters**: `limit_n` (Integer; Comment: `How many products to return. Use 10 unless the user names a number.`)
- **SQL**:

```sql
SELECT
  p.product_id, p.product_name, p.category,
  ROUND(SUM(f.net_sales))    AS revenue,
  SUM(f.quantity)            AS units,
  ROUND(SUM(f.gross_margin)) AS margin
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT :limit_n;
```

- **Usage Guidance**: `Use for "top products", "best sellers", "what's selling" questions for the current month. Returns revenue, units, and margin per product, ranked by revenue. The Aera Wireless Earbuds Pro rank in the top 10 this month: they surged online.`

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
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
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
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_date d ON f.date_key = d.date_key
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
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.category
ORDER BY revenue DESC;
```

- **Usage Guidance**: `Use for category-mix and category-margin questions for the current month. Outdoor & Camping will look weak - it collapsed in the West this month.`

#### Query 7 - Discount what-if (wraps simulate_discount)

- **Question**: `What if we discount product :product_id by :discount_pct percent?`
- **Parameters**: `product_id` (String; Comment: `A dim_product.product_id such as SKU-1042. Resolve a product name to its id first.`), `discount_pct` (Integer; Comment: `Discount percent as a whole number, e.g. 15 for 15%.`)
- **SQL**:

```sql
SELECT * FROM bramblepeak_retail.sales.simulate_discount(:product_id, :discount_pct);
```

- **Usage Guidance**: `Use for any pricing what-if ("what if we mark down X by Y percent", "impact of discounting X"). Resolve a product NAME to its product_id via dim_product first, then pass it. Returns projected units, revenue, margin, margin percent, and a margin_safe flag. CALL SYNTAX: positional arguments only. Do NOT answer pricing what-ifs with SQL over historical sales.`

This example query gives Genie a worked example of the call, so it routes what-ifs to `simulate_discount` instead of writing SQL over historical sales.

#### Query 8 - Top decline drivers for a region (wraps top_drivers)

- **Question**: `What's driving the gap in region :region this month?`
- **Parameters**: `region` (String; Comment: `A store region: West, Midwest, Northeast or South.`)
- **SQL**:

```sql
SELECT * FROM bramblepeak_retail.sales.top_drivers(:region) ORDER BY rank;
```

- **Usage Guidance**: `Use when the user asks "what's dragging down [region]", "what's driving the gap in [region]", or "top detractors for [region]". The function scopes to the current month automatically - no month argument needed. Returns the precomputed ranked drivers of the region's month-over-month revenue decline (rank 1 = biggest). For the West, rank 1 is Outdoor & Camping. CALL SYNTAX: positional argument only.`

**Check:** the Examples list shows 8 example queries. Open Query 1 and run it: one row, status OFF TRACK.

### 7b. Attach the 3 UC functions

Still in **Configure → Examples**, click **Add**, then pick the function entry for each function below and select it by the catalog, schema and function name in the table (once attached, the function's description, its UC `COMMENT`, shows next to it). There is no Usage Guidance field for a function: Genie reads its UC `COMMENT`, and the notebooks already attached rich what/how/avoid comments.

| Catalog         | Schema  | Function            |
| --------------- | ------- | ------------------- |
| `bramblepeak_retail` | `sales` | `simulate_discount` |
| `bramblepeak_retail` | `sales` | `top_drivers`       |
| `bramblepeak_retail` | `sales` | `explain_driver`    |

Click **Save** after each.

> To tune what Genie tells itself about a function, edit the `COMMENT` in `notebooks/03_Setup_Functions.py` and re-run that notebook - `CREATE OR REPLACE FUNCTION` is idempotent and just updates the comment. There is no comment override in the Genie UI.

**Check:** the Examples list also shows the 3 functions. In Agent mode, ask `What's driving the gap in the West?`: Thinking shows a call to `top_drivers`.

---

## 8. Settings - Common questions

Go back to **Configure → Settings → Common questions**. These show as suggested prompts in new chats.

Pick any subset that fits. These span the schema and exercise both example queries and functions. Ask them in any order, phrased however you like:

| Question                                                    |
| ----------------------------------------------------------- |
| `How are sales pacing against our monthly target?`          |
| `Which regions are missing target this month, worst first?` |
| `What are our top 10 products by revenue this month?`       |
| `How did Online compare to Retail this month?`              |
| `What's our revenue and margin by category this month?`     |
| `Show me the monthly revenue trend.`                        |
| `Which loyalty tier drives the most revenue this month?`    |
| `What's driving the gap in the West?`                       |
| `What if we discount the Summit 600 Down Jacket by 15%?`    |

Click **Save**.

---

## 9. Next

Work the guides in numeric order:

1. **Benchmark it** (`02_Benchmarks.md`) - paste in the 10 ground-truth questions + Evaluation notes, run them in **Agent** mode with **Run benchmarks**, get a hard accuracy number.
2. **Test & iterate** (`03_Test_Workflows.md`) - learn how to read Genie's Thinking trace, run a multi-turn conversation, and use the diagnostic toolkit to close whatever the benchmark surfaced. Then re-run the benchmark and watch the score move.
3. **Surface it** (`04_Dashboard_And_Embedded_Genie.md`) - build the AI/BI dashboard with embedded Genie.

If an answer is wrong, fix the narrowest surface that owns the mistake: a wrong region in the REGION line of the **general instructions**, a wrong measure in the **SQL expression**, a wrong routing in the **example query**. For wrong function routing, fix the function `COMMENT` in notebook 03.
