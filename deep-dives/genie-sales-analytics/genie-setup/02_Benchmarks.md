# Benchmarks - measurable Genie accuracy

Time: ~1.5 hours.

A Genie Agent without a benchmark is an agent you can only spot-check by eye. The **Benchmarks** tab gives you a regression-tested accuracy %: a fixed set of questions, each with ground-truth SQL and an Evaluation note, graded on every run. As you tune general instructions / joins / expressions / example queries, the score moves - that's how you know a change helped instead of just feeling like it did.

> This is also the answer to the question every stakeholder asks: _"how do I know it stays accurate?"_ A benchmark where most questions rate **Good** beats any verbal claim about quality.

---

## Where it lives

In the Genie Agent, the **Benchmarks** tab sits next to Chat / Monitor. Two sub-tabs:

- **Questions** - the corpus. Each row holds a question + (optional) **SQL Answer** + (optional) Evaluation note.
- **Evaluations** - runs. Each produces an accuracy % vs the corpus.

At the top of the tab there's an **Agent / Chat** toggle and a **Run benchmarks** button. The toggle is **run-wide**: every question in a run uses the same mode.

> **Pick Agent mode for runs.** It answers plain SQL questions, can call the UC functions Q-B9 and Q-B10 expect, and is the only mode in which the LLM judge grades your Evaluation notes (in Chat mode a run only compares SQL result sets).

## The iteration loop

```
1. Curate questions + ground-truth SQL + Evaluation notes  (one-time, ~30-45 min for ~10 questions)
2. Set mode to Agent, hit Run benchmarks -> accuracy %
3. Open failed questions -> see why Genie missed
4. Adjust general instructions / joins / expressions / example queries / function COMMENTs
5. Re-run -> did accuracy improve?
6. Repeat until the score plateaus (typically 85-95%)
```

## Setup

1. Genie Agent → **Benchmarks** tab → **Questions** sub-tab.
2. If Genie offers suggested benchmark questions, skip them for now: the 10 below are the yardstick the score thresholds assume. You can add your own after the first run.
3. For each of Q-B1 to Q-B10 below, click **Add benchmark** and fill:
   - **Question** - the natural-language prompt (the heading of each block below)
   - **SQL Answer** - the SQL block (optional but recommended: without it the question is marked **Manual review needed**)
   - **Evaluation note** - read by the LLM judge in Agent-mode runs

   Click **Add benchmark** to save each one.
4. **Check:** the Questions sub-tab lists 10 questions. Then go to **Run it, then iterate** below.

> How a run is scored depends on the mode. In **Chat** mode Genie runs its own SQL and compares the result set with your ground-truth SQL (up to 5,000 rows); a match is **Good**. In **Agent** mode, the mode you use here, an LLM judge reads the whole answer and grades it against the **Evaluation note**, so the note has to name everything a correct answer contains. The ground-truth SQL is still worth writing: it is your reference when you open a failure, and it is what a Chat-mode run would score against. A question with no SQL answer is marked **Manual review needed**.

---

## Ground-truth SQL + Evaluation notes (paste each into its respective field)

### Q-B1: How are sales pacing against our monthly target?

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
  ROUND(SUM(a.revenue))                                  AS company_actual_revenue,
  ROUND(SUM(t.revenue_target))                           AS company_target,
  ROUND(SUM(a.revenue) - SUM(t.revenue_target))          AS gap_to_target,
  ROUND(100 * SUM(a.revenue) / SUM(t.revenue_target), 1) AS pct_to_target,
  CASE WHEN SUM(a.revenue) >= SUM(t.revenue_target) THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM bramblepeak_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date);
```

**Evaluation note**: `Response includes company actual revenue, monthly target, the dollar gap or percent-to-target, and an explicit ON TRACK / OFF TRACK label. Percent-to-target is about 95 and the label is OFF TRACK. Does not enumerate regions or products.`

### Q-B2: Which regions are missing target this month, worst first?

```sql
WITH actual AS (
  SELECT s.region, SUM(f.net_sales) AS revenue
  FROM bramblepeak_retail.sales.fact_sales f
  JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
  JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
  WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
  GROUP BY s.region
)
SELECT t.region, ROUND(a.revenue) AS actual_revenue, ROUND(t.revenue_target) AS revenue_target,
       ROUND(100 * a.revenue / t.revenue_target, 1) AS pct_to_target
FROM bramblepeak_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
ORDER BY pct_to_target ASC;
```

**Evaluation note**: `Response lists all four regions (West, Midwest, Northeast, South) with their actual vs target, sorted worst first. The West appears at the top as the biggest miss. Uses dim_store.region, not dim_customer.region.`

### Q-B3: What are our top 10 products by revenue this month?

```sql
SELECT p.product_id, p.product_name, p.category, ROUND(SUM(f.net_sales)) AS revenue
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.product_id, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;
```

**Evaluation note**: `Response returns up to 10 products ranked by revenue (descending) for the current month, with product names. Aera Wireless Earbuds Pro appear in the top 10.`

### Q-B4: How did Online compare to Retail this month?

```sql
SELECT s.channel, ROUND(SUM(f.net_sales)) AS revenue, SUM(f.quantity) AS units,
       ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1) AS margin_pct
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY s.channel
ORDER BY revenue DESC;
```

**Evaluation note**: `Response compares both channels (Online and Retail) with revenue and margin percent (or absolute margin) for the current month. Channel comes from dim_store.channel.`

### Q-B5: What's our revenue and margin by category this month?

```sql
SELECT p.category, ROUND(SUM(f.net_sales)) AS revenue, ROUND(SUM(f.gross_margin)) AS margin,
       ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1) AS margin_pct
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.category
ORDER BY revenue DESC;
```

**Evaluation note**: `Response shows all 8 product categories with both revenue and margin (absolute or percent) for the current month.`

### Q-B6: Show the monthly revenue trend.

```sql
SELECT d.year_month, ROUND(SUM(f.net_sales)) AS revenue
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_month
ORDER BY d.year_month;
```

**Evaluation note**: `Response returns a 12-month time series of net revenue, ordered chronologically by year_month. Suitable for a line chart.`

### Q-B7: Which loyalty tier drives the most revenue this month?

```sql
SELECT c.loyalty_tier, ROUND(SUM(f.net_sales)) AS revenue, COUNT(DISTINCT f.order_id) AS orders
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_customer c ON f.customer_key = c.customer_key
JOIN bramblepeak_retail.sales.dim_date d     ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY c.loyalty_tier
ORDER BY revenue DESC;
```

**Evaluation note**: `Response includes all three loyalty tiers (Standard, Plus, VIP) with revenue per tier for the current month, ranked.`

### Q-B8: What's the average order value by region this month?

```sql
SELECT s.region,
       ROUND(SUM(f.net_sales) / COUNT(DISTINCT f.order_id), 2) AS avg_order_value,
       COUNT(DISTINCT f.order_id) AS orders
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY s.region
ORDER BY avg_order_value DESC;
```

**Evaluation note**: `Response reports AOV (revenue per distinct order) per region for the current month, using dim_store.region. Higher-AOV regions appear first.`

### Q-B9: What's driving the gap in the West this month?

> Tests routing to `top_drivers`. The function reads `performance_insights`, so this self-anchored query over that table returns the same rows as `top_drivers('West')` - without depending on the function.

```sql
SELECT rank, driver_type, dimension_value, revenue_current, revenue_prior,
       revenue_delta, contribution_to_gap_pct, insight
FROM bramblepeak_retail.sales.performance_insights
WHERE region = 'West'
  AND year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
ORDER BY rank;
```

**Evaluation note**: `Response calls the top_drivers function (visible in the Thinking trace) rather than writing hand-built SQL, and surfaces Outdoor & Camping as the #1 driver of the West's month-over-month revenue decline.`

### Q-B10: What if we discount the Summit 600 Down Jacket by 15%?

> Ground truth is the function output. Tests product-name resolution + routing to `simulate_discount`.

```sql
SELECT * FROM bramblepeak_retail.sales.simulate_discount('SKU-1042', 15);
```

**Evaluation note**: `Response resolves "Summit 600 Down Jacket" to SKU-1042 and calls simulate_discount('SKU-1042', 15) (visible in the Thinking trace) rather than writing hand-built SQL. Reports projected units, projected revenue, projected margin, and the margin_safe flag.`

---

## Interpreting results

| Accuracy | Status      | What to do                                         |
| -------- | ----------- | -------------------------------------------------- |
| > 90%    | Strong      | Ship it                                            |
| 80-90%   | Solid       | Note the failures; iterate when convenient         |
| 60-80%   | Tunable     | Likely general-instruction gaps - fix before relying on it |
| < 60%    | Investigate | Structural issue - joins, comments, or schema      |

**Common failure modes and fixes:**

- _Genie uses `dim_customer.region` instead of `dim_store.region`_ → the REGION line in the general instructions says "store region for regional reporting" - strengthen it.
- _Genie sums `unit_price` instead of `net_sales` for revenue_ → strengthen the `total_revenue` measure in **Configure → Examples** (sharper Synonyms / Instructions on that measure).
- _Genie writes raw SQL instead of calling a function_ (Q-B9, Q-B10) → strengthen the function `COMMENT` in `notebooks/03_Setup_Functions.py` (the HOW TO USE / WHAT TO AVOID block) and re-run notebook 03.
- _Genie groups differently than the ground truth_ → add or sharpen a SQL expression that matches the expected aggregation.

Log each run's score + root cause + fix somewhere you can diff over time - that history is what turns "it feels better" into "Day-1 62% → Day-3 91%".

---

## Run it, then iterate

The setup above is the easy half; the next steps are where the accuracy comes from.

1. At the top of the Benchmarks tab, toggle the mode to **Agent**.
2. Click **Run benchmarks**. The run takes ~2-3 min; then **Evaluations** shows an overall accuracy % and a rating per question: **Good**, **Bad** or **Manual review needed**.

Now the part nobody can do for you. A benchmark only earns its keep when you read the failures and close them yourself:

- Open each question rated **Bad** or **Manual review needed**. Genie shows the SQL it generated and (in Agent mode) the function calls it made or skipped.
- **Diagnose** the miss. Did it use `dim_customer.region`? Sum `unit_price` instead of `net_sales`? Write SQL instead of calling `top_drivers`? Hardcode a year-month? Pick the wrong grouping?
- **Pick the right lever** to fix it - and apply the fix in the smallest surface possible: the general instructions, a measure's Synonyms / Instructions (Configure → Examples), an example query's **Usage Guidance**, or a function `COMMENT` (edit `notebooks/03_Setup_Functions.py` and re-run that notebook).
- **Re-run** the benchmark. Did the score move? Why?
- **Repeat** until it plateaus (typically 85-95% after 2-4 passes).

Read failure → pick lever → re-run → measure: that loop is how a Genie Agent becomes production-ready. The corpus and Evaluation notes above are your fixed yardstick; the tuning is yours.

When you hit a failure and need help reading what Genie actually did (Thinking trace, which query or function it picked, why a routing went wrong), open **`03_Test_Workflows.md`** - that's the diagnostic toolkit. Use it, fix the smallest surface, come back here, re-run the benchmark, measure.
