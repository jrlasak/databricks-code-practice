# Benchmarks - measurable Genie accuracy

A Genie space without a benchmark is a space you can only spot-check by eye. The **Benchmark** tab gives
you a regression-tested accuracy %: a fixed set of questions, each with ground-truth SQL, scored on every
run. As you tune Instructions / Joins / Expressions / Queries, the score moves - that's how you know a
change helped instead of just feeling like it did.

> This is also the answer to the question every stakeholder asks: _"how do I know it stays accurate?"_
> A green-dominant benchmark beats any verbal claim about quality.

---

## Where it lives

In the Genie space, the **Benchmark** tab sits next to Chat / Monitor. Two sub-tabs:

- **Questions** - the corpus. Each row holds a question + (optional) ground-truth SQL + (optional)
  Evaluation note.
- **Evaluations** - runs. Each produces an accuracy % vs the corpus.

At the top of the tab there's an **Agent / Chat** toggle and a **▶ Run all benchmarks** button. The
mode toggle is **run-wide** - every question in a single run executes in the same mode. So you don't
mix modes per question; you pick one mode for the whole run.

> **Always pick Agent mode for runs.** It handles plain SQL questions perfectly _and_ can call
> the UC functions when the question routes to one - Chat mode can't call functions, so any
> function-backed question would fail in a Chat-mode run. One toggle, full coverage.

## The iteration loop

```
1. Curate questions + ground-truth SQL + Evaluation notes  (one-time, ~30-45 min for ~10 questions)
2. Set mode to Agent, hit Run all benchmarks -> accuracy %
3. Click into failed questions -> see why Genie missed
4. Adjust Instructions / Joins / Expressions / Queries / function COMMENTs
5. Re-run -> did accuracy improve?
6. Repeat until the score plateaus (typically 85-95%)
```

## Setup

1. Genie space → **Benchmark** tab → **Questions** sub-tab.
2. Genie auto-suggests questions. Click **Review** on each: verify (or replace) the draft ground-truth
   SQL against `bramblepeak_retail.sales`, then **Accept**. Reject anything off-scenario.
3. For the questions below that weren't auto-suggested, click **+ Add benchmark** and fill three fields:
   - **Question** - the natural-language prompt
   - **Ground truth SQL answer** (optional but recommended - without it, the question is marked for
     manual review instead of auto-scored)
   - **Evaluation note** (Agent-mode only - the LLM-judge reads this to verify the Agent's response)
4. Once all questions exist, name your benchmark (e.g. `Bramblepeak Sales Day-1 Accuracy`).
5. Toggle the mode at the top to **Agent**, then click **▶ Run all benchmarks**. Wait ~2-3 min for a
   per-question and overall green/yellow/red score.

> Scoring compares result sets, so small differences in column order or rounding usually still pass.
> When the question implies a grouping ("by region", "by category"), the ground truth must group that way.
> The Evaluation note adds a second check: even when the result set matches, the LLM judge verifies the
> response addressed the intent (e.g. "includes the ON TRACK / OFF TRACK label").

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

**Evaluation note**: `Response includes company actual revenue, monthly target, the dollar gap or percent-to-target, and an explicit ON TRACK / OFF TRACK label. Does not enumerate regions or products.`

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

**Evaluation note**: `Response returns up to 10 products ranked by revenue (descending) for the current month, with product names. Aera Wireless Earbuds Pro should rank near the top.`

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

> Tests routing to `top_drivers`. The function reads `performance_insights`, so this self-anchored query
> over that table returns the same rows as `top_drivers('West')` - without depending on the function.

```sql
SELECT rank, driver_type, dimension_value, revenue_current, revenue_prior,
       revenue_delta, contribution_to_gap_pct, insight
FROM bramblepeak_retail.sales.performance_insights
WHERE region = 'West'
  AND year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
ORDER BY rank;
```

**Evaluation note**: `Response calls the top_drivers function (visible in the Thinking trace) rather than writing hand-built SQL, and surfaces Outdoor & Camping as the #1 driver of the West's gap.`

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
| 80-90%   | Good        | Note the failures; iterate when convenient         |
| 60-80%   | Tunable     | Likely Instruction gaps - fix before relying on it |
| < 60%    | Investigate | Structural issue - joins, comments, or schema      |

**Common failure modes and fixes:**

- _Genie uses `dim_customer.region` instead of `dim_store.region`_ → the Instructions Text and the join-4
  instruction both say "store region for regional reporting" - strengthen them.
- _Genie sums `unit_price` instead of `net_sales` for revenue_ → strengthen the `total_revenue` measure
  in the SQL Expressions tab (sharper Synonyms / Instructions on that measure).
- _Genie writes raw SQL instead of calling a function_ (Q-B9, Q-B10) → strengthen the function `COMMENT`
  in `notebooks/03_Setup_Functions.py` (the HOW TO USE / WHAT TO AVOID block) and re-run notebook 03.
- _Genie groups differently than the ground truth_ → add or sharpen a SQL Expression that matches the
  expected aggregation.

Log each run's score + root cause + fix somewhere you can diff over time - that history is what turns
"it feels better" into "Day-1 62% → Day-3 91%".

---

## Now run it - and iterate (this part is the lab)

The setup above is the easy half. The iteration loop **is** the actual lab.

1. At the top of the Benchmark tab, toggle the mode to **Agent**.
2. Click **▶ Run all benchmarks** (next to **+ Add benchmark**). A spinner appears; the run takes
   ~2-3 min, then a per-question and overall green/yellow/red score lands under **Evaluations**.

Now the part nobody can do for you. A benchmark only earns its keep when you read the failures and
close them yourself:

- Click into each red or yellow question. Genie shows the SQL it generated and (in Agent mode) the
  function calls it made or skipped.
- **Diagnose** the miss. Did it use `dim_customer.region`? Sum `unit_price` instead of `net_sales`?
  Write SQL instead of calling `top_drivers`? Hardcode a year-month? Pick the wrong grouping?
- **Pick the right lever** to fix it - and apply the fix in the smallest surface possible:
  Instructions text, a Join's Instructions field, a SQL Expression's Synonyms / Instructions, a
  certified query's Usage Guidance, or a function `COMMENT` (edit `notebooks/03_Setup_Functions.py`
  and re-run that notebook).
- **Re-run** the benchmark. Did the score move? Why?
- **Repeat** until it plateaus (typically 85-95% after 2-4 passes).

That loop - read failure → pick lever → re-run → measure - is the entire skill this lab exists to
teach. The corpus and Evaluation notes above give you a fixed yardstick; the tuning is yours to do, and
the muscle you build doing it is the one that makes a Genie space production-ready in real life. **Don't
skip it. Don't outsource it. It is the implementation.**

When you hit a failure and need help reading what Genie actually did (Thinking trace, which query or
function it picked, why a routing went wrong), open **`03_Test_Workflows.md`** - that's the diagnostic
toolkit. Use it, fix the smallest surface, come back here, re-run the benchmark, measure.
