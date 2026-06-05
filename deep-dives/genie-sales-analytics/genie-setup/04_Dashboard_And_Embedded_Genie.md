# AI/BI Dashboard + Embedded Genie

The Genie space is the conversational surface. The **AI/BI dashboard** is the visual one - and you can
**embed the Genie space inside it** so a viewer sees charts and can ask follow-up questions in the same
place. This guide builds a 2-page dashboard on the same star schema and links your Genie space to it.

Work through this after the Genie space is built, benchmarked, and tested. Everything runs on Free Edition.

---

## 1. Create the dashboard

**AI/BI → Dashboards → New dashboard**. Rename it to `Bramblepeak Sales Cockpit` (click the title to edit).

The page opens with a **"Create a dashboard with Genie"** prompt in the centre - that's **Genie Code**,
an AI-assisted dashboard builder. We'll use it in § 4, but datasets need to exist first. Click
**CREATE MANUALLY ↓** below the prompt box to dismiss the Genie Code intro.

The editor has two tabs at the top: **Data** (where datasets live) and **Untitled page** (the canvas
where widgets live). Build datasets first.

---

## 2. Datasets (Data tab → + Add SQL dataset)

Click the **Data** tab at the top of the editor, then **+ Add SQL dataset** in the left panel for each
entry below. Paste the SQL, name the dataset (use the bolded name like `region_vs_target`), and save.

Several reuse the SQL from your Genie certified queries - that's intentional: the dashboard and Genie
read the *same* governed SQL, so the numbers always match. (You can ignore **+ Add dataset** - that's
for picking an existing table directly without writing SQL.)

**A - `region_vs_target`**
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
       ROUND(a.revenue - t.revenue_target) AS gap_to_target,
       ROUND(100 * a.revenue / t.revenue_target, 1) AS pct_to_target,
       CASE WHEN a.revenue >= t.revenue_target THEN 'ON TRACK' ELSE 'OFF TRACK' END AS status
FROM bramblepeak_retail.sales.sales_targets t
JOIN actual a ON a.region = t.region
WHERE t.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
ORDER BY pct_to_target ASC;
```

**B - `monthly_trend`**
```sql
SELECT d.year_month, ROUND(SUM(f.net_sales)) AS revenue, ROUND(SUM(f.gross_margin)) AS margin
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_month ORDER BY d.year_month;
```

**C - `top_products`**
```sql
SELECT p.product_name, p.category, ROUND(SUM(f.net_sales)) AS revenue, SUM(f.quantity) AS units
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.product_name, p.category
ORDER BY revenue DESC LIMIT 10;
```

**D - `category_perf`**
```sql
SELECT p.category, ROUND(SUM(f.net_sales)) AS revenue, ROUND(SUM(f.gross_margin)) AS margin,
       ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1) AS margin_pct
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_product p ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_date d    ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY p.category ORDER BY revenue DESC;
```

**E - `channel_perf`**
```sql
SELECT s.channel, ROUND(SUM(f.net_sales)) AS revenue, SUM(f.quantity) AS units,
       ROUND(100 * SUM(f.gross_margin) / SUM(f.net_sales), 1) AS margin_pct
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_store s ON f.store_key = s.store_key
JOIN bramblepeak_retail.sales.dim_date d  ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date)
GROUP BY s.channel ORDER BY revenue DESC;
```

**F - `current_month_sales_detail`** (drives the *embedded Genie*, not a widget)
```sql
SELECT f.sale_id, d.year_month, s.region, s.channel, p.category, p.product_name,
       c.loyalty_tier, f.quantity, f.net_sales, f.gross_margin
FROM bramblepeak_retail.sales.fact_sales f
JOIN bramblepeak_retail.sales.dim_store s    ON f.store_key = s.store_key
JOIN bramblepeak_retail.sales.dim_product p  ON f.product_key = p.product_key
JOIN bramblepeak_retail.sales.dim_customer c ON f.customer_key = c.customer_key
JOIN bramblepeak_retail.sales.dim_date d     ON f.date_key = d.date_key
WHERE d.year_month = (SELECT MAX(year_month) FROM bramblepeak_retail.sales.dim_date);
```
> Add this as a dataset only (no widget). The embedded Genie picks it up as queryable context so it can
> answer ad-hoc current-month questions that aren't pre-aggregated into the other datasets.

---

## 3. Link the Genie space to the dashboard

Do this **before** generating widgets so the linked space is available to viewers from the moment the
dashboard is published.

> The colourful icon in the bottom toolbar opens **Genie Code** (the dashboard-builder AI) - that is
> NOT the chat embed. Linking a Genie *space* is done in **Settings and themes** and applies to the
> whole dashboard.

1. Top-right of the editor → click the **⋮** menu → **Settings and themes**.
2. In the Settings panel, click the **General** tab (next to Theme).
3. Scroll to **Enable Genie** and make sure the toggle is ON.
4. Choose **Link existing Genie space** - the second radio. **Do NOT pick "Auto-generate Genie space"**
   (the recommended default); that builds a brand-new space from the dashboard data and ignores all
   your Instructions, joins, expressions, certified queries, and function registrations.
5. A **Paste URL here…** input appears. Paste the URL of your Bramblepeak Sales Assistant space (open the
   space in another tab and copy its URL from the browser address bar - the Genie space ID at the end
   of the URL is what the field actually needs; pasting the full URL works fine, the field extracts the
   ID). The red "Invalid Genie ID or URL" warning under the field clears once a valid value is in.
6. Close the Settings panel.

When the dashboard is published (§ 4), viewers get a Genie chat button that opens the linked space,
preloaded with the dashboard's filter context, on every page.

### The one architectural gotcha to know

> ⚠️ *Based on hands-on observation while building this lab - Databricks ships fast, so this behaviour
> may already have changed by the time you're reading this. Verify on your own workspace before
> designing demos around it: ask the embedded chat a discount what-if (or any prompt that should route
> to `simulate_discount` / `top_drivers` / `explain_driver`) and check the Thinking trace. If you see
> a function call there, this whole section is out of date - please ignore the table below.*

**"Linked" is not "the same".** The dashboard-embedded Genie inherits the linked space's *context*
(Instructions, joins, expressions, certified queries) but **NOT the function-call capability**. The embed
can only run SQL against the dashboard datasets - it will **not** call `simulate_discount`, `top_drivers`,
or `explain_driver`.

| Capability                       |   Dashboard embed   |   Full Genie space   |
| -------------------------------- | :-----------------: | :------------------: |
| SQL over dashboard datasets      |          ✅          |           ✅          |
| SQL over all star-schema tables  |          ❌          |           ✅          |
| Call UC functions                |          ❌          |           ✅          |
| Chat / Agent mode toggle         |          ❌          |           ✅          |

**Practical consequence (if the limitation still holds):** the on-track, region, product, channel,
category, and trend questions all work in the embed (the datasets hold the answers). But the
**discount what-if** and the **top-drivers function** need the **full Genie space** - open it in a
second browser tab (sidebar → **Genie Spaces** → Bramblepeak Sales Assistant) and flip to it for those.

---

## 4. Generate widgets with Genie Code

Switch from the **Data** tab back to the **Untitled page** tab at the top. You're back at the
"Create a dashboard with Genie" prompt box.

Building widgets one at a time in the right-side config panel is boring busywork - not where a
Databricks data engineer's time goes. **Let Genie Code do it.** Paste this prompt into the input box
and hit send:

```text
Build a 2-page sales cockpit for Bramblepeak Outfitters on bramblepeak_retail.sales.

Page 1 "This Month vs Target": KPI counters for company actual revenue, target attainment %,
and dollar gap; a table of region vs target sorted by attainment ascending; a grouped bar of actual
vs target by region; a 12-month line trend of revenue and margin.

Page 2 "Products & Channels": top 10 products by revenue (horizontal bar); channel comparison table
(Online vs Retail) with revenue, units, margin percent; revenue by category (horizontal bar).

Use dim_store.region for regional reporting (not dim_customer.region). Revenue = SUM(net_sales).
"This month" = the latest year_month in dim_date. Compare actual to sales_targets.revenue_target.
```

In ~30 seconds Genie Code generates a working page - KPI counters showing actual revenue, attainment %,
and the dollar gap; a region-vs-target table; a grouped Actual-vs-Target bar; a multi-line revenue +
margin trend. Not a wireframe, an actual working dashboard.

### Now experiment freely

The dashboard above is the seed. The interesting work is making it yours:

- **Ask Genie Code for more.** Type follow-ups into the same prompt box: *"Add a counter for total
  units sold this month." "Split the channel table into separate Online and Retail blocks." "Add a
  heatmap of revenue by region and category." "Move the trend chart to its own page."* Each follow-up
  modifies the existing dashboard instead of starting over.
- **Tweak what you don't like.** Click any widget and edit it in the right-side config panel - change
  chart type, swap a field, change colours. Genie Code gives you a faster start than building from
  zero; the refinement is yours.
- **Promote your best prompts into permanent widgets.** Got a prompt from `03_Test_Workflows.md` you
  love? Ask Genie Code to add a widget that visualises the same insight. This is how a "demo dashboard"
  becomes a "team dashboard."

> Genie Code occasionally puts a widget on the wrong page, picks the wrong field, or writes a query
> that doesn't quite match your intent. Fix the obvious misses in the config panel and keep moving - 
> a "good enough" dashboard you can extend beats two hours spent pixel-pushing a "perfect" one.

**Publish** the dashboard (top-right **Publish** button) once you're happy. Publishing is required for
the embedded Genie chat to be reachable to viewers.

---

## 5. Demo question bank - two surfaces

Once published, your dashboard has two AI surfaces side by side: the visuals (precomputed, fast) and
the embedded Genie chat (ad-hoc, conversational). Plus the full Genie space in a separate tab for
function-call prompts. Here's what to ask where:

🟦 = runs in the **dashboard-embedded Genie** · 🟪 = needs the **full Genie space** (function calls)

> *If § 3's caveat is now out of date and the embed can call UC functions on your workspace, treat the
> 🟪 rows as 🟦 - they'll work in the embed too.*

| Prompt                                                       | Surface | Why                                            |
| ------------------------------------------------------------ | :-----: | ---------------------------------------------- |
| `What was our total revenue this month?`                     |    🟦    | reads `region_vs_target` / detail dataset      |
| `Which region is furthest behind target?`                    |    🟦    | reads `region_vs_target`                       |
| `Top products this month?`                                   |    🟦    | reads `top_products`                           |
| `Online vs Retail this month?`                               |    🟦    | reads `channel_perf`                           |
| `Show the revenue trend`                                     |    🟦    | reads `monthly_trend`                          |
| `What's driving the gap in the West?`                        |    🟪    | needs `top_drivers` function                   |
| `What if we discount the Summit 600 Down Jacket by 15%?`     |    🟪    | needs `simulate_discount` function             |

**Tips for a live walkthrough:** pre-warm the warehouse; open both tabs (dashboard + full space)
before you start; expand Genie's "Thinking" steps on screen so viewers see the governed SQL/function
behind each answer; if Genie answers wrong, rephrase using the words from the matching certified
query's Question.

---

## Done

You now have the full stack: a star schema, a tuned Genie space with custom functions, a regression
benchmark, and an AI/BI dashboard with embedded Genie - all on Free Edition. This is exactly the
"natural-language analytics on your warehouse" capability teams are standing up in 2026.
