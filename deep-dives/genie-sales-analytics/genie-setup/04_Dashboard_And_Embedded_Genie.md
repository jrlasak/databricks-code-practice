# AI/BI Dashboard + Embedded Genie

Time: ~45 minutes.

The Genie Agent is the conversational surface. The **AI/BI dashboard** is the visual one - and you can **link the Genie Agent to it** so a viewer sees charts and can ask follow-up questions in the same place. This guide builds a 2-page dashboard on the same star schema and links your Genie Agent to it.

Work through this after the Genie Agent is built, benchmarked, and tested. Everything runs on Free Edition.

---

## 1. Create the dashboard

Sidebar → **New** → **Dashboard**. Rename it to `Bramblepeak Sales Cockpit` (click the title to edit).

The canvas opens with a prompt box in the center - that's **Genie Code**, an AI-assisted dashboard builder. You'll use it in § 4, once the datasets exist. For now, dismiss the prompt or go straight to the **Data** tab.

The editor has two tabs at the top: **Data** (where datasets live) and **Untitled page** (the canvas where widgets live). Build datasets first.

---

## 2. Datasets (Data tab → + Add SQL dataset)

Click the **Data** tab at the top of the editor, then **+ Add SQL dataset** in the left panel for each entry below. Paste the SQL, name the dataset (use the bolded name like `region_vs_target`), and save.

Several reuse the SQL from your Genie example queries - that's intentional: the dashboard and Genie read the *same* governed SQL, so the numbers always match. (You can ignore **+ Add dataset** - that's for picking an existing table directly without writing SQL.)

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

---

## 3. Link the Genie Agent to the dashboard

Do this **before** generating widgets so the linked agent is available to viewers from the moment the dashboard is published.

> The colorful icon in the bottom toolbar opens **Genie Code** (the dashboard-builder AI) - that is NOT the chat embed. Linking a Genie Agent is done in the dashboard **Settings** and applies to the whole dashboard. Databricks moves buttons in the dashboard editor often: if the icon or a menu isn't where this guide says, look for the same name elsewhere in the editor toolbar or menus.

1. Top-right of the editor → **⋮** menu → **Settings**.
2. Click the **General** tab.
3. Scroll to **Enable Genie** and make sure the toggle is ON.
4. Choose **Link existing Genie Agent**. Do NOT leave it unlinked: an unlinked dashboard gets an auto-generated agent built from the dashboard data when you publish, and that agent ignores your general instructions, joins, SQL expressions, example queries and functions.
5. A **Paste URL here…** input appears. Paste the URL of your Bramblepeak Sales Assistant agent (open the agent in another tab and copy its URL from the browser address bar; the field extracts the agent ID from the full URL). The red "Invalid Genie ID or URL" warning under the field clears once a valid value is in.
6. Close the Settings panel.

When the dashboard is published (§ 4), viewers get an **Ask Genie** button on every page that opens a chat with the linked agent. Dashboard filters do not carry over into that chat, so a viewer who filtered to the West still has to say "in the West".

### Check what the dashboard chat can do

**Linked is not the same as opening the agent.** Databricks changes this often, so check it on your workspace once you publish in § 4: click **Ask Genie**, ask a discount what-if (or any prompt that should route to `simulate_discount` / `top_drivers` / `explain_driver`) and open the Thinking trace. If it shows a function call, the dashboard chat calls UC functions on your workspace and the table below does not apply.

| Capability          |   Dashboard **Ask Genie** chat   |   Full Genie Agent   |
| ------------------- | :------------------------------: | :------------------: |
| Call UC functions   |                ❌                 |          ✅           |

**Practical consequence:** the on-track, region, product, channel, category, and trend questions all work in the dashboard chat (they are plain SQL over the agent's tables). But the **discount what-if** and the **top-drivers function** need the **full Genie Agent** - open it in a second browser tab (sidebar → **Genie Agents** → Bramblepeak Sales Assistant) and flip to it for those.

---

## 4. Generate widgets with Genie Code

Switch from the **Data** tab back to the **Untitled page** tab at the top. You're back at the Genie Code prompt box in the center of the canvas (if you dismissed it in § 1, the colorful icon in the bottom toolbar reopens Genie Code; if it has moved, look for **Genie Code** in the editor toolbar).

Building widgets one at a time in the right-side config panel is boring busywork - not where a Databricks data engineer's time goes. **Let Genie Code do it.** Paste this prompt into the input box and hit send:

```text
Build a 2-page sales cockpit for Bramblepeak Outfitters on bramblepeak_retail.sales.

Page 1 "This Month vs Target": KPI counters for company actual revenue, target attainment %,
and dollar gap; a table of region vs target sorted by attainment ascending; a grouped bar of actual
vs target by region; a 12-month line trend of revenue and margin.

Page 2 "Products & Channels": top 10 products by revenue (horizontal bar); channel comparison table
(Online vs Retail) with revenue, units, margin percent; revenue by category (horizontal bar).

Use dim_store.region for regional reporting (not dim_customer.region). Revenue = SUM(net_sales).
"This month" = the latest year_month in dim_date. Compare actual to sales_targets.revenue_target.
Compute target attainment % as SUM(actual_revenue) / SUM(revenue_target) * 100 over region_vs_target,
not an average of pct_to_target.
Use only the existing datasets region_vs_target, monthly_trend, top_products, category_perf and
channel_perf; do not create new ones.
```

In ~30 seconds Genie Code generates both pages. Page 1: KPI counters showing actual revenue, attainment %, and the dollar gap; a region-vs-target table; a grouped Actual-vs-Target bar; a multi-line revenue + margin trend. Page 2: the top-10 products bar, the Online-vs-Retail channel table and the revenue-by-category bar. Not a wireframe, an actual working dashboard.

**Check:** the attainment counter shows the same % as the storyline check's COMPANY line, and the region table has West on top, OFF TRACK. If the counter is a point or two above the COMPANY line, it is averaging the regional percentages: ask Genie Code to compute it as SUM(actual_revenue) / SUM(revenue_target) * 100.

### Now experiment freely

The dashboard above is the seed. The interesting work is making it yours:

- **Ask Genie Code for more.** Type follow-ups into the same prompt box: *"Add a counter for total units sold this month." "Split the channel table into separate Online and Retail blocks." "Add a heatmap of revenue by region and category." "Move the trend chart to its own page."* Each follow-up modifies the existing dashboard instead of starting over.
- **Tweak what you don't like.** Click any widget and edit it in the right-side config panel - change chart type, swap a field, change colors. Genie Code gives you a faster start than building from zero; the refinement is yours.
- **Promote your best prompts into permanent widgets.** Got a prompt from `03_Test_Workflows.md` you love? Ask Genie Code to add a widget that visualizes the same insight. This is how a first-draft dashboard becomes a "team dashboard."

> Genie Code occasionally puts a widget on the wrong page, picks the wrong field, or writes a query that doesn't quite match your intent. Fix the obvious misses in the config panel and keep moving - a "good enough" dashboard you can extend beats two hours spent pixel-pushing a "perfect" one.

**Publish** the dashboard (top-right **Publish** button) once you're happy. Viewers can only reach the embedded Genie chat after you publish.

**Check:** open the published dashboard, click **Ask Genie** and ask `Which region is furthest behind target?`. The answer names West, matching the region table.

---

## 5. Question bank - two surfaces

Once published, your dashboard has two surfaces side by side: the visuals (precomputed, fast) and the embedded Genie chat (ad-hoc, conversational). Plus the full Genie Agent in a separate tab for function-call prompts. Here's what to ask where:

🟦 = runs in the dashboard's **Ask Genie** chat · 🟪 = needs the **full Genie Agent** (function calls)

| Prompt                                                       | Surface | Why                                            |
| ------------------------------------------------------------ | :-----: | ---------------------------------------------- |
| `What was our total revenue this month?`                     |    🟦    | plain SQL (`total_revenue` measure)            |
| `Which region is furthest behind target?`                    |    🟦    | plain SQL (example Query 2)                    |
| `Top products this month?`                                   |    🟦    | plain SQL (example Query 3)                    |
| `Online vs Retail this month?`                               |    🟦    | plain SQL (example Query 4)                    |
| `Show the revenue trend`                                     |    🟦    | plain SQL (example Query 5)                    |
| `What's driving the gap in the West?`                        |    🟪    | needs `top_drivers` function                   |
| `What if we discount the Summit 600 Down Jacket by 15%?`     |    🟪    | needs `simulate_discount` function             |

**Tips for a live walkthrough:** pre-warm the warehouse; open both tabs (dashboard + full agent) before you start; expand Genie's "Thinking" steps on screen so viewers see the governed SQL/function behind each answer; if Genie answers wrong, rephrase using the words from the matching example query's Question.

---

## Done

You now have the full stack: a star schema, a tuned Genie Agent with custom functions, a regression benchmark, and an AI/BI dashboard with embedded Genie - all on Free Edition. This is the "natural-language analytics on your warehouse" capability teams are standing up now.
