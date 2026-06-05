# Test Workflows - how to validate the Genie space (and read what it did)

Configuring Genie is half the job. The other half is **verifying it answers correctly and learning to read
how it got there** - which query or function it ran, and what its reasoning steps were. That skill is what
separates "the AI said so" from "the AI ran this governed query, here's the SQL."

Work through this after **`02_Benchmarks.md`** - the benchmark gives you a hard score and a list of
failures; this guide is the diagnostic toolkit you use to read what Genie did, figure out why each
failure failed, and pick the smallest surface to tune. Pre-warm the warehouse first (the first query
on Free Edition is slow - run any prompt once and wait).

> **The prompts below are SEEDS, not a catalog.**
>
> The actual lab skill is to **make up your own questions**, verify the expected SQL yourself, watch
> Genie answer them, and when it misses, **tune the right surface**: Instructions text, certified
> Queries, SQL Expressions (Measures / Filters / Fields), Joins, function `COMMENT`s, or the Benchmark
> corpus. Pick the smallest surface that closes the gap and re-run.
>
> A few example workflows below will get you started. Then it's on you: invent prompts your audience
> would actually ask, run them, read the Thinking trace, find where Genie struggles, fix it, re-run.
> **That loop - generate → observe → tune → re-measure - is the implementation skill this lab exists
> to teach. Don't skip it.**

---

## How to see what an answer actually did

Every Genie answer can show its work. This is the most important habit in the whole lab.

1. **Expand "Thinking" / "Thinking complete".** Genie lists the steps it took. For a SQL answer you'll see
   the **generated SQL** - click to expand it. For an Agent answer you'll see the **function call** it made
   (e.g. `simulate_discount('SKU-1042', 15)`) and its arguments.
2. **Check it matched the right certified query.** If you wrote the prompt to match a certified query's
   **Question**, Genie usually reuses that query's SQL almost verbatim. If the SQL looks nothing like your
   certified query, that's a signal the Instructions/Question text didn't route it.
3. **Re-run the SQL yourself.** Genie shows the SQL it ran - paste it into a SQL editor or notebook cell
   against `bramblepeak_retail.sales` and confirm the numbers match. This is how you prove an answer.
4. **Watch which table it read.** For regional questions, confirm the SQL joined `dim_store` (not
   `dim_customer`) for region - the #1 thing to catch in this dataset.

> **Chat vs Agent mode** (toggle on the chat input): **Chat** = single-shot NL→SQL, fast, *cannot call
> functions*. **Agent** = multi-step, *can call UC functions*. Use Chat for the SQL prompts below and
> Agent for anything that should call `simulate_discount` / `top_drivers` / `explain_driver`.

---

# Core tests (do these)

The three rounds below cover the must-test surfaces: single-prompt SQL routing, function-call routing,
and multi-turn context. If only Rounds 1-3 pass, you have a working Genie space.

## Round 1 - single-prompt SQL checks (Chat mode)

| # | Prompt | What to expect | What to look for in "Thinking" |
|---|--------|----------------|--------------------------------|
| 1 | `What was our total revenue this month?` | One number, the company net_sales total for the current month. | SQL filters `dim_date.year_month` to the latest month (`MAX(year_month)`) and sums `net_sales` (not `unit_price`). |
| 2 | `Top 10 products by revenue this month` | Ranked table; **Aera Wireless Earbuds Pro** near the top. | Joins `dim_product`; `ORDER BY` revenue desc; `LIMIT 10`. |
| 3 | `Revenue by region this month` | 4 rows; **West** lowest. | Region comes from `dim_store.region`, not `dim_customer.region`. |
| 4 | `How did Online compare to Retail this month?` | 2 rows with revenue + margin %. | Groups by `dim_store.channel`. |
| 5 | `Revenue and margin by category this month` | 8 rows; **Outdoor & Camping** looks weak. | Joins `dim_product.category`; margin = `gross_margin`. |
| 6 | `Show me the monthly revenue trend` | 12 rows, one per month. | Groups by `dim_date.year_month`, ordered. |

If any answer is wrong: fix the **Instructions Text** first, then the relevant **Join instruction**, then
the **SQL Expression**, then the **certified Query**. Re-ask using the exact words from the matching
certified query's **Question** field - Genie matches intent against those.

---

## Round 2 - function-call checks (Agent mode)

Switch the chat to **Agent**. These must call UC functions, not write raw SQL.

| # | Prompt | What to expect | What to look for |
|---|--------|----------------|------------------|
| 7 | `What's driving the gap in the West this month?` | Ranked drivers; **Outdoor & Camping** at rank 1. | Thinking shows a call to `top_drivers('West')` - not a hand-written GROUP BY. |
| 8 | `Why is the top driver down?` (follow-up) | A one-sentence rationale. | Calls `explain_driver('West', 1)`. |
| 9 | `What if we discount the Summit 600 Down Jacket by 15%?` | Projected units up, revenue/margin, `margin_safe = true`. | Resolves the name to `SKU-1042`, then calls `simulate_discount('SKU-1042', 15)`. |
| 10 | `What about a 25% discount?` (follow-up) | Lower margin %, possibly `margin_safe = false`. | Calls `simulate_discount('SKU-1042', 25)` - re-uses the resolved product. |

If Genie answers #7-#10 with raw SQL instead of a function call, the fix is the function's UC `COMMENT`
in `notebooks/03_Setup_Functions.py` (strengthen the HOW TO USE / WHAT TO AVOID block), then re-run
notebook 03. Confirm the certified queries 7 and 8 from the setup guide exist - they give Genie a worked
example to pattern-match against.

---

## Round 3 - Multi-turn conversation test

Pick any 5-turn sequence - the one below is a representative example. Run all 5 prompts in the
**same Agent chat** (do not reset between turns) so Genie carries context forward. The point isn't this
specific sequence; it's confirming the space can hold a conversation across follow-ups, not just answer
one-shot queries.

| Turn | Prompt | Expected behavior |
|------|--------|-------------------|
| H1 | `How are sales pacing against our monthly target?` | Company total actual vs target + percent + `OFF TRACK`. Short summary - does NOT enumerate regions yet. |
| H2 | `Which regions are missing target this month, worst first?` | Per-region actual vs target, worst-first. **West** at the top with the biggest miss. |
| H3 | `What's driving the gap in the West?` | Calls `top_drivers('West')`. Rank 1 = Outdoor & Camping; product-level detractors below. |
| H4 | `What if we discount the Summit 600 Down Jacket by 15% to recover?` | Calls `simulate_discount('SKU-1042', 15)`. Projected unit lift, revenue, margin, `margin_safe = true`. |
| H5 | `What would a 25% discount do instead?` | Calls `simulate_discount('SKU-1042', 25)`. Bigger unit lift but thinner margin - contrast the two. |

**Signs it's working:** H2 references the West because H1 set the company frame; H3 knows "the West" from
H2; H4→H5 reuse the resolved product without you re-naming it. **If H5 asks you which product again**, the
chat lost context - refresh and re-run the sequence.

---

# Beyond the core (optional - pick what interests you)

The sections below are extras. You **do not** need to run them all. Cherry-pick anything that exercises
a corner of the schema you care about, or use them as starting points for prompts of your own. If
something fails, that's a tuning opportunity - find the smallest surface (Instructions, certified Query,
SQL Expression, Join, function `COMMENT`) that closes the gap and re-run.

## More example multi-turn workflows

A few extra chained conversations beyond Round 3. Each one runs in a single Agent chat (don't reset
between turns) and exercises a different question shape.

### Workflow A - Channel deep-dive (Online vs Retail)

| #  | Prompt                                                       | What to look for                                                                                              |
| -- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| A1 | `How did Online compare to Retail this month?`               | 2 rows; revenue + margin % per channel. Retail is the larger channel.                                         |
| A2 | `Which categories sell best Online?`                         | Carries "Online" from A1; groups by category filtered to Online. Electronics ranks high (earbuds surge).      |
| A3 | `Is Online's margin better or worse than Retail's?`          | Compares margin % across channels - no new dimension.                                                         |
| A4 | `Show me the Online revenue trend over the last 6 months.`   | Filters channel = Online, groups by `year_month`, last 6 months; current month steps up.                      |

### Workflow B - Cohort: new vs returning customers

Tests the customer-region trap explicitly (B4 asks where customers live, not where stores are).

| #  | Prompt                                                          | What to look for                                                                                                       |
| -- | --------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| B1 | `How many customers signed up this year?`                       | Counts `dim_customer` where `signup_date` falls in the current year.                                                   |
| B2 | `Of those, how many ordered this month?`                        | Intersects the cohort with current-month orders.                                                                       |
| B3 | `Compare new-customer AOV to everyone else's this month.`       | Two AOVs side by side.                                                                                                 |
| B4 | `Which region has the highest new-customer AOV?`                | Uses `dim_customer.region` (where customers live), **not** `dim_store.region`. Watch which Genie picks.                |

### Workflow C - Concentration & risk

How much of revenue rides on a handful of SKUs - and what a hypothetical loss looks like.

| #  | Prompt                                                                       | What to look for                                                                                          |
| -- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| C1 | `What share of this month's revenue comes from our top 5 products?`          | Top-5 revenue / total revenue.                                                                            |
| C2 | `If we lost the #1 product entirely, what would total revenue be?`           | Subtraction question - tests carrying a prior result as a filter. The Aera Earbuds are #1.                |
| C3 | `Of the top 5, which has the thinnest margin?`                               | Joins margin to the same top-5 list; carries the list across turns.                                       |

### Workflow D - Margin & discount health

Mixes plain SQL with a function call.

| #  | Prompt                                                                              | What to look for                                                                                |
| -- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| D1 | `What's our overall gross margin percent this month?`                               | Single % = SUM(gross_margin) / SUM(net_sales) * 100.                                            |
| D2 | `Rank categories by margin percent.`                                                | All 8 categories ranked; they cluster ~40-60%.                                                  |
| D3 | `How much revenue came from discounted orders this month?`                          | Uses the `is_discounted` filter (discount_amount > 0).                                          |
| D4 | `If we mark the Summit 600 Down Jacket down 20%, is the margin still safe?`         | Calls `simulate_discount('SKU-1042', 20)`; reports `projected_margin_pct` and `margin_safe`.    |

## More standalone prompts to spark your own

One-off questions, no chaining. Pick a few that interest you, run them, watch the Thinking trace, then
write your own variants.

- `Compare this month's revenue to last month's.`
- `Which quarter had the highest revenue?`
- `Do we sell more on weekends or weekdays?` *(uses `dim_date.is_weekend`)*
- `What's the average number of line items per order this month?` *(basket size)*
- `How many distinct customers ordered this month?`
- `Which brand generates the most revenue?`
- `What's the share of orders placed by VIPs this month?`
- `Which region is furthest above its target this month?` *(should surface the strongest region)*
- `Which subcategories within Apparel grew the most month over month?`
- `Which region has the most VIPs?` ⚠️ should use **customer** region; the prior region prompts use **store** region. Confirm Genie switches.
- `What happens to margin if we discount the Aera Wireless Earbuds Pro by 15%?` → `simulate_discount('SKU-1009', 15)`

> **The point of this section isn't to run all of them.** Pick whatever's interesting, then start writing
> your own. When one trips Genie up, that's the loop: read the Thinking trace, find the smallest surface
> to fix, re-run, measure. Promote the best of your prompts into certified queries and benchmark cases.

---

## What "good" looks like

- Every numeric answer is reproducible: you can expand the SQL, run it yourself, and get the same number.
- Regional answers join `dim_store` for region. Revenue answers sum `net_sales`. Margin answers use
  `gross_margin`.
- The three function prompts call functions (visible in Thinking), not improvised SQL.
- The 5-turn conversation in Round 3 holds context start to finish.

Once you've iterated until the benchmark in `02_Benchmarks.md` plateaus and the manual tests above
behave, **surface the space in a dashboard** via `04_Dashboard_And_Embedded_Genie.md`.
