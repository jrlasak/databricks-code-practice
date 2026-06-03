# Build a Genie Space: Natural-Language Analytics on a Retail Star Schema

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Connect with me:

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Training materials and resources
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

---

> ## 🛠 Read this first - it sets what this lab is
>
> **This lab is not about writing SQL or Python.** The data layer (catalog, star schema, ~120k rows of
> synthetic sales, the 3 Unity Catalog functions Genie calls) is fully built - **one notebook**
> (`00_Run_All.py`) provisions everything on Databricks serverless in ~3-6 min. After that,
> there is no more code to write.
>
> **The rest of the lab is hands-on configuration of a Databricks Genie space inside the Databricks UI.**
> That means **a lot of copy-paste** from the `genie-setup/` guides into the Genie **Configure** panel:
> Instructions text, joins, SQL expressions, certified queries, function registrations, common questions,
> a benchmark corpus, and an AI/BI dashboard. Plus reading SQL to verify Genie's answers.
>
> **The Genie space these guides walk you through is one example of how a production space could look - 
> not the final answer.** I **strongly encourage you to modify it, improve it, break it.** Come up with
> your own questions. When Genie misses one, find the smallest surface that fixes it (Instructions,
> certified Query, SQL Expression, Join, function `COMMENT`, or the Benchmark corpus) and tune it. Add
> new certified queries. Add new benchmark cases. Re-run. Measure. Iterate.
>
> **That tuning loop - generate questions → observe Genie → fix the smallest surface → re-measure - is
> the actual implementation skill this lab exists to teach.** Pasting in the starter config gets you to
> a baseline. The practice that makes you good at building Genie spaces in real life comes from doing
> the loop yourself.

---

Stand up a production-grade **Databricks Genie space** end to end: a clean retail sales **star schema**,
custom **Unity Catalog functions** Genie can call, a regression **benchmark** that scores its accuracy, and
an **AI/BI dashboard with embedded Genie**. You finish with the exact "ask your data in plain English"
capability companies are racing to ship in 2025-26 - and the judgment to make it accurate, not just demo-able.

The data tells a story on purpose. You play an analyst at **Aurora Outfitters**, a US omnichannel retailer.
This month the company missed its sales target, and you'll use Genie to find out why: the **West** region is
dragging, **Outdoor & Camping** collapsed there, and one product is surging online. By the end, Genie can
answer "are we on track? → why? → what's driving it? → what if we discount to recover?" as a connected
conversation.

## What you'll learn

- **Connect Genie to a star schema** and make it accurate with column comments, join paths, SQL
  expressions (measures / filters / fields), and certified queries.
- **Write a tight Genie system prompt (Instructions)** that delegates to the right surface instead of
  duplicating what those surfaces already encode.
- **Register pre-built Unity Catalog functions** so Genie's Agent mode calls governed tools (a pricing
  what-if, a gap-driver decomposition) instead of guessing SQL.
- **Benchmark Genie** with ground-truth SQL and LLM-judge evaluation notes to get a repeatable accuracy %
  and a tuning loop.
- **Read Genie's reasoning** - inspect which query or function produced each answer, and reproduce it.
- **Embed Genie in an AI/BI dashboard** and understand the embed's capability limits (it can't call
  functions - and why that matters).
- **Practice the tuning loop yourself** - generate questions → watch Genie → fix the smallest surface →
  re-measure. This is the skill the lab is built around.

## Architecture

A clean Kimball-style **star schema** in Unity Catalog (`aurora_retail.sales`):

```
                 dim_date
                    |
 dim_customer --- fact_sales --- dim_product
                    |
                 dim_store          + sales_targets (region x month)
                                    + performance_insights (precomputed gap drivers)
```

- `fact_sales` - ~120k order lines (12 months). Measures: `net_sales`, `gross_margin`, `quantity`.
- 4 conformed dimensions joined by surrogate keys (`*_key`), clean `snake_case` columns (no backticks).
- `sales_targets` powers "on track vs target"; `performance_insights` is filled by a workflow notebook and
  read by the UC functions.

On top of the data: a **Genie space** (Instructions + joins + expressions + 8 certified queries + 3 UC
functions), a **benchmark**, and a **2-page AI/BI dashboard** with the Genie space embedded. All of it runs
on **Databricks Free Edition** (serverless SQL, UC functions, Genie, and AI/BI dashboards are all included).

## Prerequisites

- Comfort **reading** SQL (you won't be writing any) and basic star-schema concepts (facts, dimensions, joins).
- A Databricks **Free Edition** account (see below). No prior Genie experience needed.
- ~2-3 hours: ~10 min for the data-setup notebook, the rest is Genie configuration + tuning in the UI.

> ⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace.
> Go to **User Settings → Developer → AI-powered code completion** and turn **OFF**:
> - **Autocomplete as you type**
> - **Automatic Assistant Autocomplete**
>
> This lab is about *configuring and reasoning about Genie*, not auto-generated code - turn the helpers off.

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you
     don't already have one.
   - Familiarize yourself with the workspace, notebooks, and the AI/BI section in the sidebar.

2. **Import this repository to Databricks**
   - In Databricks, go to the Workspace sidebar → **Repos** → **Add Repo** (or your personal folder →
     **Create → Git folder**).
   - Paste the GitHub URL for this repository, authenticate if prompted, and select the main branch.
   - The repo appears as a folder you can edit and run from directly.
   - See [Repos in Databricks](https://docs.databricks.com/repos/index.html) for details.

3. **Provision the data layer** - open `00_Run_All.py` and **Run all**. It runs notebooks 01→04
   on serverless (~3-6 min) and creates catalog `aurora_retail`, schema `sales`, 7 tables, and 3 functions.
   The output prints a storyline check (West off track) and final row counts. Re-running is safe.

4. **Build the Genie space** - follow `genie-setup/01_Genie_Space_Setup.md` step by step in the Databricks
   UI (sidebar → **Genie Spaces**). This is a click-through build; the guide gives you every value to paste.

5. **Benchmark it** - set up the regression benchmark in `genie-setup/02_Benchmarks.md`, run it in Agent
   mode, and read the per-question scores. This gives you a hard accuracy number out of the gate.

6. **Test & iterate** - work through `genie-setup/03_Test_Workflows.md` to learn how to read Genie's
   Thinking trace, run a multi-turn conversation, and diagnose whatever the benchmark surfaced. Tune
   the smallest surface that closes each gap (Instructions / Joins / SQL Expressions / certified Queries
   / function `COMMENT`s) and re-run the benchmark. **This loop is the lab.**

7. **Surface it** - build the AI/BI dashboard with embedded Genie in
   `genie-setup/04_Dashboard_And_Embedded_Genie.md`.

## Repo layout

```
genie-sales-analytics/
├── README.md                          <- you are here
├── 00_Run_All.py                      <- one-click: runs every notebook in order (start here)
├── notebooks/
│   ├── 01_Setup_Tables.py             <- catalog, schema, star schema (with UC comments)
│   ├── 02_Generate_Data.py            <- deterministic synthetic sales + baked-in storyline
│   ├── 03_Setup_Functions.py          <- 3 Unity Catalog functions Genie calls
│   ├── 04_Compute_Insights.py         <- "daily job": fills performance_insights
│   └── variables.py                   <- shared config (catalog/schema/anchors)
└── genie-setup/
    ├── 01_Genie_Space_Setup.md        <- build the Genie space (the main runbook)
    ├── 02_Benchmarks.md               <- regression benchmark + ground-truth SQL
    ├── 03_Test_Workflows.md           <- validation prompts + diagnostic toolkit
    └── 04_Dashboard_And_Embedded_Genie.md  <- AI/BI dashboard + embed the space
```

## The transformation

Before: you know Genie exists and can demo a canned question. After: you can connect Genie to a real model,
make it accurate with instructions/joins/expressions/functions, prove its accuracy with a benchmark, read
exactly how it answered, and embed it in a dashboard - and you know the one capability the embed quietly
drops. That's the difference between "we tried the AI thing" and "we shipped governed natural-language
analytics."
