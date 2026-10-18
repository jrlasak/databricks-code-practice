# Build a Genie Agent: Natural-Language Analytics on a Retail Star Schema

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Connect with me:

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🎧 [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Interview prep and free Databricks practice
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

---

> ## 🛠 Read this first - it sets what this lab is
>
> **This lab is not about writing SQL or Python.** The data layer (catalog, star schema, ~115k rows of synthetic sales, the 3 Unity Catalog functions Genie calls) is fully built - **one notebook** (`00_Run_All.py`) provisions everything on Databricks serverless in ~3-6 min. After that, there is no more code to write.
>
> **The rest of the lab is hands-on configuration of a Databricks Genie Agent (formerly called a Genie space) inside the Databricks UI.** That means **a lot of copy-paste** from the `genie-setup/` guides into the Genie **Configure** panel: general instructions, joins, SQL expressions, example queries, the 3 functions, common questions, a benchmark corpus, and an AI/BI dashboard. Plus reading SQL to verify Genie's answers.
>
> **The Genie Agent these guides walk you through is one example of how a production agent could look, not the final answer.** I **strongly encourage you to modify it, improve it, break it.** Come up with your own questions. When Genie misses one, find the smallest surface that fixes it (general instructions, an example query, a SQL expression, a join, a function `COMMENT`, or the benchmark corpus) and tune it. Add new example queries. Add new benchmark cases. Re-run. Measure. Iterate.
>
> **That tuning loop - generate questions → observe Genie → fix the smallest surface → re-measure - is the skill that makes a Genie Agent accurate in production.** Pasting in the starter config gets you to a baseline. The practice that makes you good at building Genie Agents in real life comes from doing the loop yourself.

---

Stand up a production-grade **Databricks Genie Agent** end to end: a clean retail sales **star schema**, custom **Unity Catalog functions** Genie can call, a regression **benchmark** that scores its accuracy, and an **AI/BI dashboard with embedded Genie**. You finish with the exact "ask your data in plain English" capability companies are racing to ship, and the judgment to make it accurate, not just impressive on the first question.

The data tells a story on purpose. You play an analyst at **Bramblepeak Outfitters**, a US omnichannel retailer. This month the company missed its sales target, and you'll use Genie to find out why: the **West** region is dragging, **Outdoor & Camping** collapsed there, and one product is surging online. By the end, Genie can answer "are we on track? → why? → what's driving it? → what would a markdown on our healthiest seller do to margin?" as a connected conversation.

## What you'll learn

- **Connect Genie to a star schema** and make it accurate with column comments, join paths, SQL expressions (measures / filters / fields), and example SQL queries.
- **Write tight general instructions (the Genie system prompt)** that delegate to the right surface instead of duplicating what those surfaces already encode.
- **Attach pre-built Unity Catalog functions** so Genie's Agent mode calls governed tools (a pricing what-if, a month-over-month driver decomposition) instead of guessing SQL.
- **Benchmark Genie** with ground-truth SQL and LLM-judge evaluation notes to get a repeatable accuracy % and a tuning loop.
- **Read Genie's reasoning** - inspect which query or function produced each answer, and reproduce it.
- **Embed Genie in an AI/BI dashboard** and test what the dashboard's **Ask Genie** chat can and cannot do compared with the full agent (function calls, filter context).
- **Practice the tuning loop yourself** - generate questions → watch Genie → fix the smallest surface → re-measure. This is the skill the lab is built around.

## Architecture

A clean Kimball-style **star schema** in Unity Catalog (`bramblepeak_retail.sales`):

```
                 dim_date
                    |
 dim_customer --- fact_sales --- dim_product
                    |
                 dim_store          + sales_targets (region x month)
                                    + performance_insights (precomputed month-over-month drivers)
```

- `fact_sales` - ~115k order lines (12 months). Measures: `net_sales`, `gross_margin`, `quantity`.
- 4 conformed dimensions joined by surrogate keys (`*_key`), clean `snake_case` columns (no backticks).
- `sales_targets` powers "on track vs target"; `performance_insights` is filled by `notebooks/04_Compute_Insights.py` (the "daily job") and read by the UC functions.

On top of the data: a **Genie Agent** (general instructions + joins + expressions + 8 example SQL queries + 3 UC functions), a **benchmark**, and a **2-page AI/BI dashboard** with the Genie Agent embedded. All of it runs on **Databricks Free Edition** (serverless SQL, UC functions, Genie, and AI/BI dashboards are all included).

## Prerequisites

- Comfort **reading** SQL (you won't be writing any) and basic star-schema concepts (facts, dimensions, joins).
- A Databricks **Free Edition** account (see below). No prior Genie experience needed.
- ~4.5-5 hours: ~10 min for the data layer (`00_Run_All` runs in ~3-6 min, then you read its storyline check), ~1.5 h to build the Genie Agent (01), ~1.5 h to benchmark and tune it (02), ~45 min of core tests and a first tuning pass (03), ~45 min for the dashboard (04). Each guide fits one sitting.

> ⚠️ **Before you start**: Disable AI code suggestions in your Databricks workspace. Go to **Settings → Developer → Code Editor** and turn **OFF**:
> - **Autocomplete as you type**
> - **Automatic Genie Code Autocomplete**
>
> Autocomplete fills in SQL you should be reading and checking yourself. You'll still use Genie Code on purpose in `04_Dashboard_And_Embedded_Genie.md` to generate the dashboard widgets.

## How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace, notebooks, and the AI/BI section in the sidebar.

2. **Import this repository to Databricks**
   - In Databricks, go to **Workspace** → your home folder → **Create → Git folder**.
   - Paste the GitHub URL for this repository, authenticate if prompted, and select the main branch.
   - The repo appears as a folder you can edit and run from directly.
   - See [Git folders](https://docs.databricks.com/aws/en/repos/) for details.

3. **Provision the data layer** - open `00_Run_All.py` and **Run all**. It runs notebooks 01→04 on serverless (~3-6 min) and creates catalog `bramblepeak_retail`, schema `sales`, 7 tables, and 3 functions. The output prints a storyline check (West off track) and final row counts. Keep this output open: the Genie guides check answers against its actual-vs-target numbers. Re-running is safe.

4. **Build the Genie Agent** - follow `genie-setup/01_Genie_Agent_Setup.md` step by step in the Databricks UI (sidebar → **Genie Agents**). This is a click-through build; the guide gives you every value to paste.

5. **Benchmark it** - set up the regression benchmark in `genie-setup/02_Benchmarks.md`, run it in Agent mode, and read the per-question ratings. This gives you a hard accuracy number out of the gate.

6. **Test & iterate** - work through `genie-setup/03_Test_Workflows.md` to learn how to read Genie's Thinking trace, run a multi-turn conversation, and diagnose whatever the benchmark surfaced. Tune the smallest surface that closes each gap (general instructions / joins / SQL expressions / example queries / function `COMMENT`s) and re-run the benchmark. **This loop is the lab.**

7. **Surface it** - build the AI/BI dashboard with embedded Genie in `genie-setup/04_Dashboard_And_Embedded_Genie.md`.

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
    ├── 01_Genie_Agent_Setup.md        <- build the Genie Agent (the main runbook)
    ├── 02_Benchmarks.md               <- regression benchmark + ground-truth SQL
    ├── 03_Test_Workflows.md           <- validation prompts + diagnostic toolkit
    └── 04_Dashboard_And_Embedded_Genie.md  <- AI/BI dashboard + embed the agent
```

## The transformation

Before: you know Genie exists and can run a canned question. After: you can connect Genie to a real model, make it accurate with instructions/joins/expressions/functions, prove its accuracy with a benchmark, read exactly how it answered, and embed it in a dashboard - and you know how to test what the dashboard chat can do before you promise it to a stakeholder. That's the difference between "we tried the AI thing" and "we shipped governed natural-language analytics."
