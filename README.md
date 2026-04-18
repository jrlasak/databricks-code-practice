# Databricks Code Practice

### Get fluent in Databricks by typing, not watching.

**104 exercises + 5 production-grade pipeline labs. All on Databricks Free Edition.**

> **New (18 April 2026):** 5 full-scale pipeline labs + 1 benchmark deep-dive just landed. If you starred this repo for the exercises, they're still here - now alongside end-to-end project work.

---

Databricks tutorials are everywhere. Tutorials that actually make you fluent are not. Watching someone else write a MERGE doesn't mean you can write one under interview pressure. Reading about Auto Loader doesn't mean you can debug the broken job you inherit on day 1 of a new team.

Fluency comes from reps, not reading. This repo is the reps: code you type, assertions that fail loudly until you get it right, pipelines you ship end to end. Three structured paths, all on Databricks Free Edition:

- **`exercises/`** when you want focused reps on a single concept. LeetCode-style, 5-30 min each - MERGE, window functions, Auto Loader, time travel, liquid clustering. Write code, run assertions, pass or iterate.
- **`pipeline-labs/`** when you want to build something end-to-end. Multi-notebook medallion pipelines on a realistic business scenario (streaming retail, fintech fraud, cert-aligned). 2-3 hours each, same structure you'll ship in production.
- **`deep-dives/`** when you want to measure the impact of a technique with numbers. Apply one lever at a time, benchmark the delta, prove what it actually buys you.

Walk away able to answer the Delta Lake question in your next interview, debug the Auto Loader job you inherit on day 1 of a new team, and point at production-grade code you've written yourself - not watched someone else write.

104 exercises across 13 notebooks, plus 5 full project labs. No cloud account, no cluster config. Free.

## Author

**Jakub Lasak** - Databricks Data Engineer. Helping you interview like seniors, execute like seniors, and think like seniors.

- [LinkedIn](https://www.linkedin.com/in/jrlasak/) (13.5K followers) - Databricks projects and tips
- [Substack](https://dataengineer.wiki/substack) - Newsletter for data engineers
- [DataEngineer.wiki](https://dataengineer.wiki) - Cheat sheets, learning paths, cert guides

> **Prepping for interviews?** Writing code is one half of the battle - knowing the questions that actually come up is the other. I maintain [Databricks Interview Cheat Sheets](https://dataengineer.wiki/products) by seniority level (junior / mid / senior / bundle).

## What's Inside

|               | Exercises                                  | Pipeline Labs                                                    | Deep-Dives                                              |
| ------------- | ------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------------- |
| **Format**    | Single notebook, one TODO per exercise     | Multi-notebook guided project                                    | Single-topic deep investigation                         |
| **Time**      | 5-30 min per exercise                      | 2-3 hours per lab                                                | 1-2 hours                                               |
| **Scope**     | One concept (MERGE, window functions, ...) | End-to-end project (ingestion -> bronze -> silver -> gold)       | One topic measured in depth                             |
| **Narrative** | None. "Given table X, write..."            | Business scenario. "You're building a streaming pipeline for..." | Benchmark-driven. "Apply technique, measure the delta." |
| **Order**     | Pick any, skip around                      | Sequential notebooks that build on each other                    | Sequential; each step layers on the last                |
| **Goal**      | Drill a skill until it's automatic         | See how concepts fit in a real project                           | Prove what a technique actually buys you                |

## Catalog

### Exercises (`exercises/`)

<!-- TOPICS-START -->

| Topic                               | Notebooks | Exercises | Description                                                                                                                          |
| ----------------------------------- | --------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| [Delta Lake](exercises/delta-lake/) | 6         | 51        | MERGE operations, time travel, schema enforcement, OPTIMIZE, liquid clustering, change data feed                                     |
| [ELT](exercises/elt/)               | 7         | 53        | Spark SQL joins, window functions, PySpark transformations, Auto Loader, batch ingestion, medallion architecture, complex data types |

**Total: 13 notebooks, 104 exercises**

<!-- TOPICS-END -->

More exercise topics coming - next up: Streaming, Unity Catalog, Performance, and DLT.

### Pipeline Labs (`pipeline-labs/`)

Multi-notebook, end-to-end medallion pipelines with a business scenario. Each runs 2-3 hours and ships with a synthetic data generator.

| Lab                                                                      | What You Build                                                                                       | Focus                                                                                         |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [Apparel Retail 360 (DLT)](pipeline-labs/apparel-streaming/)             | End-to-end retail analytics pipeline on Delta Live Tables with a full medallion architecture.        | DLT, Medallion, SCD Type 2, Streaming, Data Quality Expectations                              |
| [Fintech Transaction Monitoring](pipeline-labs/fintech-monitoring/)      | Real-time fraud-monitoring pipeline for a payment processor handling 500K+ transactions/day.         | Structured Streaming, Rescued Data, Watermarked Dedup, Stream-Static Joins, Liquid Clustering |
| [DE Associate Certification Prep](pipeline-labs/de-associate-cert-prep/) | Production-grade pipeline covering every exam domain of the Databricks Data Engineer Associate cert. | Auto Loader, COPY INTO, Medallion, SCD2, Jobs, Unity Catalog                                  |
| [PySpark Developer Cert Prep](pipeline-labs/pyspark-cert-zenith/)        | E-commerce analytics pipeline covering every domain of the Spark Developer Associate cert.           | DataFrame API, Structured Streaming, Data Skew, Performance Tuning                            |

### Deep-Dives (`deep-dives/`)

Single-topic labs that measure the impact of a technique with numbers, not intuition.

| Lab                                                                    | What You Build                                                                              | Focus                                                                     |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [6 Delta Optimization Techniques](deep-dives/optimization-techniques/) | Iteratively apply and measure core Delta performance levers on a synthetic 50M-row dataset. | Partitioning, Z-Order, OPTIMIZE, Auto Optimize, Liquid Clustering, VACUUM |

## How to Use

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (free, no credit card)
2. Clone or import this repo into Databricks (Workspace -> Create -> Git folder)
3. Navigate to the folder you want, open its README, follow the instructions

Everything runs on Free Edition: serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.

## Which Should I Start With?

- **New to Databricks?** Start with [DE Associate Cert Prep](pipeline-labs/de-associate-cert-prep/) - broadest fundamentals.
- **Want quick reps on a specific concept?** [Delta Lake exercises](exercises/delta-lake/) or [ELT exercises](exercises/elt/) - drill one concept at a time.
- **Comfortable with batch, new to streaming?** [Apparel DLT](pipeline-labs/apparel-streaming/), then [Fintech Monitoring](pipeline-labs/fintech-monitoring/).
- **Preparing for a cert?** [DE Associate](pipeline-labs/de-associate-cert-prep/) or [Spark Developer Associate](pipeline-labs/pyspark-cert-zenith/).
- **Already shipping pipelines, want to go deeper on performance?** [Delta Optimization Techniques](deep-dives/optimization-techniques/).

## Stay in the Loop

New exercises and labs ship regularly. Follow on [LinkedIn](https://www.linkedin.com/in/jrlasak/) or subscribe to the [Substack newsletter](https://dataengineer.wiki/substack) to be notified when new content drops.

## Feedback

Found a bug? Have a suggestion? [Open an issue](../../issues).

---

> **Disclaimer**: This is an independent educational resource created by Jakub Lasak. Not affiliated with, endorsed by, or sponsored by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.
