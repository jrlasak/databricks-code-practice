# Databricks Code Practice

### Get fluent in Databricks by typing, not watching.

**149 exercises, 4 production-grade pipeline labs, and 2 deep-dives. All on Databricks Free Edition.**

Clone once, import into Databricks, pick a folder. Exercises fail loud until your code is right; labs ship with synthetic data so you build production-style pipelines, not toy ones.

---

## Author

**Jakub Lasak** - Databricks Data Engineer. Helping you interview like seniors, execute like seniors, and think like seniors.

- [LinkedIn](https://www.linkedin.com/in/jrlasak/) (15.5K followers) - Databricks projects and tips
- [Substack](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Newsletter for data engineers, 4.8K subscribers
- [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- [DataEngineer.wiki](https://dataengineer.wiki?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Cheat sheets, learning paths, cert guides

> **Prepping for interviews?** Writing code is one half of the battle - knowing the questions that actually come up is the other. I maintain [Databricks Interview Cheat Sheets](https://dataengineer.wiki/products?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) by seniority level (junior / mid / senior / bundle).

## What's Inside

Fluency comes from reps, not reading. Three structured paths:

- **`exercises/`** - focused reps on a single concept. LeetCode-style, 5-30 min each.
- **`pipeline-labs/`** - end-to-end medallion pipelines on a business scenario. 2-3 hours each.
- **`deep-dives/`** - go deep on a single topic, hands-on, end to end. 1-3 hours each.

|               | Exercises                                  | Pipeline Labs                                                    | Deep-Dives                                              |
| ------------- | ------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------------- |
| **Format**    | Single notebook, one TODO per exercise     | Multi-notebook guided project                                    | Single-topic deep investigation                         |
| **Time**      | 5-30 min per exercise                      | 2-3 hours per lab                                                | 1-3 hours                                               |
| **Scope**     | One concept (MERGE, window functions, ...) | End-to-end project (ingestion -> bronze -> silver -> gold)       | One topic, hands-on in depth                            |
| **Narrative** | None. "Given table X, write..."            | Business scenario. "You're building a streaming pipeline for..." | Focused. "Go deep on one topic, end to end."            |
| **Order**     | Pick any, skip around                      | Sequential notebooks that build on each other                    | Sequential; each step layers on the last                |
| **Goal**      | Drill a skill until it's automatic         | See how concepts fit in a real project                           | Build real, hands-on command of one topic               |

## Catalog

### Exercises (`exercises/`)

<!-- TOPICS-START -->

| Topic                               | Notebooks | Exercises | Description                                                                                                                          |
| ----------------------------------- | --------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| [Delta Lake](exercises/delta-lake/) | 6         | 51        | MERGE operations, time travel, schema enforcement, OPTIMIZE, liquid clustering, change data feed                                     |
| [ELT](exercises/elt/)               | 7         | 53        | Spark SQL joins, window functions, PySpark transformations, Auto Loader, batch ingestion, medallion architecture, complex data types |
| [Streaming](exercises/streaming/)   | 6         | 45        | Structured Streaming basics, windowed aggregations & watermarks, stream-static joins, stream-stream joins, foreachBatch patterns, checkpointing & recovery |

**Total: 19 notebooks, 149 exercises**

<!-- TOPICS-END -->

More exercise topics coming - next up: Unity Catalog, Performance, and DLT.

### Pipeline Labs (`pipeline-labs/`)

Multi-notebook, end-to-end medallion pipelines with a business scenario. Each runs 2-3 hours and ships with a synthetic data generator.

| Lab                                                                      | What You Build                                                                                       | Focus                                                                                         |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| [Apparel Retail 360 (Lakeflow)](pipeline-labs/apparel-streaming/) | End-to-end retail analytics pipeline on Lakeflow Spark Declarative Pipelines with a full medallion architecture. | Lakeflow Declarative Pipelines, Medallion, SCD Type 2, Streaming, Data Quality Expectations |
| [Fintech Transaction Monitoring](pipeline-labs/fintech-monitoring/)      | Real-time fraud-monitoring pipeline for a payment processor handling 500K+ transactions/day.         | Structured Streaming, Rescued Data, Watermarked Dedup, Stream-Static Joins, Liquid Clustering |
| [DE Associate Certification Prep](pipeline-labs/de-associate-cert-prep/) | Production-grade pipeline drilling the core topics of the Databricks Data Engineer Associate cert. | Auto Loader, COPY INTO, Medallion, SCD2, Jobs, Unity Catalog                                  |
| [PySpark Developer Cert Prep](pipeline-labs/pyspark-cert-zenith/)        | E-commerce analytics pipeline covering six of the seven Spark Developer Associate exam sections.           | DataFrame API, Structured Streaming, Data Skew, Performance Tuning                            |

### Deep-Dives (`deep-dives/`)

Single-topic labs that go deep on one technique or capability - hands-on, end to end.

| Lab                                                                    | What You Build                                                                              | Focus                                                                     |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [6 Delta Optimization Techniques](deep-dives/optimization-techniques/) | Iteratively apply and measure core Delta performance levers on a synthetic 50M-row dataset. | Partitioning, Z-Order, OPTIMIZE, Auto Optimize, Liquid Clustering, VACUUM |
| [Build a Genie Space](deep-dives/genie-sales-analytics/) | Stand up and tune a production Databricks Genie space on a retail star schema, then benchmark its natural-language answer accuracy against ground-truth SQL. | Genie, Unity Catalog Functions, Star Schema, Benchmarking, AI/BI Dashboards |

## How to Use

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (free, no credit card)
2. Clone or import this repo into Databricks (Workspace -> Create -> Git folder)
3. Navigate to the folder you want, open its README, follow the instructions

Everything runs on Free Edition: serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.

## Which Should I Start With?

- **New to Databricks?** Start with [DE Associate Cert Prep](pipeline-labs/de-associate-cert-prep/) - broadest fundamentals.
- **Want quick reps on a specific concept?** [Delta Lake exercises](exercises/delta-lake/), [ELT exercises](exercises/elt/), or [Streaming exercises](exercises/streaming/) - drill one concept at a time.
- **Comfortable with batch, new to streaming?** Start with [Streaming exercises](exercises/streaming/) for atomic concept drills, then [Apparel Lakeflow](pipeline-labs/apparel-streaming/) and [Fintech Monitoring](pipeline-labs/fintech-monitoring/) for end-to-end pipelines.
- **Preparing for a cert?** [DE Associate](pipeline-labs/de-associate-cert-prep/) or [Spark Developer Associate](pipeline-labs/pyspark-cert-zenith/).
- **Already shipping pipelines, want to go deeper on performance?** [Delta Optimization Techniques](deep-dives/optimization-techniques/).
- **Want to ship natural-language analytics (Genie / AI/BI)?** [Build a Genie Space](deep-dives/genie-sales-analytics/) - connect Genie to a star schema, benchmark its accuracy, embed it in a dashboard.

## Stay in the Loop

New exercises and labs ship regularly. Follow on [LinkedIn](https://www.linkedin.com/in/jrlasak/) or subscribe to the [Substack newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) to be notified when new content drops.

## Changelog

- **3 June 2026** - New deep-dive: [Build a Genie Space](deep-dives/genie-sales-analytics/) - natural-language analytics on a retail star schema, with a regression benchmark and an embedded AI/BI dashboard.
- **2 June 2026** - New exercise topic: [Streaming](exercises/streaming/) - 45 LeetCode-style reps across 6 notebooks (Structured Streaming, watermarks, stream-static & stream-stream joins, foreachBatch, checkpointing).
- **18 April 2026** - Consolidated into one repo: 4 pipeline labs and the first deep-dive (Delta optimization) joined the original Delta Lake + ELT exercises.
- **7 April 2026** - Initial release: Delta Lake + ELT exercises.

## Feedback

Found an exercise that can't be solved as written? That's the most useful thing you can report - [open an issue](../../issues/new/choose). Suggestions for new topics welcome too.

Finished an exercise and want to keep your work? Fork the repo - your solutions stay in your fork. Please don't open a PR with solved TODO cells; it deletes the exercise for everyone else. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[CC BY-SA 4.0](LICENSE). In plain terms:

- **Use it, fork it, adapt it, teach from it** - at work, in a study group, on your blog, in a bootcamp, in paid training. Commercial use is fine.
- **Credit it clearly.** Name this repo and link back, visibly enough that your audience knows where the exercises came from. Passing them off as your own material is the one thing that isn't allowed.
- **Share adaptations under the same terms.** If you modify the exercises, your version carries this license too - so nobody can take a fork private and lock it down.

Using this in paid training or a course? Go ahead - just keep the attribution obvious. I'd genuinely like to hear about it: [say hi](https://www.linkedin.com/in/jrlasak/).

---

> **Disclaimer**: This is an independent educational resource created by Jakub Lasak. Not affiliated with, endorsed by, or sponsored by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.
