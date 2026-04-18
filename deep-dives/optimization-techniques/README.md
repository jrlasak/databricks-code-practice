# 6 Delta Lake Optimization Techniques: A Hands‑On Learning Project

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** — Helping you interview like seniors, execute like seniors, and think like seniors.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack) - Exclusive content for Data Engineers
- 🌐 [DataEngineer.wiki](http://dataengineer.wiki/) - Training materials and resources
- 🚀 [More Practice Labs](https://dataengineer.wiki/projects) - Delta Live Tables, table optimization, and more

## 1. Purpose & Overview

Modern lakehouse performance hinges on _layout_ and _file hygiene_. This project is a guided lab that lets you iteratively apply and observe core Delta Lake optimization levers:

- Physical partitioning
- Z-Ordering
- Manual compaction (OPTIMIZE)
- Auto Optimize (optimizeWrites + autoCompact)
- Liquid Clustering
- VACUUM lifecycle hygiene

You will generate a synthetic 50M‑row sales dataset, capture baseline query metrics, then layer techniques - measuring their impact (files scanned, data read, scan time) via the Spark UI and table metadata.

All instructions and code live in the notebook: **[`project.ipynb`](project.ipynb)**. Open it first; proceed cell by cell.

## 2. Learning Objectives

By completing the lab you will be able to:

- Choose between partitioning, Z-Ordering, and Liquid Clustering based on data shape & query patterns
- Diagnose small-file and data-skipping issues using `DESCRIBE DETAIL` + Spark UI
- Apply manual compaction and contrast with auto compaction
- Understand retention safety around VACUUM
- Build a lightweight empirical metrics log to justify optimization choices

## 3. What You Will Build

A sequence of Delta tables representing successive optimization strategies:
| Logical Role | Table Key (see registry) | Technique Illustrated |
|--------------|--------------------------|-----------------------|
| Baseline raw | `sales_raw` | Many small files, unoptimized |
| Country-partitioned | `sales_partitioned` | Low-cardinality partitioning |
| Z-Ordered copy | `sales_raw_zorder` | Multi-column data skipping |
| Fragmented (pre-compaction) | `sales_to_compact` | Small file proliferation |
| Auto Optimize enabled | `sales_auto_compact` | Automatic write sizing + async compaction |
| Liquid Clustered | `sales_liquid_clustered` | Adaptive clustering |

A single helper registry centralizes fully qualified names for reproducibility.

## 4. Architecture & Runtime Assumptions

- Databricks (Community / Free or higher tier). Some VACUUM retention behaviors differ on Free Edition (cannot disable retention safety).
- Spark SQL + PySpark (no external data sources required)
- Delta Lake tables stored in a user-created catalog & schema (created automatically if permitted)

## 5. Prerequisites

- Basic Spark & Delta Lake familiarity (DataFrames, SQL, catalog objects)
- Comfort reading Spark UI (scan details, tasks, input size)
- Python (for minor helper code)

## 6. How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace, clusters, and notebook interface.

2. **Import this repository to Databricks**
   - In Databricks, go to the Workspace sidebar and click the "Repos" section, click "Add Repo".
     - Alternatively, go to your personal folder, click "create" and select "git folder".
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Repos in Databricks](https://docs.databricks.com/repos/index.html).

3. Open `project.ipynb`
4. Execute cells sequentially - pick the serverless cluster. The notebook is idempotent - data generation skips if the base table already exists.
5. After each optimization action, open the Spark UI (SQL / DataFrame tab) and record metrics.

## 7. Extending the Lab

Try adding:

1. Date-based partition layer vs country: compare scan metrics.
2. Programmatic metrics capture notebook that stores results into a Delta table for plotting trends.
3. Incremental data growth simulation + periodic Z-Order refresh policy.
4. Photon vs non-Photon runtime comparison (CPU cost vs performance).
5. Streaming ingestion (Auto Loader) to stress clustering adaptiveness.

## 8. Feedback Wanted

I want this to be maximally useful for learners. After running the notebook, please consider opening a Discussion or Issue with:

1. Were the notebook instructions clear at each step? Where did you pause or re-read?
2. Which optimization concept remained fuzzy, and what supporting visual or explanation would help?
3. Would a short video walkthrough add value, or do you prefer self-discovery?

Happy to credit you as a contributor if you provide actionable feedback.

## 9. Troubleshooting Quick Tips

| Symptom                                 | Possible Cause                       | Action                                                                      |
| --------------------------------------- | ------------------------------------ | --------------------------------------------------------------------------- |
| Baseline table regenerates unexpectedly | Catalog/schema context lost          | Re-run config cell to `USE CATALOG` + `USE SCHEMA`                          |
| Z-Order command not found               | Wrong runtime / missing Delta extras | Ensure cluster has Delta support (DBR 11+ recommended)                      |
| VACUUM removed no files                 | Retention window not elapsed         | Check history timestamps; wait or (demo only) lower retention (not on Free) |
| Minimal scan improvement after Z-Order  | Predicate low selectivity            | Test narrower filters; ensure chosen columns are in WHERE                   |

## 10. Cleanup

Run the final cleanup cell (commented by default) to drop the entire catalog if you want to fully reset.

```sql
-- Optional
-- DROP CATALOG IF EXISTS delta_optimization_project CASCADE;
```

---

Open `project.ipynb` now and start with the configuration cell. Record metrics; experimentation beats theory. Enjoy!
