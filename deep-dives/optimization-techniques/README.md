# 6 Delta Lake Optimization Techniques: A Hands-On Learning Project

> Independent educational resource; not endorsed by Databricks, Inc. "Databricks" and "Delta Lake" are trademarks of their respective owners.

## Author

<img src="https://dataengineer.wiki/download/profilepicture.jpg?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice" alt="Jakub Lasak" width="80" style="border-radius: 50%;" />

**Jakub Lasak** - Databricks Data Engineer (ex-Uber). I write about the Databricks work that gets you hired and promoted.

- 🔗 [LinkedIn](https://www.linkedin.com/in/jrlasak/) - Databricks projects and tips
- 📬 [Substack Newsletter](https://dataengineer.wiki/substack?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Exclusive content for Data Engineers
- 🎧 [Podcast](https://dataengineer.wiki/podcast?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Databricks and data engineering, on Spotify, Apple, and YouTube
- 🌐 [DataEngineer.wiki](https://dataengineer.wiki/?utm_source=github&utm_medium=readme&utm_campaign=databricks-code-practice) - Interview prep and free Databricks practice
- 🚀 [More Practice Labs](https://github.com/jrlasak/databricks-code-practice) - more pipeline labs, deep-dives, and exercises in this repo

## 1. Purpose & Overview

Modern lakehouse performance hinges on _layout_ and _file hygiene_. This is a read-and-run deep-dive: there are no exercises; you run every cell and read the evidence it produces. It applies six Delta Lake optimization levers to the same 10M-row sales table and measures each one, three per notebook.

In `01_Data_Layout`:

- Physical partitioning
- Z-Ordering
- Liquid Clustering

In `02_File_Maintenance`:

- Manual compaction (OPTIMIZE)
- Auto Optimize (`optimizeWrite` + `autoCompact`)
- VACUUM lifecycle hygiene

You own `sales`, the order-line fact table of a wholesale distributor: 10M lines from 4,000 business accounts in 10 countries over two years. The account-support app looks up one account's lines for one product, and on the table as loaded, that lookup reads all of the table's roughly 2,000 files. Each technique above is a candidate fix, and the measurements tell you which one is worth its maintenance cost.

You will generate a synthetic 10M-row sales dataset, capture baseline query metrics, then apply each technique to its own copy of the table and measure its impact (files read, bytes read, duration) in the query profile and with `DESCRIBE DETAIL`. In `01_Data_Layout` every layout is measured on two lookups: one account's lines for one product with the account's country, and the same lookup without the country. The second lookup shows where partitioning stops helping and clustering keeps skipping.

**Time:** about 2 hours across three notebooks: `00_Setup_Environment` about 15 minutes, `01_Data_Layout` about 1 hour, `02_File_Maintenance` about 45 minutes. Most of it is reading: the cells themselves run for 1 to 2 minutes in `00` and in `01`, and about 4 minutes in `02`.

Run the notebooks in [`notebooks/`](notebooks/) in order: `00_Setup_Environment` (catalog and data), `01_Data_Layout` (partitioning, Z-Order, Liquid Clustering), `02_File_Maintenance` (compaction, auto compaction, VACUUM).

## 2. Learning Objectives

By completing the lab you will be able to:

- Choose between partitioning, Z-Ordering, and Liquid Clustering based on data shape & query patterns
- Diagnose small-file and data-skipping issues using `DESCRIBE DETAIL` + the query profile
- Apply manual compaction and contrast with auto compaction
- Explain why VACUUM keeps 7 days of unreferenced files and what lowering that retention risks
- Record files read, bytes read and duration for each layout in one comparison table, and use it to justify an optimization choice

## 3. What You Will Build

One Delta table per optimization strategy; every variant is built from the `sales_raw` baseline:

| Notebook | Logical Role | Table | Technique Illustrated |
|----------|--------------|-------|-----------------------|
| `00` | Baseline raw | `sales_raw` | Many small files, unoptimized |
| `01` | Country-partitioned | `sales_partitioned` | Low-cardinality partitioning |
| `01` | Z-Ordered copy | `sales_raw_zorder` | Multi-column data skipping |
| `01` | Liquid Clustered | `sales_liquid_clustered` | Incremental clustering on declared keys |
| `02` | Fragmented (pre-compaction) | `sales_to_compact` | Small file proliferation |
| `02` | Auto Optimize enabled | `sales_auto_compact` | Automatic write sizing + post-write auto compaction |

The `TABLES` registry and `tbl()` helper in `notebooks/variables.py` hold every fully qualified table name, so each cell refers to a table by its short key (`tbl('raw')`).

## 4. Architecture & Runtime Assumptions

- Databricks Free Edition or a paid workspace. On serverless compute (the only compute on Free Edition) you cannot disable the VACUUM retention check.
- Spark SQL + PySpark (no external data sources required)
- Delta Lake tables stored in a user-created catalog & schema (created automatically if permitted)

## 5. Prerequisites

- Basic Spark & Delta Lake familiarity (DataFrames, SQL, catalog objects)
- Comfort reading a query profile (files and bytes read, pruning)
- Python (for minor helper code)

## 6. How to Start

1. **Create a Databricks Account**
   - Sign up for a [Databricks Free Edition account](https://www.databricks.com/learn/free-edition) if you don't already have one.
   - Familiarize yourself with the workspace and notebook interface.

2. **Import this repository to Databricks**
   - In Databricks, click **Workspace** in the sidebar, open your home folder, then click **Create > Git folder**.
   - Paste the GitHub URL for this repository.
   - Authenticate with GitHub if prompted, and select the main branch.
   - The repo will appear as a folder in your workspace, allowing you to edit, run notebooks, and manage files directly from Databricks.
   - For more details, see the official Databricks documentation: [Git folders in Databricks](https://docs.databricks.com/aws/en/repos/).

3. Open the `notebooks/` folder, attach `00_Setup_Environment` to **Serverless** compute and run it. It creates the `delta_optimization_project` catalog and writes the 10M-row `sales_raw` table in about 2,000 files; a re-run reuses the table.
4. Run `01_Data_Layout`, then `02_File_Maintenance`, cell by cell.
5. After each benchmark query, click **See performance** under the cell output, open the query profile, and read the IO metrics (files and bytes read, pruning) and the wall-clock duration. Record them in the comparison table at the end of `01_Data_Layout`.

## 7. Extending the Lab

Try the experiments listed at the end of `02_File_Maintenance`, under **Suggested Next Experiments**.

## 8. Feedback Wanted

Tell me where this lab slowed you down. After running the notebooks, open a Discussion or Issue with:

1. Were the notebook instructions clear at each step? Where did you pause or re-read?
2. Which optimization concept remained fuzzy, and what supporting visual or explanation would help?
3. Would a short video walkthrough add value, or do you prefer self-discovery?

Happy to credit you as a contributor if you provide actionable feedback.

## 9. Troubleshooting Quick Tips

| Symptom | Possible Cause | Action |
| ------- | -------------- | ------ |
| `NameError: name 'tbl' is not defined` | The serverless session restarted after an idle break and lost the notebook's variables | Re-run the `%run ./variables` cell at the top of the notebook you are in |
| Baseline query reads only a few files | `sales_raw` was compacted since it was written (background auto compaction or predictive optimization) | Re-run `00_Setup_Environment`: it regenerates `sales_raw` whenever it holds fewer than 1,800 files |
| VACUUM removed no files | Retention window not elapsed | Check history timestamps: the files become eligible 7 days after they left the table (`delta.deletedFileRetentionDuration`) |
| Z-Order or Liquid query reads most of its files | The copy was compacted into 1 or 2 large files, so there is nothing to skip | Check `numFiles` in `DESCRIBE DETAIL` for that copy: it should hold about 9 files of about 18 MB. If it holds 1 or 2, re-run the copy cell in `01_Data_Layout`, which creates the table with `delta.targetFileSize = '16mb'` before the load |

## 10. Cleanup

Run `notebooks/03_Cleanup` to drop the `delta_optimization_project` catalog and every table in it. It runs:

```sql
DROP CATALOG IF EXISTS delta_optimization_project CASCADE;
```

---

Record metrics after every step; the numbers are the point.
