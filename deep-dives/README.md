# Deep-Dives

Single-topic labs that go deep on one technique or capability - hands-on, end to end. Not a broad pipeline; one topic, done thoroughly, until you can do it for real.

1-5 hours each; every lab README gives its own estimate. Sequential - each step layers on the last.

## Labs

| Lab | What You Build | Focus |
|---|---|---|
| [6 Delta Optimization Techniques](optimization-techniques/) | Measure files and bytes read on partitioned, Z-Ordered and Liquid Clustered copies of a 10M-row, 2,000-file sales table, then run OPTIMIZE, auto compaction and VACUUM and read what each one changed in the table history. | Partitioning, Z-Order, Liquid Clustering, OPTIMIZE, Auto Compaction, Deletion Vectors, VACUUM |
| [Build a Genie Agent](genie-sales-analytics/) | Stand up a Databricks Genie Agent on a retail star schema (joins, SQL expressions, 8 example queries, 3 Unity Catalog functions), score it with a 10-question benchmark, then embed it in an AI/BI dashboard. | Genie Agent, Unity Catalog Functions, Star Schema, Benchmarks, AI/BI Dashboards |

## How a deep-dive works

1. **One topic** - each deep-dive goes deep on a single technique or capability, not a broad end-to-end pipeline.
2. **Set up once** - run the `00_*` notebook; it provisions the catalog, schema, and synthetic data.
3. **Work through it** - sequential notebooks (or setup guides) build the topic up step by step.
4. **Prove it** - end with something real: a measured result (a perf delta, a benchmark accuracy %) or a working capability you can demo, not "trust me."

You finish knowing one topic cold - and able to do it for real, not just describe it.

## Folder layout

Each deep-dive is self-contained:

```
{deep-dive}/
  00_*.py                        # Run once - provisions the catalog/schemas and synthetic data
  01_*.py, 02_*.py, ...          # Numbered notebooks (or setup guides), work through in order
  README.md                      # Scenario + walkthrough
```

Exact files vary by lab (some keep the notebooks in a `notebooks/` folder, some add a `data_generator.py`, others a `genie-setup/` guide folder) - each lab's `README.md` is the source of truth.

## How to run

1. Open the deep-dive folder you want.
2. Read its `README.md` for the scenario and walkthrough.
3. Run the `00_*` setup notebook once.
4. Work through the numbered notebooks (or setup guides) in order - each one builds on the previous step.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
