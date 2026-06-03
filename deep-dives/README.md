# Deep-Dives

Single-topic labs that go deep on one technique or capability - hands-on, end to end. Not a broad pipeline; one topic, done thoroughly, until you can do it for real.

1-3 hours each. Sequential - each step layers on the last.

## Labs

| Lab | What You Build | Focus |
|---|---|---|
| [6 Delta Optimization Techniques](optimization-techniques/) | Iteratively apply and measure core Delta performance levers on a synthetic 50M-row dataset. | Partitioning, Z-Order, OPTIMIZE, Auto Optimize, Liquid Clustering, VACUUM |
| [Build a Genie Space](genie-sales-analytics/) | Stand up and tune a production Databricks Genie space on a retail star schema, then benchmark its natural-language answer accuracy against ground-truth SQL. | Genie, Unity Catalog Functions, Star Schema, Benchmarking, AI/BI Dashboards |

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

Exact files vary by lab (some add a `data_generator.py`, others a `genie-setup/` guide folder) - each lab's `README.md` is the source of truth.

## How to run

1. Open the deep-dive folder you want.
2. Read its `README.md` for the scenario and walkthrough.
3. Run the `00_*` setup notebook once.
4. Work through the numbered notebooks (or setup guides) in order - each one builds on the previous step.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
