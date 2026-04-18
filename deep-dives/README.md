# Deep-Dives

Single-topic labs that measure the impact of a technique with numbers, not intuition. Apply one lever at a time, benchmark the delta, prove what it actually buys you.

1-2 hours each. Sequential - each step layers on the last.

## Labs

| Lab | What You Build | Focus |
|---|---|---|
| [6 Delta Optimization Techniques](optimization-techniques/) | Iteratively apply and measure core Delta performance levers on a synthetic 50M-row dataset. | Partitioning, Z-Order, OPTIMIZE, Auto Optimize, Liquid Clustering, VACUUM |

## How a deep-dive works

1. **Baseline** - generate a synthetic dataset, capture starting query metrics (files scanned, data read, scan time).
2. **Layer one technique** - apply it, rerun the benchmark, record the delta.
3. **Layer the next** - repeat for each technique.
4. **Compare** - the before/after table tells you exactly what each lever buys you.

No "trust me, it's faster" - you measure it yourself.

## Folder layout

Each deep-dive is self-contained:

```
{deep-dive}/
  00_Setup_Environment.py        # Run once - creates catalog/schemas and generates the dataset
  01_*.py, 02_*.py, ...          # Numbered notebooks, work through in order
  data_generator.py              # Synthetic data (no external services needed)
  README.md                      # Scenario + walkthrough
```

## How to run

1. Open the deep-dive folder you want.
2. Read its `README.md` for the scenario and walkthrough.
3. Run `00_Setup_Environment.py` once.
4. Work through the numbered notebooks in order - each one builds on the previous benchmark.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
