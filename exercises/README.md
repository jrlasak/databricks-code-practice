# Exercises

LeetCode-style reps on a single Databricks concept. Short, atomic, assertion-based. 5-20 min each. Pick any, skip around - exercise 5 never depends on exercise 1.

## The three-cell pattern

Every exercise follows the same structure:

1. **Read** the problem description (markdown cell with requirements + expected output).
2. **Write** your solution in the TODO cell. Your code writes to the output table named in the exercise.
3. **Run** the validation cell - assertions either pass or fail loudly.

TODO cell, then validation cell:

```python
# EXERCISE_KEY: merge_ex1
spark.sql("""
    -- your MERGE INTO db_code.merge_operations.merge_ex1_target here
""")

# Validation cell
result = spark.table("db_code.merge_operations.merge_ex1_target")
assert result.count() == 7, f"Expected 7 rows, got {result.count()}"
print("Exercise 1 passed!")
```

Solutions live in a separate notebook under `solutions/`, so you don't peek by accident.

## Topics

| Topic | Notebooks | Exercises | Focus |
|---|---|---|---|
| [Delta Lake](delta-lake/) | 6 | 51 | MERGE, time travel, schema enforcement, OPTIMIZE, liquid clustering, change data feed |
| [ELT](elt/) | 7 | 53 | Spark SQL joins, window functions, PySpark transformations, Auto Loader, batch ingestion, medallion, complex types |
| [Streaming](streaming/) | 6 | 44 | Structured Streaming basics, windowed aggregations & watermarks, stream-static joins, stream-stream joins, foreachBatch patterns, checkpointing & recovery |

**Total: 19 notebooks, 148 exercises.**

More topics coming - next up: Unity Catalog, Performance, Lakeflow Spark Declarative Pipelines (formerly DLT).

## Folder layout

Each topic folder contains:

```
{topic}/
  00_Setup.py                    # Shared data: catalog, schemas, base tables (each notebook runs it for you)
  01_{notebook}.py               # Exercise notebook (read problem, write code, validate)
  02_{notebook}.py
  ...
  setup/
    {notebook}-setup.py          # Per-notebook setup (called automatically via %run)
  solutions/
    {notebook}-solutions.py      # Hints, reference solutions, common mistakes
```

## Difficulty

Within each notebook, exercises run roughly from easy to hard:

| Level | Time | What It Tests |
|---|---|---|
| Easy | 5-10 min | Single concept, one correct approach |
| Medium | 10-20 min | Multiple concepts, edge cases, production awareness |
| Hard | 15-20 min | System design thinking, tradeoffs, multi-step |

A junior starts at exercise 1. A senior skips straight to the end.

## How to run

1. Open the topic folder you want.
2. Open any numbered notebook and run it from the top. The two `%run` cells under the intro run `00_Setup.py` and that notebook's own setup notebook for you.
3. Read the problem, write your solution in the TODO cell, run the validation cell.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
