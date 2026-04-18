# Exercises

LeetCode-style reps on a single Databricks concept. Short, atomic, assertion-based. 5-30 min each. Pick any, skip around - exercise 5 never depends on exercise 1.

## The three-cell pattern

Every exercise follows the same structure:

1. **Read** the problem description (markdown cell with requirements + expected output).
2. **Write** your solution in the TODO cell.
3. **Run** the validation cell - assertions either pass or fail loudly.

```python
# TODO cell
result = spark.sql("""
    -- your solution here
""")

# Validation cell
assert result.count() == 42, f"Expected 42 rows, got {result.count()}"
print("Exercise 1 passed!")
```

Solutions live in a separate `solutions/` notebook so you don't accidentally peek.

## Topics

| Topic | Notebooks | Exercises | Focus |
|---|---|---|---|
| [Delta Lake](delta-lake/) | 6 | 51 | MERGE, time travel, schema enforcement, OPTIMIZE, liquid clustering, change data feed |
| [ELT](elt/) | 7 | 53 | Spark SQL joins, window functions, PySpark transformations, Auto Loader, batch ingestion, medallion, complex types |

**Total: 13 notebooks, 104 exercises.**

More topics coming - next up: Streaming, Unity Catalog, Performance, DLT.

## Folder layout

Each topic folder contains:

```
{topic}/
  00_Setup.py                    # Run once - creates catalog, schemas, base tables
  01_{notebook}.py               # Exercise notebook (read problem, write code, validate)
  02_{notebook}.py
  ...
  setup/
    {notebook}-setup.py          # Per-notebook setup (called automatically via %run)
  solutions/
    {notebook}-solutions.py      # Hints, reference solutions, common mistakes
```

## Difficulty

Within each notebook, exercises progress from easy to hard:

| Level | Time | What It Tests |
|---|---|---|
| Easy | 5-10 min | Single concept, one correct approach |
| Medium | 10-20 min | Multiple concepts, edge cases, production awareness |
| Hard | 20-30 min | System design thinking, tradeoffs, multi-step |

A junior starts at exercise 1. A senior skips straight to the end.

## How to run

1. Open the topic folder you want.
2. Run `00_Setup.py` once - creates the catalog, schemas, and base tables.
3. Open any numbered notebook, read the problem, write your solution, run the validation cell.

All on Databricks Free Edition. Serverless compute, Unity Catalog, Delta Lake. No cloud account, no cluster config.
