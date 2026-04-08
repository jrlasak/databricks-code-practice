# Databricks Code Practice

LeetCode-style coding problems for Databricks data engineers. Import this repo into your Databricks workspace, pick a topic folder, run its `00_Setup.py`, then solve exercises - write code, run assertions, check pass/fail.

104 exercises across 13 notebooks. All free.

### How is this different from my [hands-on labs](https://github.com/jrlasak)?

| | This repo | Hands-on labs |
|---|---|---|
| **Format** | Single exercise, one TODO cell | Multi-notebook guided project |
| **Time** | 5-30 minutes per exercise | 2-3 hours per lab |
| **Narrative** | None. "Given table X, write a query that..." | Business scenario. "You're building a streaming pipeline for..." |
| **Scope** | One concept per exercise (MERGE, window functions, ...) | End-to-end project (ingestion, transformation, quality, orchestration) |
| **Order** | Pick any exercise, skip around freely | Sequential notebooks that build on each other |
| **Goal** | Drill a specific skill until it's automatic | See how multiple concepts fit together in a real project |

Use this repo when you want focused reps on a single concept. Use the labs when you want to build something end-to-end.

> **Disclaimer**: This is an independent educational resource created by Jakub Lasak. It is not affiliated with, endorsed by, or sponsored by Databricks, Inc.

## Author

**Jakub Lasak** - Databricks Data Engineer

- [LinkedIn](https://www.linkedin.com/in/jrlasak/) (13.5K followers)
- [Substack](https://dataengineerwiki.substack.com/) (newsletter)
- [DataEngineer.wiki](https://dataengineer.wiki) (cheat sheets, learning paths, cert guides)

## How It Works

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) (free)
2. Clone or download this repo
3. Import the notebooks into your Databricks workspace
4. Pick a topic folder, run `00_Setup.py` once to create shared tables
5. Open any exercise notebook, solve the TODO cells, run the validation cells

Each exercise is atomic - you can skip around freely. Exercise 5 never depends on Exercise 1's output.

## Topics

<!-- TOPICS-START -->
| Topic | Notebooks | Exercises | Description |
|---|---|---|---|
| [Delta Lake](delta-lake/) | 6 | 51 | MERGE operations, time travel, schema enforcement, OPTIMIZE, liquid clustering, change data feed |
| [ELT](elt/) | 7 | 53 | Spark SQL joins, window functions, PySpark transformations, Auto Loader, batch ingestion, medallion architecture, complex data types |

**Total: 13 notebooks, 104 exercises**
<!-- TOPICS-END -->

## Notebook Structure

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

## How Exercises Work

Every exercise follows the same pattern:

1. **Read** the problem description (markdown cell with requirements + expected output)
2. **Write** your solution in the TODO cell
3. **Run** the validation cell - assertions either pass or fail

```python
# TODO cell
# EXERCISE_KEY: merge_ex1
result = spark.sql("""
    -- your solution here
""")

# Validation cell
assert result.count() == 42, f"Expected 42 rows, got {result.count()}"
print("Exercise 1 passed!")
```

Solutions are in a separate `solutions/` notebook so you don't accidentally peek.

## Difficulty

Exercises within each notebook progress from easy to hard:

| Level | Time | What It Tests |
|---|---|---|
| Easy | 5-10 min | Single concept, one correct approach |
| Medium | 10-20 min | Multiple concepts, edge cases, production awareness |
| Hard | 20-30 min | System design thinking, tradeoffs, multi-step |

A junior can start with the first few exercises. A senior can skip straight to the end.

## Requirements

- Databricks Community Edition (free) or any Databricks workspace
- Serverless compute or a cluster with DBR 13.3+
- Python and SQL only (no Scala/R)

## Feedback

Found a bug? Have a suggestion? [Open an issue](../../issues).
