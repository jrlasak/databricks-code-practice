# Contributing

Thanks for being here. This repo is a practice ground, so contributing works a little differently than a normal open-source project.

## Do not open a PR with your solutions

**Your solved exercises stay in your fork.** That is the whole point of forking - it's your workspace, your progress, your notes.

The most common PR this repo gets is a completed `01_merge-operations.py`. Understandable instinct, but merging one would delete the exercise for everyone else. If you finish a notebook, keep it. Push it to your fork, star the repo if it helped, and move on to the next one.

## What is genuinely useful

### 1. Report a broken exercise

This is the highest-value thing you can do. Exercises are validated by assertions, and an assertion can be wrong - the setup data may not contain the case the problem describes, or the reference solution may not match the stated requirement.

Example: an exercise asks for same-customer/same-day order pairs, but no customer in the base table has two distinct order IDs on the same date, so both the exercise and the solution return zero rows and still "pass."

If an exercise can't be solved as written, [open an issue](../../issues/new/choose). Include:

- Which notebook and which exercise number
- What the assertion says vs. what you get
- The output or error, pasted as text

### 2. Report a factual error

Databricks moves fast. If a notebook describes behavior that no longer matches the product - a deprecated option, a changed default, a renamed feature - say so and link the current docs. Say where you ran it: Free Edition serverless, or a cluster and its Databricks Runtime version.

### 3. Suggest an exercise or topic

Topics land here when most Databricks engineers hit them regularly. "Optimize a skewed join" is a good suggestion. "Fix ORC compatibility in a legacy Hive migration" is not - too narrow to earn a slot.

Open an issue describing the concept and, ideally, the specific mistake or misunderstanding an exercise on it would catch.

## If you do want to send a PR

Fixes are welcome - a broken assertion, a wrong expected count, a typo, a dead link, a stale doc claim. Before you open it:

- **One fix per PR.** Don't bundle a typo fix with a new exercise.
- **Never commit a solved TODO cell.** Check your diff. If it contains working code where a `# TODO` used to be, that's a solution leak into the exercise.
- **Keep the exercise solvable.** If you change setup data, paste each reference solution from the matching `solutions/` notebook into its TODO cell, run the exercise notebook end to end, and confirm every assertion passes with a non-zero result. Then put each TODO cell back exactly as it is on `main`, starting with its `# EXERCISE_KEY` line, before you commit.
- **Notebooks are `.py` source files**, not `.ipynb`. Edit them as Python with the Databricks cell markers intact.
- **Match the surrounding style.** No em dashes, no emoji in exercise text, assertion messages state expected vs. actual.

New exercises are a bigger ask. Open an issue first so we can agree the topic is worth a slot before you write eight of them.

## Licensing of contributions

This repo is licensed under [CC BY-SA 4.0](LICENSE). By contributing, you agree your contribution is licensed the same way: free for anyone to reuse and adapt, including commercially, as long as they credit the source and license their adaptation under the same terms.

## Questions

Not sure whether something is a bug or your own mistake? Open an issue anyway. A wrong report costs a minute; a broken exercise silently wastes everyone else's afternoon.
