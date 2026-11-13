# Student practice and assignment

Two parts. **Warm-ups** are small changes to *this* repository, so you get comfortable reading and changing working pipeline code. The **assignment** asks you to build your own pipeline for a different data source. A predictive model belongs to a later session.

## Part 1 · Warm-up exercises

Work on a branch: `git switch -c practice/<your-name>`. For each exercise, change the code, add or update a test, and run:

```sh
uv run --locked ruff check . && uv run --locked ruff format --check . && uv run --locked pytest
```

"Done when" tells you how to check your work. The hints point to where to start reading. They don't give the answer.

### Level 1 · Read and trace

| # | Exercise | Done when | Hint |
| --- | --- | --- | --- |
| 1 | Trace one fixture row from the CSV to the `market_features` table. List every function it passes through. | You can explain each step to a classmate without notes | Start at `run_command` in [`cli.py`](../src/market_data/cli.py) and follow the [reading order](ARCHITECTURE.md#read-the-code-in-this-order) |
| 2 | Run the failure demos. For each, say which task failed, whether Prefect retried it, and why. | Your answers match the Prefect UI | `--demo-failure transient` and `--demo-failure invalid` in the [instructor guide](INSTRUCTOR_DEMO.md#5260--diagnose-a-failure-and-assign-the-exercise-8-minutes) |
| 3 | Find five security controls in the code. | You can name the file and line for each | [SECURITY.md](SECURITY.md) |

### Level 2 · Change a rule

| # | Exercise | Done when | Hint |
| --- | --- | --- | --- |
| 4 | Add a **10-session moving average** (`sma_10`) to daily Gold. | It's in the preview output and in SQLite, and a test checks its warm-up nulls | [`features.py`](../src/market_data/ingestion/features.py), both SQL schemas, `UPSERT_FEATURES` in `storage.py`, [`test_transform.py`](../tests/test_transform.py) |
| 5 | Reject any daily bar whose `high` is more than 20 % above its `low`. | A new test case in `test_validate.py` passes with a clear error message | `_ohlcv_checks` in [`validation.py`](../src/market_data/ingestion/validation.py) |
| 6 | Add an `hourly` option to the `counts` command. | `market-data counts --hourly` prints row and distinct-key counts for both hourly tables | `row_counts` in `storage.py`, `make_parser` in `cli.py` |

### Level 3 · Design a change

| # | Exercise | Done when | Hint |
| --- | --- | --- | --- |
| 7 | **Store rescued fields for daily data**, as the hourly flow already does. | A daily batch with an extra column stores it in `market_prices._rescued_data` | [SCHEMA_DRIFT.md §6](SCHEMA_DRIFT.md#6-practice) |
| 8 | **Build a real quarantine.** Good rows load and bad rows go to a `quarantine_prices` table with the reason. | A test shows 29 rows in Silver and 1 in quarantine | Decide what happens when *every* row is bad |
| 9 | **Detect gaps** in stored daily history using weekdays only, and print the missing dates. | A test with a deleted row finds exactly that date | Remember the README warning about holidays |
| 10 | Run CI on pushes to `dev` too. Explain one advantage and one cost. | The workflow change, plus two sentences in your PR description | `on:` in [`ci.yml`](../.github/workflows/ci.yml) |

Open a pull request into **your own** `dev` branch for review. Don't merge into `main`, because `main` deploys to production.

## Part 2 · Build your own pipeline

Adapt this repository to a source you can explain, such as weather observations, sports results, public transport, air quality or retail transactions. Submit a repository and a five-minute walkthrough video or live demo. Include a small offline fixture and instructions a classmate can follow from a clean checkout.

**Pick a source that has:** a clear record key (for example `station_id` + `observed_at`), at least one numeric field you can validate, and a free API or downloadable file. Avoid sources that need paid keys or contain personal data.

| Criterion | Points | Evidence |
| --- | ---: | --- |
| Source and Bronze | 15 | Fetch data with documented parameters and save one raw snapshot with metadata (`_source`, `_ingested_at`). |
| Validation | 15 | Define required fields, a record key and two domain checks. Show a useful error for a rejected batch. |
| Transformation | 15 | Use Polars to create a clean Silver table and one meaningful derived value in Gold. Explain its inputs and any nulls. |
| Persistent store | 15 | Use SQLite locally, or PostgreSQL when deployed. Show a primary key and parameterized upserts. Keep any credentials in `.env` (ignored) or a Prefect Secret block. |
| Repeatability | 15 | Run the same window twice and show stable row and distinct-key counts. Provide one backfill command. |
| Orchestration | 15 | Show a Prefect run with visible tasks and logs, plus one recoverable or permanent failure example. |
| Documentation | 10 | Include setup, example commands, expected results and at least one known limitation. |

**Bonus (up to +10):** handle schema drift (rescue or quarantine), add CI with GitHub Actions, or deploy on a schedule.

### Before you submit

- [ ] A classmate cloned your repository and ran the quickstart without help
- [ ] `git ls-files` shows no `.env`, database file or credentials
- [ ] Running the pipeline twice doesn't change the row counts
- [ ] The README says what the data is, where it comes from, and one thing your pipeline can't handle yet
