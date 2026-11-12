# Instructor walkthrough · 60 minutes

Start with the [historical ingestion notebook](../notebooks/01_historical_ingestion.ipynb) in one VS Code pane and the linked source files in the other. Keep one terminal for commands and one for Prefect. The fixture is the default for every core exercise.

## Before class

1. Open the code editor with `cli.py`, `flows/historical_ingestion.py`, `ingest.py`, `validate.py`, `transform.py` and `storage.py`.
2. Run `uv python install 3.12` and `uv sync --locked --group notebook` at the repository root. Open the notebook with the project's `.venv` kernel and run all cells. Copy `.env.example` to `.env` if needed. Run `uv run --locked pytest`.
3. Run notebook Section 5 and open `data/market_data.sqlite3` in your SQLite viewer if you plan to show the tables outside the notebook.
4. Start Prefect in its own terminal and open [the local dashboard](http://127.0.0.1:4200).

   ```sh
   export PREFECT_API_URL=http://127.0.0.1:4200/api
   export PREFECT_SERVER_UI_API_URL=http://127.0.0.1:4200/api
   export PREFECT_API_KEY=
   uv run --locked prefect server start --host 127.0.0.1 --port 4200
   ```

   In the command terminal, point to that server and run the fixture preview:

   ```sh
   export PREFECT_API_URL=http://127.0.0.1:4200/api
   export PREFECT_API_KEY=
   uv run market-data run --source fixture --skip-db
   ```

   For PowerShell and connection checks, see [local Prefect setup](../README.md#prefect-ui-and-local-daily-schedule).
5. If demonstrating GitHub deployment, prepare the [Cloud workspace, Process pool, worker and branches](DEPLOYMENT.md) before class. Open GitHub Actions and the Prefect Cloud deployment screen. Rehearse with a fixture run.

## 00–07 · Tour the repository (7 minutes)

Open notebook Section 1 and run `uv run market-data --help` in the terminal. Follow `cli.py` into `flows/historical_ingestion.py`, then point out the plain Python modules, SQL and tests. Explain how `uv.lock` makes each checkout resolve the same dependencies.

Use the [reading order](ARCHITECTURE.md#read-the-code-in-this-order) as the navigation path. Ask students to identify where to change a data rule and where to change the schedule.

## 07–15 · Fetch and inspect Bronze (8 minutes)

```sh
uv run market-data run --source fixture --skip-db
uv run python -c "import polars as pl; print(pl.read_parquet('PASTE_BRONZE_PATH_HERE').head())"
```

Start in notebook Section 2. The notebook prints the full path to its snapshot under `data/bronze/`. Use the `save_bronze` task log to find the separate CLI snapshot. Point out the original column names and ingestion metadata. Show the 30-row date range and explain the exclusive end. Optional live comparison if time and Yahoo access permit:

```sh
uv run market-data run --source yfinance --symbol 'GC=F' --start-date 2025-01-06 --end-date 2025-02-15 --skip-db
```

Identify `GC=F` as gold futures and the CSV as synthetic.

## 15–25 · Read the Polars validation and features (10 minutes)

Use notebook Sections 3 and 4 to show the duplicate-key check, inspect the features, and plot close, moving averages, and daily returns.

In `validate.py`, trace a date, the OHLC bounds, volume, and the duplicate check. In `transform.py`, trace `shift(1)` and the windows over each symbol. Ask what a future observation could change about the displayed rows. Point out the first non-null five-observation mean and volatility. The 20-observation mean starts on observation 20.

## 25–35 · Load and query SQLite (10 minutes)

```sh
uv run market-data run --source fixture
uv run market-data counts --source fixture
```

Use the SQL cell in notebook Section 5 to query the SQLite file. Show both tables, the common primary key, and nulls in the joined feature rows. In `storage.py`, trace the one transaction and explain why a short backfill can change later features. Record the counts before the rerun.

## 35–45 · Read Prefect states and show deployment (10 minutes)

Set `local_api_url` in notebook Section 6 to `"http://127.0.0.1:4200/api"` and run the cell. Open the [task diagram](ARCHITECTURE.md#orchestration-and-computation) next to the preview run in the local UI. Compare it with the database run from the previous stage, which also shows the Bronze task and the commit. In the command terminal:

```sh
uv run market-data serve --source fixture
```

Keep it running. Show `historical-ingestion/workshop-daily` in Deployments and click **Quick run**. Explain that Prefect schedules run creation, and the serving process executes each run.

If the Cloud setup is ready, show the Secret block name with its value hidden. Trace that name from `prefect.yaml` to `Secret.load(...).get()` in the flow, then show the commit SHA and worker status. Use the [deployment guide](DEPLOYMENT.md) for the full exercise. The local run covers orchestration if Cloud is unavailable.

## 45–52 · Prove repeatability and show a backfill (7 minutes)

```sh
uv run market-data run --source fixture
uv run market-data counts --source fixture
uv run market-data backfill --source fixture --symbol XAUUSD-DEMO --start-date 2025-01-13 --end-date 2025-01-18
uv run market-data catch-up --source fixture
```

Expect 30 rows and 30 distinct keys per table after the rerun. The manual backfill fetches five sessions and refreshes Gold from stored history. Catch-up uses the stored maximum and seven calendar dates of overlap. Ask how the class would find an older gap and what a market calendar would add.

## 52–60 · Diagnose a failure and assign the exercise (8 minutes)

```sh
uv run market-data run --source fixture --skip-db --demo-failure transient
uv run market-data run --source fixture --skip-db --demo-failure invalid
```

Open each run in Prefect. The first fails on attempt one, retries, and completes. The second writes Bronze and then fails validation with a missing `High` column. The load task never starts. Point students to notebook Section 7 for an optional pandas and Polars timing exercise. Show [the assignment and rubric](STUDENT_ASSIGNMENT.md) and ask students to choose a source and a record key.

## Backup routes

- **Yahoo unavailable** · Use the CSV for every stage. The date window and example outputs stay deterministic.
- **Supabase unavailable** · Use the local SQLite file and notebook query for the storage lesson.
- **Prefect Cloud unavailable** · Run the local server and `serve` command. If you cannot start a dedicated server, use a local profile with a temporary Prefect server and show terminal task states.
