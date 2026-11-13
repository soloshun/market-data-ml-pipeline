# Concepts map and further reading

Two weeks is enough to build one real pipeline, not to cover all of data engineering. This page lists the ideas behind this repository, where each one appears in the code, and where to read further. The second half lists important topics this workshop **doesn't** cover, so you know what to explore next.

Use it as a checklist. If you can explain a concept in your own words and point to it in the code, tick it off.

**Start with the theory.** The [Theory → Working Pipeline slides](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s) introduce most of Part 1: the anatomy of a pipeline, ingestion patterns, medallion storage, data quality, history tracking, ETL vs ELT, and the modern development workflow. This page connects each idea to the code.

## Part 1 · Concepts you have already used

### Pipeline design

| Concept | In one line | Where it is in this repo | Go deeper |
| --- | --- | --- | --- |
| **ETL vs ELT** | Transform before loading vs load raw and transform inside the warehouse | Python transforms then loads (ETL); Bronze keeps raw data so ELT-style rebuilds are possible | [Fundamentals of Data Engineering, ch. 8](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/) |
| **Batch vs streaming vs incremental** | Batch and streaming describe *how often* you process. Incremental describes *which data* you load | A batch pipeline that loads incrementally: `catch-up` and the hourly watermark | [Slides: ingestion patterns](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s) |
| **Medallion architecture** | Bronze = as received, Silver = cleaned and typed, Gold = ready for analysis or ML | [ARCHITECTURE.md](ARCHITECTURE.md) | [Databricks medallion](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| **Append-only raw data** | Never overwrite raw input. Add a new snapshot instead | Each run writes a new Bronze file, even for the same dates | [Slides: storage](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s) |
| **Idempotency** | Running the same job twice gives the same result | `ON CONFLICT … DO UPDATE` in [`storage.py`](../src/market_data/ingestion/storage.py), `market-data counts` | [Maxime Beauchemin: Functional data engineering](https://maximebeauchemin.medium.com/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a) |
| **Upsert** | Insert a new key or update an existing one, in one statement | `UPSERT_PRICES` in `storage.py` | [PostgreSQL `INSERT … ON CONFLICT`](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT) |
| **Incremental load / watermark** | Remember how far you got, and only fetch what's new | `latest_dates`, `latest_hourly_bar`, the `catch-up` command | [README catch-up exercise](../README.md#practice-catch-up-after-missed-runs) |
| **Backfill** | Deliberately re-run an older date range | `market-data backfill`, `start_at` and `end_at` on the hourly flow | [README: repair historical ranges](../README.md#run-live-and-repair-historical-ranges) |
| **Overlap window** | Re-read a few recent periods to pick up late revisions | `OVERLAP_DAYS`, `OVERLAP` in [`hourly.py`](../src/market_data/ingestion/hourly.py) | — |
| **Transactions / atomicity** | Silver and Gold commit together or not at all | `_database_cursor` in `storage.py`, `test_gold_error_rolls_back_silver` | [SQLite transactions](https://www.sqlite.org/lang_transaction.html) |

### Data quality

| Concept | In one line | Where | Go deeper |
| --- | --- | --- | --- |
| **Data contract / schema validation** | Downstream users get the promised columns, types and rules | `validate_prices` in [`validation.py`](../src/market_data/ingestion/validation.py) | [Pandera](https://pandera.readthedocs.io/), [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) |
| **Expectations** | Named quality rules, each with an action on failure: warn, drop the row, or fail the batch | The `checks` dictionaries in `validation.py` are expectations whose action is *fail* | [Databricks expectations](https://docs.databricks.com/aws/en/ldp/expectations), [Great Expectations](https://docs.greatexpectations.io/) |
| **Pydantic models** | Validate one record or config object at a time, with an error for every field at fault | Prefect flow parameters, [`security/audit.py`](../security/audit.py) waivers | [Pydantic docs](https://docs.pydantic.dev/), [SCHEMA_DRIFT.md](SCHEMA_DRIFT.md#two-ways-to-write-a-contract-polars-and-pydantic) |
| **Fail fast** | Stop the batch on bad input instead of loading part of it | `_check_rows` | [SCHEMA_DRIFT.md](SCHEMA_DRIFT.md) |
| **Schema drift, rescue, quarantine, evolution** | Ways to handle a source that changes shape | `rescue_extra_columns`, notebook Section 3a | [SCHEMA_DRIFT.md](SCHEMA_DRIFT.md) |
| **Primary keys and constraints** | Let the database enforce rules, not just Python | [`sql/schema.sql`](../sql/schema.sql) | [PostgreSQL constraints](https://www.postgresql.org/docs/current/ddl-constraints.html) |
| **Lineage metadata** | Record where and when each row came from | `_source`, `_symbol`, `_ingested_at` in Bronze | [OpenLineage](https://openlineage.io/) |

### Time and time series

| Concept | In one line | Where | Go deeper |
| --- | --- | --- | --- |
| **Event time vs processing time** | When it happened vs when we recorded it | `trade_date` / `bar_start_utc` vs `_ingested_at` / `ingested_at` | [Streaming 101 (O'Reilly)](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) |
| **Half-open intervals `[start, end)`** | Include the start, exclude the end, so windows never overlap | Every window in `config.py` and `hourly.py` | — |
| **Store timestamps in UTC** | Convert at the edges, store one timezone | `bar_start_utc`, `utc_hour` | [Python `zoneinfo`](https://docs.python.org/3/library/zoneinfo.html) |
| **Trading calendars** | Weekends and holidays aren't gaps | Discussed in the README backfill notes | [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars) |
| **Look-ahead (future) leakage** | Features may only use data available at that moment | `shift(1)` and trailing windows in [`features.py`](../src/market_data/ingestion/features.py), `test_future_observations_do_not_change_prior_features` | [MLOPS.md](MLOPS.md) |

### Storage and compute

| Concept | In one line | Where | Go deeper |
| --- | --- | --- | --- |
| **Columnar files (Parquet)** | Store by column, so reading a few columns is cheap | Bronze snapshots | [Apache Parquet](https://parquet.apache.org/docs/) |
| **Partitioning** | Folder layout that lets readers skip data | `ingest_date=…/symbol=…/` | [Hive-style partitioning in Polars](https://docs.pola.rs/user-guide/io/hive/) |
| **Lazy evaluation and predicate pushdown** | Plan the query, then read only what's needed | Notebook Section 7 | [Polars lazy API](https://docs.pola.rs/user-guide/lazy/) |
| **OLTP vs OLAP** | Row stores for transactions vs column stores for analysis | SQLite and Postgres here. Warehouses such as BigQuery are OLAP | [DuckDB: why columnar](https://duckdb.org/why_duckdb) |

### Orchestration and operations (DataOps)

| Concept | In one line | Where | Go deeper |
| --- | --- | --- | --- |
| **Flows, tasks, DAGs** | A run made of tracked steps | [`flows/`](../src/market_data/flows/) | [Prefect concepts](https://www.prefect.io/docs/v3/concepts/flows) |
| **Retries for transient errors only** | Retry timeouts, never bad data | `retry_transient` in [`_shared.py`](../src/market_data/flows/_shared.py) | [Prefect retries](https://www.prefect.io/docs/v3/how-to-guides/workflows/retries) |
| **Schedules vs execution** | A schedule *creates* runs. A worker or `serve` process *executes* them. Passing CI doesn't mean anything is running | `market-data serve`, Managed work pool | [DEPLOYMENT.md](DEPLOYMENT.md) |
| **CI/CD** | Every change is tested, and `main` is released automatically | [`ci.yml`](../.github/workflows/ci.yml) | [GitHub Actions docs](https://docs.github.com/en/actions) |
| **Environment separation** | Dev (local SQLite) vs production (Supabase and Prefect Cloud) | `.env`, `prefect.yaml` | [The Twelve-Factor App: config](https://12factor.net/config) |
| **Reproducible environments** | Same packages everywhere | `uv.lock`, `uv sync --locked` | [uv docs](https://docs.astral.sh/uv/) |
| **Secrets management** | Credentials live in a vault, not in code | Prefect Secret block, GitHub secrets | [SECURITY.md](SECURITY.md) |
| **Conventional Commits** | Commit messages that tools and humans can scan | Git history, [`AGENTS.md`](../AGENTS.md) | [conventionalcommits.org](https://www.conventionalcommits.org/) |

## Part 2 · Important topics to read about next

These aren't in the workshop code. Each one is a common interview topic and part of a working data engineer's job.

| Topic | Why it matters | Start here |
| --- | --- | --- |
| **Dimensional modelling** (facts, dimensions, star schema) | How analytics tables are designed in most companies | Kimball, *The Data Warehouse Toolkit* |
| **History tracking:** soft deletes, SCD type 2, append-only logs | Answering both *what is true now?* and *what was true then?* Silver here keeps only the latest value | [Slides: data history](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s), [dbt snapshots](https://docs.getdbt.com/docs/build/snapshots) |
| **dbt and SQL-first transformation** | The most common way to build Silver and Gold in a warehouse | [dbt fundamentals course](https://learn.getdbt.com/) |
| **Cloud warehouses** (BigQuery, Snowflake, Redshift) | Where most company data lives | Any provider's free tier |
| **Open table formats** (Apache Iceberg, Delta Lake) | ACID tables, time travel and schema evolution on object storage | [Iceberg](https://iceberg.apache.org/), [Delta Lake](https://delta.io/) |
| **Object storage** (S3, GCS) | Durable home for Bronze. It's what the Cloud flow is missing | [Amazon S3 user guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) |
| **Change data capture (CDC)** | Stream every insert, update and delete out of an application database | [Debezium](https://debezium.io/) |
| **Streaming** (Kafka, windows, late data, exactly-once) | When minutes are too slow | [Kafka introduction](https://kafka.apache.org/intro), [Streaming 101](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) |
| **Distributed compute** (Spark) | When one machine isn't enough | [Spark quick start](https://spark.apache.org/docs/latest/quick-start.html) |
| **Bayesian hyperparameter tuning** | The standard way to tune models: learn from earlier trials instead of trying everything | [Optuna](https://optuna.org/), [MLOPS.md](MLOPS.md#hyperparameter-tuning-the-industry-standard) |
| **Ray and Ray Tune** | Parallel Python for training and tuning across many cores or machines | [Ray Tune](https://docs.ray.io/en/latest/tune/index.html) |
| **Feature stores and drift monitoring** | Reusing features safely, and noticing when a model's inputs change | [MLOPS.md](MLOPS.md#feature-store-ideas-mapped-to-this-project) |
| **Data observability** (freshness, volume, schema, distribution, lineage) | Noticing broken data before your users do | [Great Expectations](https://docs.greatexpectations.io/), [Soda](https://docs.soda.io/) |
| **Other orchestrators** (Airflow, Dagster) | You'll meet them at work; the ideas transfer from Prefect | [Airflow](https://airflow.apache.org/docs/), [Dagster](https://docs.dagster.io/) |
| **Infrastructure as code** (Terraform) | Replace ClickOps with reviewed, repeatable code | [Terraform tutorials](https://developer.hashicorp.com/terraform/tutorials) |
| **Containers** (Docker) | Package code and dependencies into one runnable image | [Docker get started](https://docs.docker.com/get-started/) |
| **Data governance and privacy** (PII, retention, access control) | Legal and ethical duties when the data is about people | Your local data-protection law, [Supabase RLS](https://supabase.com/docs/guides/database/postgres/row-level-security) |
| **Cost awareness** | Cloud bills grow with careless queries and schedules | Your provider's pricing calculator |

## Books

- Joe Reis and Matt Housley, *Fundamentals of Data Engineering*: the best map of the whole field. Read this first.
- Martin Kleppmann, *Designing Data-Intensive Applications*: how databases and distributed systems really work.
- Ralph Kimball and Margy Ross, *The Data Warehouse Toolkit*: dimensional modelling.
- Chip Huyen, *Designing Machine Learning Systems*: the bridge into MLOps.

## A suggested self-study order

1. Re-read this repository with Part 1 open. Explain each concept to a classmate using the code.
2. Do the [student assignment](STUDENT_ASSIGNMENT.md) with a different data source.
3. Rebuild your Silver and Gold layers in **dbt + DuckDB**. It's free and local, and it introduces SQL-first modelling.
4. Move Bronze to **object storage** and add one **data quality tool**.
5. Learn **dimensional modelling** and design a star schema for your source.
6. Pick one: streaming (Kafka), Spark, or a cloud warehouse.
