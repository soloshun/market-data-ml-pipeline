# Market Data ML Pipeline roadmap

By the end of the workshop, students should be able to trace a gold futures forecast from the source bars through training, evaluation, and a saved batch prediction. They should also be able to explain when this workload fits on one machine and when a distributed engine would help.

Checked items exist in the project. Unchecked items are class demos or planned work. Cloud release steps are called out separately from code that is ready on `dev`.

**Guiding rule:** keep every step runnable on a laptop with free tiers. Add one new idea at a time and reuse what already exists. For example, new features go into the existing Gold table, and the prediction flow copies the ingestion flow's structure.

| Sprint | Lecture | What it covers | Main guide | Status |
| --- | --- | --- | --- | --- |
| 1 | **Data engineering and DataOps** | Ingest, validate, store, orchestrate, release | [Theory slides](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s), [notebook](../notebooks/01_historical_ingestion.ipynb) | Wrapping up |
| 2 | **End-to-end ML lifecycle** | Dataset, EDA, features, XGBoost and LightGBM, tuning | [MLOPS.md](MLOPS.md) | Planned |
| 3 | **Automations using MLflow** | Tracking, registry, deployment strategies, scheduled prediction | [MLOPS.md](MLOPS.md#deployment-strategies) | Planned |
| 4 | **Distributed vs single-thread compute** | Spark, pandas vs Polars, when to distribute | Notebook Section 7 | Partly built |
| 5 | **Business dashboard with AI coding agents** *(optional)* | A Next.js dashboard over everything built so far | [Sprint 5](#sprint-5--business-dashboard-with-ai-coding-agents-optional) | Optional |

## Session calendar

| Session | Plan | Roadmap |
| --- | --- | --- |
| Tuesday, Nov 17 | Merge [PR #2](https://github.com/soloshun/market-data-ml-pipeline/pull/2) live, show the new Prefect schedule, and leave time for questions. Show the `protect-main` ruleset and how a failing dependency audit blocks the merge until it is fixed. | Sprint 1 wrap-up |
| Thursday, Nov 19 | Give students most of the session to share what they built. Close with a short MLOps introduction, with no coding. | Student projects, Sprint 2 framing |
| Tuesday, Nov 24 | Walk through one forecasting example from hourly data to features, tuning, model selection, and inference. | Sprints 2–3 |
| Tuesday, Dec 1 | Use the full session to build that workflow together. | Sprints 2–3 |

```mermaid
flowchart LR
    A["Data engineering<br/>and DataOps"] --> B["End-to-end<br/>ML lifecycle"]
    B --> C["Automations<br/>using MLflow"]
    C --> D["Distributed vs<br/>single-thread compute"]
    D -.-> E["Optional: business<br/>dashboard"]
```

**Where it ends up:** every weekday at 00:00, 06:00, 12:00 and 18:00 (Accra = UTC), ingestion loads the latest completed hourly bars. A few minutes after it finishes, a prediction flow forecasts the next six hours and stores the forecast. When the six hours have passed, the forecast is scored against what actually happened.

```mermaid
flowchart LR
    I["hourly-ingestion<br/>00, 06, 12, 18 UTC"] -->|"completes"| P["prediction flow<br/>6-hour forecast"]
    P --> F[("hourly_forecasts")]
    I -->|"6 hours later the target bar arrives"| S["score forecasts"]
    S --> F
```

## Sprint 1 · Data engineering and DataOps

- [x] **Foundations.** The [Theory → Working Pipeline slides](https://gamma.app/docs/Theory-Working-Pipeline-s6v9hogx9py3t3s) cover pipeline anatomy, batch vs streaming vs incremental, medallion storage, data quality, history tracking, ETL vs ELT and the dev-to-prod workflow. The [concepts map](CONCEPTS.md) links each idea to the code.
- [x] **Local daily lesson.** Run the [ingestion notebook](../notebooks/01_historical_ingestion.ipynb) or the CLI with synthetic `XAUUSD-DEMO` data. Inspect Bronze Parquet and its metadata, validate prices with Polars for Silver, and calculate trailing features for Gold. Query Silver and Gold in SQLite.
- [x] **Hourly cloud ingestion.** The released Prefect flow fetches completed Yahoo Finance `GC=F` bars and writes hourly Silver and Gold to Supabase. Silver is the first stored layer in this path. The local daily lesson remains available without cloud accounts.
- [x] **Operate and recover.** Read Prefect logs and task states, retry temporary fetch failures, and fail invalid batches before storage. Use the latest saved bar and a short overlap to catch up after a missed run. Replay an older range explicitly and rerun it without duplicating keys. Silver and Gold commit together.
- [x] **Handle changes at the source.** The [schema drift lesson](SCHEMA_DRIFT.md) covers the kinds of drift and four responses: rescue, fail fast, quarantine and evolve. Hourly Silver persists `_rescued_data` on `dev`. Apply the [additive Supabase schema change](DEPLOYMENT.md#2-set-up-supabase) before releasing it. Quarantine is a notebook exercise.
- [x] **Verify, release, and protect credentials.** The locked `uv` environment and fixture tests support local rehearsals. Develop on `dev`, review a release PR into `main`, and use GitHub Actions to run checks and release to Prefect when enabled. Store the Supabase URI in a Prefect Secret block and the Prefect key in GitHub Actions secrets. See the [deployment guide](DEPLOYMENT.md), which shows both terminal and UI routes.
- [x] **Security gate.** On every PR into `main`, the dependency audit fails the checks when a locked package has a known vulnerability. See the [security walkthrough](SECURITY.md).
  - [x] Turn on the `protect-main` ruleset ([SECURITY.md §4](SECURITY.md#4-make-the-security-check-block-the-merge)). Without it, a failing check doesn't stop the Merge button.
- [ ] **Finish the code walkthrough.** Cover the remaining modules with the [reading order](ARCHITECTURE.md#read-the-code-in-this-order). Leave some parts for students to discover through the [warm-up exercises](STUDENT_ASSIGNMENT.md#part-1--warm-up-exercises).
- [ ] **Class wrap-up: push to production.** Use the [schedule PR](https://github.com/soloshun/market-data-ml-pipeline/pull/2) to move from 06:00 and 12:00 daily to 00:00, 06:00, 12:00 and 18:00 on weekdays in Accra. Review the diff, watch **checks**, merge, watch **deploy**, then compare the deployed commit and schedule in Prefect. Show how to pause or resume and inspect a catch-up run. If time allows, demonstrate a scoped failure alert. Leave time for questions and [student projects](STUDENT_ASSIGNMENT.md).

**Show at the end:** Re-run an ingestion window, explain why the same keys remain unique, and trace one record from its source through Silver and Gold.

## Sprint 2 · End-to-end ML lifecycle

Begin with a short walkthrough and discussion. Trace acquisition and cleaning back to Sprint 1, then explain what changes when a dataset needs to answer a prediction question. Build the training notebook in the coding session.

- [ ] **Frame the problem.** Predict the six-hour percentage return of `GC=F` at each weekday checkpoint (00:00, 06:00, 12:00, 18:00 UTC). For a noon prediction, use only completed bars, the last one starting at 11:00, and aim at the close of the bar ending at 18:00. To show a price, multiply the latest close by one plus the predicted return. Returns are easier to model than raw prices because their scale doesn't drift with the gold price. Skip historical examples when the exact target is missing. Six observed rows can span more than six clock hours.
- [ ] **Assemble and explore the dataset.** Join hourly Silver and Gold as shown in the [MLOps handoff](MLOPS.md). Freeze the input snapshot and record its source, query, and date range. Plot closes and returns, inspect gaps and warm-up nulls, and count usable examples at each checkpoint. Stored bars can include provider revisions, so the snapshot alone does not prove what was available historically at every checkpoint. Prepare a longer synthetic hourly sample for anyone without Supabase access. The short ingestion fixture is only for smoke tests.
- [ ] **Explore before modelling (EDA).** Plot the distribution of six-hour returns and compare it with a normal curve. Gold has fat tails. Check autocorrelation of returns at lags 1–24, average volatility by hour of day and day of week, and where the gaps and market closures fall. Each finding should suggest a feature or rule out an idea. Keep the plots in the training notebook so the reasons behind the feature choices are recorded.
- [ ] **Add features in Gold, not in the notebook.** Extend `build_hourly_features` so training and prediction use the same code. This avoids training/serving skew, where the model sees features computed one way in training and another way in production. Every feature must be trailing, using only the current and earlier bars. Keep it small: the [feature list in MLOPS.md](MLOPS.md#features-to-try) has eight candidates, each with the reason it might help. It also explains **cyclical time features** with a picture of what the model sees with and without them.

  Adding columns to `hourly_features` is a deliberate **schema evolution**. Update both SQL schemas, the upsert and the tests together, as described in [SCHEMA_DRIFT.md](SCHEMA_DRIFT.md#step-5--evolve-the-schema-on-purpose). Compare the model with and without the new features on the same validation dates.
- [ ] **Tune XGBoost and LightGBM side by side, then pick one.** Start with a zero-return baseline. Then run the **same** `GridSearchCV` on both libraries: the same features, the same `TimeSeriesSplit` folds and the same scoring (mean absolute error). Each gets a small grid of eight combinations, shown in [MLOPS.md](MLOPS.md#tune-two-models-pick-one). Put the results in one comparison table: cross-validated MAE with its spread across folds, RMSE, directional hit rate, training time, and the improvement over the baseline. Pick the model with the lower MAE. If the two are within one standard deviation of each other, prefer the faster or simpler one. Score the chosen model once on an untouched test period at the end. If neither beats the baseline, show the results anyway and keep the baseline as the default. That outcome is a useful lesson too.
- [ ] *Optional extension:* run [Optuna](https://optuna.org/) with the same number of trials to show how a smarter search than a grid finds good settings faster.

**Show at the end:** One notebook that recreates the examples, explains one input-to-target row, and reports baseline, XGBoost and LightGBM scores on the same dates.

## Sprint 3 · Automations using MLflow

Use this session to answer three questions: What was trained? Which version should run? How will we know when it stops working? Locally we answer them with MLflow. In production we copy the same ideas into a Supabase table, because Prefect Managed workers have no MLflow server. See [where the model lives](MLOPS.md#where-the-model-lives-local-vs-production). The lecture order: challenges in traditional ML → from experimentation to deployment → MLflow → tracking, packaging, registration, serving and inference → putting it all together.

- [ ] **Show the problem first.** Open the Sprint 2 notebook and ask: which settings produced the best score last week? Which data was it trained on? Without tracking, nobody knows. This is the "challenges in traditional ML" slide, made concrete.
- [ ] **Track runs with MLflow.** Run [MLflow Tracking on local SQLite](https://mlflow.org/docs/latest/ml/tracking/tutorials/local-database/) with a local artifacts folder, so no server is needed. Log each tuning trial from Sprint 2 as a run with the snapshot ID, split dates, feature list, hyperparameters, metrics and git commit. Compare runs in the MLflow UI. If MLflow setup fails in class, a small CSV run table keeps the lesson moving.
- [ ] **Package and register.** Log the chosen model with its MLflow flavor (`mlflow.xgboost` or `mlflow.lightgbm`), together with an input signature listing the expected feature columns. Register it in the **Model Registry** and give the chosen version the alias `champion`. Prediction code loads `models:/<name>@champion`, so promoting a new version only means moving the alias. For the Prefect exercise, pass hyperparameters through a configuration block, kept separate from the model artifact and its version.
- [ ] **Production model registry in Supabase.** Create `market_data.model_registry` from the [sketch in MLOPS.md](MLOPS.md#where-the-model-lives-local-vs-production). Each row stores the **trained model as text**, whichever library won (XGBoost JSON or a LightGBM model string), plus the library name, hyperparameters, feature columns, metrics, training window and git commit. Hyperparameters alone are not enough, because the prediction flow needs the trained trees, not just the recipe. A partial unique index guarantees at most one `champion`.
- [ ] **Weekly training flow with a production gate.** A Prefect deployment runs every Saturday at 02:00 (`0 2 * * 6`), while the market is closed. It tunes XGBoost and LightGBM with the same small `GridSearchCV` and keeps the better one as the candidate. Then it scores the candidate, the current champion and the zero-return baseline on the same unseen checkpoints, and applies the [gate rules](MLOPS.md#the-production-gate). A winner becomes `champion` and the old one is retired, in one transaction. A loser is saved as `rejected` with the reason. Add `xgboost`, `lightgbm` and `scikit-learn` to `prefect.yaml` with the same pins as `uv.lock`. `test_deployment.py` checks this.
- [ ] **Deployment strategies (theory).** Before automating anything, discuss how a new model version reaches production safely: blue-green, canary, shadow and A/B testing. See [MLOPS.md](MLOPS.md#deployment-strategies). The weekly gate is a simple form of **blue-green** with an offline test. **Shadow** is the natural next step: the newest rejected or candidate model also predicts every live checkpoint, and it is promoted only after it scores better on real forecasts. Setting the previous model back to `champion` is the rollback.
- [ ] **Serve: batch first, real-time as a demo.** The main path is **batch inference**: a Prefect prediction flow that loads the champion, reads the latest Gold row at a checkpoint, and upserts a forecast. Locally it loads `models:/<name>@champion` from MLflow. In production it loads `model_text` from the `champion` row in Supabase, using `model_family` to pick the library. The forecast records the symbol, checkpoint time, target time, `model_id`, predicted return and the time it was predicted. Key it by `(symbol, checkpoint_utc, model_id)` so reruns update the same row. Optionally show `mlflow models serve` locally to contrast real-time REST serving with batch.
- [ ] **Run it after ingestion.** The simple option is a cron about 10 minutes after each ingestion run (`10 */6 * * 1-5`). The sturdier option is a Prefect **deployment trigger** that starts prediction when `hourly-ingestion` completes. Comparing the two makes a good discussion: what happens to each when ingestion runs late? Training keeps its own weekly schedule and never runs on the ingestion schedule.
- [ ] **Automatic backfill for a near-real-time pipeline.** If the prediction flow misses checkpoints, for example after an outage, the next run should find checkpoints that have bars but no forecast and fill them in. It uses only the features available at each checkpoint (point-in-time), just as ingestion catch-up uses a watermark. Mark these forecasts as backfilled (`predicted_at` later than the checkpoint) so they are evaluated separately. A backfilled forecast never *could* have been used live. Forecasts are append-only: when a bar is revised later, keep the original forecast as it was made.
- [ ] **Monitor and maintain.** A scoring step fills in the actual return once the target bar arrives. These are late-arriving labels. Start with data freshness, missing inputs and failed runs. Then compare recent error with the baseline. Explain the difference between schema drift (fields changed), data drift (input values shifted), concept drift (the input-to-target relationship changed) and performance drift (errors worsened). Use those checks to decide when to investigate or retrain.

**Show at the end:** Take one saved prediction and trace it back to the exact model, its training run and its input window. Then show the gate rejecting a model that didn't beat the champion, with the reason stored in `model_registry`. AWS hosting, a permanent MLflow server, and a public inference API are discussion topics or extensions.

## Sprint 4 · Distributed vs single-thread compute

Lecture order: what Spark is and how it works → why it was created → when and when not to use it → case study: scanning table sizes across Databricks workspaces → solutions and a performance comparison → conclusions → classroom benchmark.

- [ ] **Teach the execution model.** Explain what the driver, executors, partitions and shuffle do, and how Spark builds a lazy plan, much like Polars' `scan_parquet` in notebook Section 7. Show how work can use several cores on one machine before showing what changes across machines. [Spark cluster overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [ ] **Explain why Spark exists.** Hadoop MapReduce wrote intermediate results to disk between every step. Spark keeps them in memory, which made iterative ML and interactive queries practical on clusters. Then ask what has changed since: a single laptop now has many cores and plenty of RAM, and engines such as Polars and DuckDB use them well.
- [ ] **Case study.** Use a synthetic table-size inventory for the Databricks workspace scan. Discuss memory, data growth and workload shape when deciding what to distribute. Compare the candidate solutions on the same input.
- [ ] **Benchmark a real operation.** [Notebook Section 7](../notebooks/01_historical_ingestion.ipynb) already compares pandas with PyArrow against lazy Polars on one size, about 2 million rows, and checks that the results match. Extend it to several sizes, and add a `POLARS_MAX_THREADS=1` run in a separate process to separate the effect of threads from the effect of the query plan. Note that pandas computes mostly on one thread, but its PyArrow Parquet reader can use several. Keep file reads separate from computation, warm up, repeat, and report median time and Polars' [thread pool size](https://docs.pola.rs/api/python/stable/reference/api/polars.thread_pool_size.html). Let the results decide which tool wins each case.
- [ ] **Try the same operation in local PySpark.** Explain why startup and shuffle can cost more than the work itself on small inputs. Local Spark is still one machine. A real cluster or distributed model training is optional.

**Show at the end:** One timing table and a short recommendation for this project, supported by the measurements. Keep the ingestion and forecasting demos runnable on a laptop.

## Sprint 5 · Business dashboard with AI coding agents *(optional)*

A session facilitated with a co-instructor on building software with AI coding agents. The goal is a small **business analytics dashboard** over everything the pipeline produces, kept in this repository under `dashboard/`.

- [ ] **Write the brief before the code.** Who is the user, for example an analyst who asks "should I trust today's forecast?" Which five numbers do they need? Write a one-page spec and add a `dashboard/AGENTS.md` with conventions. Agents do better work from a clear brief, just as people do.
- [ ] **Read through a safe data layer.** Create SQL views in Supabase for the dashboard, for example latest forecast, forecast vs actual, and pipeline freshness. Query them **server-side** with the read-only role from [SECURITY.md exercise 1](SECURITY.md#6-where-this-goes-next). Never ship the service-role key or the database URI to the browser.
- [ ] **Build a simple Next.js app.** Suggested panels:
  - latest close, and the next six-hour forecast as a price and a return
  - forecast vs actual over recent checkpoints
  - rolling error of the model vs the zero-return baseline, and directional hit rate
  - pipeline health: time of the last ingested bar, last run state, rows per day, bars with rescued fields
- [ ] **Review like a teammate.** Ask the agent for small changes, read every diff, run the app, and keep CI green. A PR from an agent goes through the same `protect-main` ruleset as anyone else's.

**Show at the end:** A dashboard that answers one business question end to end, from the Yahoo bar through Silver, Gold, the model and the forecast, to a number on screen.
