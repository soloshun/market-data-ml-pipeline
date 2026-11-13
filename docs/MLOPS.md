# MLOps kickoff

The local notebook teaches ingestion with a synthetic daily fixture. The MLOps lessons use live `GC=F` gold futures bars from the hourly Prefect Cloud deployment:

- **Silver** (`market_data.hourly_prices`) holds completed one-hour bars.
- **Gold** (`market_data.hourly_features`) holds trailing returns, moving averages and volatility.
- Both tables join on `(symbol, bar_start_utc)`.

## The production rhythm we are building towards

| Every weekday at (Africa/Accra = UTC) | Flow | Status |
| --- | --- | --- |
| 00:00, 06:00, 12:00, 18:00 | `hourly-ingestion` loads completed bars into Silver and Gold | Built. The [schedule PR](DEPLOYMENT.md#1-how-a-change-reaches-production) switches it from 06:00 and 12:00 daily to this cadence |
| About 15 minutes later | A prediction flow reads the latest Gold row and forecasts the next six hours | MLOps sessions |

The 15-minute gap gives ingestion time to finish. A sturdier design triggers the prediction flow when ingestion **completes**, using a Prefect automation or a deployment trigger, instead of guessing a delay. Comparing the two designs makes a good class discussion.

## Start with a read-only query

In Supabase **SQL Editor**:

```sql
SELECT p.bar_start_utc,
       p.bar_start_utc + interval '1 hour' AS bar_end_utc,
       p.close, f.hourly_return, f.sma_5, f.sma_20, f.volatility_5
FROM market_data.hourly_prices AS p
JOIN market_data.hourly_features AS f USING (symbol, bar_start_utc)
WHERE p.symbol = 'GC=F' AND p.source = 'yfinance'
ORDER BY p.bar_start_utc DESC
LIMIT 12;
```

## What is known at a checkpoint?

Use the query to discuss **point-in-time correctness**. At the 12:00 checkpoint, the bar that started at 11:00 has closed and been ingested, so its features may be used. For a six-hour forecast, the target is the return from that close to the close of the bar ending at 18:00. Anything after 12:00 is the future and must not leak into the features.

Historical training examples need both an eligible input bar and the exact target bar. Skip examples whose target is missing because of a market closure or a feed gap. For example, gold futures pause daily and close from Friday evening to Sunday evening.

## First modelling steps

1. Build one example per checkpoint from a **fixed snapshot** of the hourly tables. Record the query, the data window and the snapshot date. The live tables can change as Yahoo revises bars.
2. Split examples **chronologically**, never randomly, and leave a six-hour gap at the split so training targets don't overlap the test period. Compare the first model against a zero-return baseline.
3. Record the feature columns, target, split, model version and test metrics. Use the same feature code at training and prediction time.

## Topics saved for the MLOps sessions

The [roadmap](ROADMAP.md) has the full plan for Lectures 9–11, covering features, XGBoost and LightGBM, tuning, MLflow, scheduled prediction and Spark.

- Experiment tracking and a model registry
- Hyperparameter tuning with time-series cross-validation
- **Data drift** (the distribution of inputs changes) and **concept drift** (the relationship between inputs and target changes). This is different from the [schema drift](SCHEMA_DRIFT.md) covered in the data engineering sessions
- Scheduled batch prediction, storing forecasts, and scoring them once the actual outcome arrives

The current code covers ingestion and feature storage. Build the training dataset and model on branches from `dev`.
