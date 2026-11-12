# MLOps kickoff

The local notebook teaches ingestion with a synthetic daily fixture. The MLOps lesson uses live `GC=F` bars from the hourly Prefect Cloud deployment. Silver (`market_data.hourly_prices`) holds completed bars. Gold (`market_data.hourly_features`) holds trailing returns, moving averages, and volatility. The tables join on `(symbol, bar_start_utc)`.

Start in Supabase **SQL Editor** with a read-only query:

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

Use this example to discuss what is available at a prediction checkpoint. At 12:00 Accra, the bar starting at 11:00 can contribute to the features once it has closed and been ingested. For a six-hour forecast, the target is the return to the close of the bar ending at 18:00. Historical examples need both an eligible input bar and the exact target bar. Omit examples that lack the target during a market closure or data gap.

1. Build one example per chosen checkpoint from a fixed snapshot of the hourly tables. Record the query, data window, and snapshot date. The stored tables reflect the latest revisions to bars.
2. Split examples chronologically and compare the first model against a zero-return baseline. Leave room at the split for the six-hour targets to finish.
3. Record the feature columns, target, split, model version, and test metrics. Later, use the same feature definitions when generating a scheduled forecast.

The current implementation covers ingestion and feature storage. Develop the training dataset and model work on branches from `dev`. The first deployment exercise uses the dedicated schedule PR into `main`.
