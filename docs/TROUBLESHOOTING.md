# Troubleshooting

| Symptom | What to check |
| --- | --- |
| `uv` or Python unavailable | Install uv, restart the terminal, and run `uv python install 3.12` then `uv sync --locked`. The project pins the Python minor version in `.python-version`. Run from the repository root. PowerShell uses `Copy-Item` and `$env:NAME = 'value'`. |
| PostgreSQL skipped | This is expected with `--skip-db` or a blank `DATABASE_URL`. To require a load, set the URI in `.env` and run with `--require-db`. The fixture still works offline. |
| Supabase timeout or unreachable host | Direct Supabase connections typically need IPv6. Copy the Session pooler URI from Connect for an IPv4-only network and confirm your network permits outbound PostgreSQL traffic. |
| SSL or password error | Use the PostgreSQL URI from Connect, URL-encode special characters in the password and use `sslmode=require` or `verify-full` with the correct CA. Use `sslmode=disable` only for a local loopback test server. |
| `market_data.market_prices` does not exist | Run [schema.sql](../sql/schema.sql) in SQL Editor in the same project that the URI connects to. |
| Yahoo throttles, returns an empty frame, or has a bad ticker | The fetch task retries a temporary failure or ambiguous empty response up to twice. Check the yfinance log for the ticker and window. If spot gold is unavailable, use the explicit `GC=F` gold futures example or the fixture. |
| Yahoo changed its pandas MultiIndex | The adapter supports a flat OHLCV layout and two column levels with the ticker on either level. Look at the source index and labels, then add a representative mocked frame to `tests/test_ingest.py` before updating the adapter. |
| Dates appear shifted or a window is empty | Use ISO dates such as `2025-01-06`. Daily data uses the source session date, with inclusive start and exclusive end. Live defaults exclude the current UTC date. The fixture covers weekdays from January 6 to February 14, 2025. Real instruments have their own trading calendars. |
| Moving averages or returns are null | The first return, first four 5-observation means, first nineteen 20-observation means and first five volatility values are warm-up nulls. A narrow CLI preview calculates from the fetched window, and stored Gold uses the full saved history. |
| Prefect UI keeps retrying | Check `http://127.0.0.1:4200/api/health` for `true`. Then open `http://127.0.0.1:4200/ui-settings` and check that `api_url` is `http://127.0.0.1:4200/api`. If it points to Cloud, restart the server with the [local Prefect settings](../README.md#prefect-ui-and-local-daily-schedule), then reload the dashboard. |
| Prefect UI shows no run | Set `PREFECT_API_URL` to the local URL ending in `/api` in the run terminal. Make sure a local profile or exported URL points to the intended server. Allow loopback connections if the runtime is sandboxed. |
| Prefect deployment stays scheduled or late | Keep `market-data serve` or the Process worker running against the same API as its deployment. Check the pool name, worker status, schedule status, and configured timezone. Starting a server alone provides the API and UI. |
| Cloud worker cannot clone or load the project | Check private Git access on the worker, the deployment's Git revision and pool, and whether the worker synced the current lockfile. Set `DATABASE_URL` on the worker. The Cloud deployment skips local Bronze snapshots. |
| A rerun generated another Bronze file | Bronze is append-only per snapshot. Compare row and distinct-key counts for the same symbol in PostgreSQL using [verify.sql](../sql/verify.sql). Repeated upserts refresh timestamps. |
| Catch-up missed an older gap | Run `market-data backfill` over the explicit missing range. The incremental window uses the maximum stored date with overlap. It cannot identify arbitrary older gaps. |

[Supabase connection docs](https://supabase.com/docs/guides/database/connecting-to-postgres) · [Prefect retry docs](https://www.prefect.io/docs/v3/how-to-guides/workflows/retries)
