# Deploy the workshop with GitHub, Prefect Cloud, and Supabase

Start with the local notebook. It creates SQLite and lets you inspect Silver and Gold before setting up any cloud accounts. For deployment, GitHub Actions registers the tested code in Prefect Cloud, a Process worker executes it, and Supabase stores the tables.

```mermaid
flowchart LR
    A["Merge dev into main"] --> B["GitHub Actions runs checks"]
    B --> C["Register the tested commit in Prefect Cloud"]
    C --> D["Process worker clones that commit"]
    D --> E["Load the named Prefect Secret block"]
    E --> F["Fetch Yahoo rows and validate Silver"]
    F --> G["Commit Silver and Gold to Supabase"]
```

## 1. Prepare GitHub

Use a repository where you can push branches and configure GitHub Actions. If you are adapting the workshop, fork it or push a copy to your own repository. The workflow derives the clone URL from the repository that runs it.

Create `main` from the original branch if it does not exist, and do your changes on `dev`. In this workshop checkout those branches already exist locally. Check which branches are on your GitHub remote before pushing.

```sh
git remote -v
git branch -avv
git push -u origin main
git push -u origin dev
```

On GitHub, set `main` as the default branch. Open a pull request from `dev` into `main`. Leave it open while you prepare Supabase and Prefect. If you're using GitHub CLI and need to sign in, run `gh auth login`.

## 2. Set up Supabase

1. Create a Supabase project and open **SQL Editor**. Run the full [PostgreSQL schema](../sql/schema.sql). It creates `market_data.market_prices` and `market_data.market_features`.
2. In Supabase, click **Connect**. Copy the PostgreSQL connection URI. Choose **Session pooler** if the worker is on IPv4 or **Direct connection** if it can reach the direct host. These are database credentials for the worker.
3. Replace the password placeholder with the URL-encoded database password. Use `sslmode=require` or certificate verification. Add `?sslmode=require` if the URI has no query parameters and `&sslmode=require` if it already has some. Keep the URI handy for the next step.

[Supabase connection guide](https://supabase.com/docs/guides/database/connecting-to-postgres)

## 3. Set up Prefect Cloud

Create a workspace in Prefect Cloud. Use a separate terminal from the local Prefect lesson. If it inherited the lesson's Prefect exports, clear those before logging in.

```sh
unset PREFECT_API_URL PREFECT_API_KEY PREFECT_SERVER_UI_API_URL
uv run --locked prefect cloud login
uv run --locked prefect work-pool create --type process market-data-workshop
```

Select the workspace you will use for the deployment. If the CLI is still targeting localhost because of a local setting, export your Cloud workspace API URL in this terminal. Check the workspace in the browser when creating the block and pool.

### Create the Supabase Secret block

1. In your Prefect Cloud workspace, open **Blocks** and choose **Add Block**.
2. Search for **Secret** and choose **Add**.
3. Set the block name to `market-data-supabase-url`. Paste the complete PostgreSQL URI from Supabase into **Value**, then save the block.
4. Open [`prefect.yaml`](../prefect.yaml) and find the flow parameter `database_secret_block`. It contains the block name. At runtime the worker resolves the name using `Secret.load(block_name).get()` in the [ingestion flow](../src/market_data/flows/historical_ingestion.py).

Teach **Secret blocks** for credentials such as a database URI. Use **Prefect Variables** for ordinary settings such as a ticker or date range. To rotate the database credential, update the block value and start a new run. [Prefect Secret block reference](https://reference.prefect.io/prefect/blocks/system/#prefect.blocks.system.Secret)

## 4. Connect GitHub Actions to Prefect

In GitHub, go to **Settings → Secrets and variables → Actions**. Add the following to the repository.

| Kind | Name | Value |
| --- | --- | --- |
| Repository variable | `PREFECT_API_URL` | Prefect Cloud workspace API URL ending in `/workspaces/<id>` |
| Repository variable | `ENABLE_PREFECT_DEPLOY` | `true` |
| Repository secret | `PREFECT_API_KEY` | A Prefect Cloud key authorized to register deployments in that workspace |

GitHub uses its API key to register the deployment. The worker uses its own Prefect login to read the Supabase Secret block when the flow runs. The database URI is stored in the Prefect workspace.

## 5. Start the worker and register the deployment

Use a laptop or server that can reach GitHub, Prefect Cloud, and Supabase. On the worker machine, install the locked dependencies from a checkout containing this project, then sign in to the same Prefect Cloud workspace. Private GitHub repositories also need read access from the worker's Git account. Check that with `git ls-remote origin HEAD` in the checkout.

```sh
uv sync --locked --no-dev
uv run --locked --no-dev prefect cloud login
uv run --locked --no-dev prefect worker start --pool market-data-workshop --limit 1
```

Keep the worker terminal running. In Prefect Cloud, check that `market-data-workshop` is online. Merge the pull request into `main`. In GitHub Actions, confirm that **Verify and deploy** passed and registered the deployment. If the commit was already merged before you enabled deployment, use **Run workflow** on `main`.

The worker checks out the registered commit when a run starts. When `uv.lock` changes, update the worker's checkout, sync it again, and restart the worker.

## 6. Verify one fixture run and one live run

Run these commands from a terminal pointed at the Cloud workspace.

```sh
uv run --locked prefect deployment inspect 'historical-ingestion/workshop-cloud'
uv run --locked prefect deployment run 'historical-ingestion/workshop-cloud' \
  --params '{"source":"fixture","incremental":false}' --watch
```

Check that the deployment version matches the GitHub commit SHA. The run should log a commit to PostgreSQL. In Supabase SQL Editor, run [the fixture verification queries](../sql/verify.sql) and confirm both tables have 30 fixture rows and 30 distinct keys. The worker skips local Bronze snapshots.

Then try the deployment defaults, which catch up live Yahoo Finance data for `GC=F`.

```sh
uv run --locked prefect deployment run 'historical-ingestion/workshop-cloud' --watch
```

```sql
SELECT symbol, count(*) AS rows, max(trade_date) AS latest_date
FROM market_data.market_prices
WHERE symbol = 'GC=F'
GROUP BY symbol;
```

The 06:00 Africa/Accra schedule starts paused. After both runs succeed, change `active: false` to `active: true` in `prefect.yaml` on `dev` and merge. A student's copy can change the work pool and Secret block names in the same file to match their workspace.

[Prefect YAML deployments](https://www.prefect.io/docs/v3/how-to-guides/deployments/prefect-yaml) · [Prefect code retrieval](https://www.prefect.io/docs/v3/how-to-guides/deployments/store-flow-code)
