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

1. Sign in to the [Supabase Dashboard](https://supabase.com/dashboard). Click **New project**, select your organization, choose a project name and region, set a database password, and click **Create new project**. Save that password privately. You'll need it to complete the connection URI.
2. When the project is ready, open **SQL Editor → New query**. Copy the full contents of [`sql/schema.sql`](../sql/schema.sql) into the editor and click **Run**. It creates `market_data.market_prices` and `market_data.market_features`.
3. In the project header, click **Connect**. In the connection string section, select **URI**, then choose **Session pooler** for a typical IPv4 worker. Choose **Direct connection** if the worker can reach the project's direct database host. Copy the connection string as shown.
4. Replace the password placeholder with the database password. Percent-encode special characters in the password, such as `@` as `%40` and `#` as `%23`. Preserve the host, port and username from **Connect**.
5. Set `sslmode=require` in the URI. Append `?sslmode=require` when there are no query parameters or `&sslmode=require` when there are existing parameters. If `sslmode` is already set, edit it in place. Paste the completed URI into the Prefect Secret block in the next section.

To check the schema, run this query in Supabase SQL Editor. Both columns should show a table name.

```sql
SELECT to_regclass('market_data.market_prices') AS silver,
       to_regclass('market_data.market_features') AS gold;
```

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

1. Open [Prefect Cloud](https://app.prefect.cloud) and select the workspace where the worker and deployment will run.
2. In the sidebar, choose **Blocks → Add Block**. Search for the built-in **Secret** block and select it to open the form. You can also get the form link by running `uv run --locked prefect block create secret` after signing in to the workspace.
3. Enter `market-data-supabase-url` as the **Block Name**. In **Value**, paste the complete `postgresql://…` URI from Supabase, with the password filled in and TLS enabled. Click **Save** or **Create**.
4. Go back to **Blocks** and check that `market-data-supabase-url` appears as a Secret in the same workspace. You can also list block names with `uv run --locked prefect block ls`.
5. Open [`prefect.yaml`](../prefect.yaml). Its `database_secret_block` parameter must match the block's name exactly. The worker uses `Secret.load(block_name).get()` when it needs to read or write Supabase. The URL is entered in the Secret form.

Teach **Secret blocks** for credentials such as a database URI. Use **Prefect Variables** for ordinary settings such as a ticker or date range. To rotate the database credential, update the block value and start a new run. [Prefect Secret block reference](https://reference.prefect.io/prefect/blocks/system/#prefect.blocks.system.Secret)

## 4. Connect GitHub Actions to Prefect

In GitHub, go to **Settings → Secrets and variables → Actions**. Use the **Variables** tab for the two variables below and **Secrets → New repository secret** for the key. After signing in with the Prefect CLI, `uv run --locked prefect config view` shows the workspace's `PREFECT_API_URL` to copy. Create the Prefect key in the Cloud UI under your profile's **API Keys**.

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

In the run log, find the latest stored date and the requested start and end dates. To see the watermark the next run will use, run this in Supabase SQL Editor:

```sql
SELECT symbol, count(*) AS rows, max(trade_date) AS latest_date
FROM market_data.market_prices
WHERE symbol = 'GC=F'
GROUP BY symbol;
```

## 7. Understand the daily catch-up

In [`prefect.yaml`](../prefect.yaml), `incremental: true` makes every default Cloud run read the last committed trade date per ticker from Supabase. The deployment requests Yahoo Finance data through yesterday UTC. An empty table starts with the prior 60 calendar days. After that, each run rereads seven calendar days ending on the last committed date, then fetches everything available after it through the same cutoff.

For example, if the worker was offline for several scheduled runs, the saved date stays where it was. When the next run starts, its later cutoff covers the missing sessions. Upserts update overlapping keys, and both tables commit together. Older holes inside already loaded history need a manual backfill. Try the [same recovery with a local fixture](../README.md#practice-catch-up-after-missed-runs).

The deployment schedules one run at **06:00 Africa/Accra** when active. It starts paused. After the manual fixture and live runs succeed, change `active: false` to `active: true` in `prefect.yaml` on `dev` and merge when you're ready to schedule it. A student's copy can change the work pool and Secret block names in the same file to match their workspace.

[Prefect YAML deployments](https://www.prefect.io/docs/v3/how-to-guides/deployments/prefect-yaml) · [Prefect code retrieval](https://www.prefect.io/docs/v3/how-to-guides/deployments/store-flow-code)
