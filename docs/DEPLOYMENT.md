# GitHub and Prefect deployment

`dev` is the teaching branch. Checks run on pushes and pull requests targeting `dev` or `main`. After checks pass on a push to `main`, GitHub Actions can register the exact commit as a Prefect deployment. A Process worker checks out that commit and runs it on a persistent machine.

```mermaid
flowchart LR
    A["Push to dev"] --> B["GitHub Actions checks"]
    B --> C["Pull request to main"]
    C --> D["Merge and check main"]
    D -->|Deployment enabled| E["Register the commit with Prefect Cloud"]
    E --> F["Process worker clones the registered commit"]
    F --> G["Fetch Yahoo rows and validate Silver"]
    G --> H["Commit Silver and Gold in PostgreSQL"]
```

## 1. Establish the branches

The implementation checkout has local `dev` and `main` branches. In a checkout that still has only the original `master` branch, run `git branch main master` and `git branch dev master` once. Review and commit the workshop on `dev`.

```sh
git switch dev
git status --short
git add .github .env.example .gitignore .python-version AGENTS.md README.md docs notebooks prefect.yaml pyproject.toml sql src tests uv.lock
git diff --cached --stat
git commit -m "Build the Market Data ML Pipeline workshop"
git push -u origin main
git push -u origin dev
```

On GitHub, set `main` as the default branch. Open a pull request from `dev` to `main`, watch **Verify and deploy / checks**, and merge after it passes. Future workshop changes follow the same branch and pull request sequence. The GitHub account needs repository write access and permission to configure Actions. GitHub CLI users can authenticate with `gh auth login`.

## 2. Connect Prefect Cloud

Create a Prefect Cloud workspace and obtain its workspace API URL and a key with deployment permissions. In GitHub **Settings → Secrets and variables → Actions**, add:

| Kind | Name | Value |
| --- | --- | --- |
| Repository variable | `PREFECT_API_URL` | Workspace API URL, ending in `/workspaces/<id>` |
| Repository variable | `ENABLE_PREFECT_DEPLOY` | `true`, once the pool is ready |
| Repository secret | `PREFECT_API_KEY` | The Prefect Cloud API key used for registration |

From an instructor terminal with the workshop environment installed, sign in and create a **Process** pool once:

```sh
uv run prefect cloud login
uv run prefect work-pool create --type process market-data-workshop
```

The database password belongs on the worker. GitHub Actions only needs Prefect access. Private repositories also need read-only Git authentication on the worker. Configure it for the worker account and verify it with `git ls-remote origin HEAD` from its checkout. Keep credentials out of the Git URL.

## 3. Run a worker on a persistent host

Use a machine that can reach GitHub, Prefect Cloud, and Supabase. On the host, clone the repository, check out `main`, and install its locked environment with `uv sync --locked --no-dev`. Sign in to the same workspace there with `uv run prefect cloud login`, using a key authorized for worker operations.

Create the schema in Supabase if needed and set `DATABASE_URL` in the ignored `.env` on the worker host. The deployed flow fetches Yahoo Finance rows in memory and starts persistence at Silver. Bronze is part of the local lesson.

```sh
uv run --locked --no-dev --env-file .env prefect worker start --pool market-data-workshop --limit 1
```

The Process worker uses the host's installed dependencies. When `uv.lock` changes, update the host checkout, run `uv sync --locked --no-dev`, and restart the worker. The application source for each run comes from the registered Git commit. Keep the worker running for scheduled and manual runs. Prefect Cloud handles scheduling and run history. The Process worker supplies the compute.

## 4. Verify registration and execution

After enabling the GitHub variable, push to `main` or run **Verify and deploy** on `main` using **Run workflow**. Confirm that the deployment's version is the GitHub commit SHA.

```sh
uv run prefect deployment inspect 'historical-ingestion/workshop-cloud'
uv run prefect deployment run 'historical-ingestion/workshop-cloud' --params '{"source":"fixture","incremental":false}' --watch
uv run market-data counts --source fixture
```

The command overrides the source for one rehearsal. Check that the flow completes, its log says Bronze was skipped, and both fixture counts are 30. For a manual live run, use the deployment defaults:

```sh
uv run prefect deployment run 'historical-ingestion/workshop-cloud' --watch
uv run market-data counts --source yfinance
```

The deployment defaults to live catch-up for `GC=F`. Its 06:00 Africa/Accra schedule starts paused. After the manual run succeeds, enable the schedule by changing `active: false` to `active: true` in `prefect.yaml` on `dev` and merging. Future registrations apply the schedule and parameters from YAML.

The same configuration can be exercised against a local Prefect server with `PREFECT_API_URL` set to its API. From `src/`, pass the Git clone URL and SHA as `PREFECT_CODE_REPOSITORY` and `PREFECT_CODE_REVISION`, then run `uv run --locked prefect deploy --prefect-file ../prefect.yaml --name workshop-cloud --no-prompt`.

[Prefect YAML deployments](https://www.prefect.io/docs/v3/how-to-guides/deployments/prefect-yaml) · [Prefect code retrieval](https://www.prefect.io/docs/v3/how-to-guides/deployments/store-flow-code) · [uv in GitHub Actions](https://docs.astral.sh/uv/guides/integration/github/)
