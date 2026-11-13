# Security in a data pipeline

A pipeline has keys to things that matter: a database password, a token that can deploy code, and a copy of your data. Security work starts with three questions: **what are we protecting, from what, and how would we notice?** This guide answers them for this repository. Every control below is already in the code, so you can open the file and read it.

## 1. What we protect

| Asset | Where it lives | What happens if it leaks or is misused |
| --- | --- | --- |
| Supabase connection URI (includes the database password) | Your ignored `.env`, and a Prefect **Secret block** in Cloud | Anyone can read, change or delete your tables |
| Prefect API key | A GitHub Actions **secret** | Anyone can register or run deployments in your workspace |
| Source code and workflow files | GitHub | A malicious change could steal the secrets above at run time |
| Third-party packages | `uv.lock` | A vulnerable or hijacked package runs with your credentials |
| The data itself | Bronze Parquet, SQLite, Supabase | Wrong or tampered data quietly feeds the ML model |

## 2. Controls already in this repository

Open each file and find the line. This makes a good 15-minute scavenger hunt for a class.

### Secrets stay out of Git

| Control | Where |
| --- | --- |
| `.env` and `.env.*` are ignored. Only `.env.example`, which holds placeholders, is committed | [`.gitignore`](../.gitignore), [`.env.example`](../.env.example) |
| CI **fails** if any `.env` file is ever tracked | *Check that local environment files are untracked* in [`ci.yml`](../.github/workflows/ci.yml) |
| The Cloud flow reads the database URI from a Prefect Secret block **at run time**, so the URI never appears in code, `prefect.yaml`, or GitHub | `database_target` in [`flows/_shared.py`](../src/market_data/flows/_shared.py) |
| GitHub holds only the Prefect key, as an encrypted Actions secret. GitHub never sees the database password | [DEPLOYMENT.md §4](DEPLOYMENT.md#4-connect-github-actions-to-prefect) |

**Rule of thumb:** each system gets only the secret it needs. GitHub can deploy but can't read the database. The Prefect worker can read the database but holds no GitHub credentials.

### The CI/CD pipeline is locked down

| Control | Why it matters | Where |
| --- | --- | --- |
| `permissions: contents: read` | The workflow's own GitHub token can't push code or change settings | [`ci.yml`](../.github/workflows/ci.yml) |
| Actions pinned to a full commit SHA, for example `actions/checkout@3d3c42e…`, with the version in a comment | A tag like `@v7` can be moved to point at different code. A SHA can't | `ci.yml` |
| `persist-credentials: false` on checkout | Later steps can't reuse the checkout token | `ci.yml` |
| Deploy runs only on `main`, only after **checks** pass, and only when `ENABLE_PREFECT_DEPLOY` is `true` | Code from a branch can't reach production. Add the ruleset in §4 so that every change to `main` also needs a passing PR | `deploy` job in `ci.yml` |
| The deploy step checks the shape of `PREFECT_API_URL` before using the key | A misconfigured variable fails loudly instead of sending the key somewhere unexpected | `ci.yml` |
| PRs from forks don't receive repository secrets | Strangers can't open a PR that prints your key | GitHub's default behaviour |

### Dependencies are pinned and audited

| Control | Where |
| --- | --- |
| `uv.lock` pins every package version and hash. `uv sync --locked` refuses to run if the lock is out of date | [`uv.lock`](../uv.lock), every command in the README |
| The Prefect Managed pool installs the **same** versions. A test fails if they diverge | `pip_packages` in [`prefect.yaml`](../prefect.yaml), [`test_deployment.py`](../tests/test_deployment.py) |
| `uv audit` checks the lockfile against the OSV vulnerability database on every PR into `main` and every release. A known vulnerability **fails** the checks | *Audit locked dependencies* in `ci.yml` |
| Dependabot alerts watch the dependency graph between releases | GitHub **Security** tab (see §3) |

### The code defends itself

| Control | Where |
| --- | --- |
| **Parameterized SQL.** Values are always sent as `?` or `%s` parameters, never pasted into the SQL string. Table names are inserted with f-strings, but only from fixed constants in the code, never from user input | [`storage.py`](../src/market_data/ingestion/storage.py) |
| **TLS required.** `connect()` refuses a remote database unless `sslmode` is `require`, `verify-ca` or `verify-full` | `connect` in `storage.py` |
| **Input validation.** Tickers must match a strict pattern before they reach Yahoo, a file path or SQL | `resolve_symbols` in [`config.py`](../src/market_data/config.py) |
| **Database constraints.** Prices must be positive and finite, OHLC bounds must hold, and synthetic `XAUUSD-DEMO` rows can only carry the `fixture` source. The database enforces this even if the Python is bypassed | [`sql/schema.sql`](../sql/schema.sql) |
| **Not exposed by default.** Tables live in the `market_data` schema, which Supabase does not publish through its public Data API | Comment at the top of `schema.sql` |
| **Failure demos are opt-in.** They need fixture mode and `--skip-db`, so they can never sabotage a real load | `ingest_historical_prices` in [`historical_ingestion.py`](../src/market_data/flows/historical_ingestion.py) |

## 3. Walk through GitHub's security features

1. Open the repository's **Security** tab. Find **Dependabot alerts** and **Secret scanning**.
2. Open an alert, or the empty list, and explain the fields: affected package, vulnerable version range, patched version and severity.
3. In **Settings → Advanced Security** (called **Code security** on some plans), show which features are on. **Push protection** blocks a push that contains a recognised secret *before* it reaches GitHub.
4. Open **Actions → Verify and deploy** for a PR or a run on `main`. The run summary shows *Dependency audit passed* or *failed*. The step log names the package, advisory and fixed version.

## 4. Make the security check block the merge

A red check on its own **does not stop anyone from clicking Merge**. GitHub only enforces it when a rule on `main` requires it. This is the step most teams forget.

**UI (ClickOps):** in the repository, open **Settings → Rules → Rulesets → New ruleset → New branch ruleset**.

1. Name it `protect-main` and set **Enforcement status** to **Active**.
2. Under **Target branches**, choose **Add target → Include default branch** (`main`).
3. Tick **Require a pull request before merging**.
4. Tick **Require status checks to pass**, click **Add checks**, and select `checks`. GitHub suggests it once the workflow has run on a PR.
5. Tick **Block force pushes**, then click **Create**.

**Terminal:**

```sh
gh api -X POST repos/OWNER/REPO/rulesets --input - <<'JSON'
{
  "name": "protect-main",
  "target": "branch",
  "enforcement": "active",
  "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
  "rules": [
    {"type": "pull_request", "parameters": {"required_approving_review_count": 0,
      "dismiss_stale_reviews_on_push": false, "require_code_owner_review": false,
      "require_last_push_approval": false, "required_review_thread_resolution": false}},
    {"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": false,
      "required_status_checks": [{"context": "checks"}]}},
    {"type": "non_fast_forward"}
  ]
}
JSON
```

Now try it with the class. Open a PR that pins an old, vulnerable package version. Watch the audit fail, see the Merge button disabled, then upgrade the package and watch it turn green.

If an advisory has no fix yet and a release can't wait, add repository admins to the ruleset's **bypass list** so an admin can merge that one PR. Record the reason in the PR so the decision is visible.

Run the same audit locally before you push:

```sh
uv audit --locked --preview-features audit
```

Review the package, version, advisory and fix before you update the lockfile with `uv lock --upgrade-package <name>`. Then update the matching pin in `prefect.yaml`. `test_deployment.py` will remind you.

## 5. If a secret leaks

Deleting the commit is **not** enough. Git history, forks, clones and CI logs may keep a copy.

1. **Rotate first.** In Supabase, reset the database password. In Prefect Cloud, delete the API key and create a new one.
2. **Update the stores.** Paste the new value into the Prefect Secret block or the GitHub secret, then start a run to confirm it works.
3. **Clean up.** Remove the value from the code. If the repository is public, also rewrite history with a tool such as `git filter-repo` and ask collaborators to re-clone.
4. **Look for misuse.** Check Supabase logs and Prefect run history for activity you don't recognise.

## 6. Where this goes next

This repository shows **software composition analysis**, which means checking the packages you depend on. Larger teams add more layers:

| Practice | Example tools |
| --- | --- |
| Static analysis of your own code (SAST) | [SonarQube](https://docs.sonarsource.com/sonarqube-server/analyzing-source-code/analysis-overview), [Semgrep](https://semgrep.dev/docs/), [Bandit](https://bandit.readthedocs.io/) for Python, GitHub CodeQL |
| Dependency and license policy | [Sonatype IQ Server](https://help.sonatype.com/en/sonatype-iq-server.html), [SonarQube dependency analysis](https://docs.sonarsource.com/sonarqube-server/advanced-security/analyzing-projects-for-dependencies) |
| Secret scanning before commit | [gitleaks](https://github.com/gitleaks/gitleaks), pre-commit hooks |
| Short-lived credentials instead of stored keys | GitHub OIDC to a cloud provider, database roles with least privilege |
| Row-level access control | [Supabase Row Level Security](https://supabase.com/docs/guides/database/postgres/row-level-security) |

**Exercises**

1. Create a Supabase database role that can only `SELECT` from `market_data`. Use it for the MLOps read-only queries.
2. Add [Bandit](https://bandit.readthedocs.io/) to CI. Does it flag the f-string SQL in `storage.py`? Explain why that case is safe, and how you would record that decision in the code.
3. Write down which secret each system holds: your laptop, GitHub, Prefect Cloud and Supabase. Is there any secret that more than one system needs?

[Dependabot alerts](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configure-dependabot-alerts) · [Secret scanning and push protection](https://docs.github.com/en/code-security/how-tos/secure-your-secrets/prevent-future-leaks/enable-push-protection) · [Security hardening for GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions)
