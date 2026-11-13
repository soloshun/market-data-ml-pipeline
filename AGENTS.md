# Market Data ML Pipeline workshop

- Keep the normal workflow runnable with `uv sync --locked` and `uv run market-data run`.
- Keep Polars business logic in plain functions and Prefect tasks focused on orchestration.
- Keep synthetic symbols separate from live symbols. Update SQL, tests, and the instructor guide together when contracts change.
- Run `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pytest`. Use a disposable PostgreSQL database for the optional integration tests.
- Keep credentials in ignored local files or the appropriate environment. Commit `uv.lock` and use `dev` for work before merging to `main`.
- Use Conventional Commits with a scope, for example `feat(ingestion): add hourly gold bars`.
- Keep the `pip_packages` pins in `prefect.yaml` equal to `uv.lock`. `tests/test_deployment.py` enforces this.
- Leave `chore/hourly-six-hour-schedule` untouched. The instructor uses it as the live PR-to-`main` release demo.
- Docs are student-facing. When behavior changes, update the matching guide in `docs/`, and `docs/CONCEPTS.md` if a new concept appears.
- Track planned work in `docs/ROADMAP.md`. Tick an item in the same commit that delivers it.
- Never add or extend a dependency-audit waiver in `security/audit-waivers.toml` without the instructor's explicit approval. Prefer upgrading the package.
