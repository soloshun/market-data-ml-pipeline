# GitHub security walkthrough

Open the repository's **Security and quality** tab and find **Dependabot alerts** and **Secret scanning**. In **Settings**, open **Advanced Security** to see which controls are active. GitHub can alert on known dependency vulnerabilities and detect supported secrets. Push protection can block a push containing a supported secret.

For class, open the alert list and show how a dependency alert identifies the affected package, version, and patched release. For a leaked credential, revoke or rotate it at the provider. Store the Prefect API key in GitHub Actions secrets and the Supabase URI in a Prefect Secret block.

Run the following from the repository root to audit the locked Python environment, including the notebook group:

```sh
uv audit --locked
```

This queries OSV from your machine and uses no Actions runner minutes. Dependabot only alerts on dependencies in GitHub's dependency graph. Use the local audit for the versions resolved by `uv.lock`. Review any findings and their available fixes before updating the lockfile.

[Dependabot alerts](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configure-dependabot-alerts) · [Secret scanning and push protection](https://docs.github.com/en/code-security/how-tos/secure-your-secrets/prevent-future-leaks/enable-push-protection)
