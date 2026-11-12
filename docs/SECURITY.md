# GitHub security walkthrough

Open the repository's **Security and quality** tab and find **Dependabot alerts** and **Secret scanning**. In **Settings**, open **Advanced Security** to see which controls are active. GitHub can alert on known dependency vulnerabilities and detect supported secrets. Push protection can block a push containing a supported secret.

For class, open the alert list and show how a dependency alert identifies the affected package, version, and patched release. For a leaked credential, revoke or rotate it at the provider. Store the Prefect API key in GitHub Actions secrets and the Supabase URI in a Prefect Secret block.

The **Verify and deploy** workflow audits the locked Python environment when it runs on `main`. The Actions summary shows the outcome, and **Audit locked dependencies** in the checks job has the details. A finding or scan error adds a warning. Lint and tests continue to gate deployment. Dependabot alerts continue to monitor GitHub's dependency graph between releases.

To run the same audit locally, including the notebook dependencies:

```sh
uv audit --locked --preview-features audit
```

The audit checks versions resolved in `uv.lock` against OSV. Review the package, version, advisory, and available fixes before updating the lockfile.

This is a small example of software composition analysis. Sonatype IQ Server can evaluate dependencies for vulnerability and license policies. SonarQube analyzes source code for quality and security issues, and its dependency analysis is an additional capability. [Sonatype IQ Server](https://help.sonatype.com/en/sonatype-iq-server.html) · [SonarQube analysis](https://docs.sonarsource.com/sonarqube-server/analyzing-source-code/analysis-overview) · [SonarQube dependency analysis](https://docs.sonarsource.com/sonarqube-server/advanced-security/analyzing-projects-for-dependencies)

[Dependabot alerts](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/configure-dependabot-alerts) · [Secret scanning and push protection](https://docs.github.com/en/code-security/how-tos/secure-your-secrets/prevent-future-leaks/enable-push-protection)
