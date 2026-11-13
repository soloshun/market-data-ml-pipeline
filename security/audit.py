"""Run the dependency audit, applying only reviewed, unexpired waivers.

The same command runs in CI and on a laptop:
    uv run --locked python security/audit.py
"""

import os
import subprocess
import sys
import tomllib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

WAIVERS = Path(__file__).with_name("audit-waivers.toml")
MAX_WAIVER = timedelta(days=90)


class Waiver(BaseModel):
    """One reviewed exception to the dependency audit: the data contract for a waiver."""

    # strict: no silent conversions, e.g. approved_by = 42 is an error, not "42".
    # forbid: a misspelled field such as `expiry` is an error, not silently ignored.
    model_config = ConfigDict(strict=True, extra="forbid", str_strip_whitespace=True)

    id: str = Field(pattern=r"^[A-Z]+-[A-Za-z0-9-]+$", description="As uv audit prints it")
    package: str = Field(min_length=1)
    reason: str = Field(min_length=20, description="A full sentence explaining the risk")
    approved_by: str = Field(min_length=1)
    approved_on: date
    expires: date
    until_fixed: bool

    @model_validator(mode="after")
    def expires_within_90_days(self) -> "Waiver":
        """Reject waivers that never expire or last longer than the policy allows."""
        if not self.approved_on < self.expires <= self.approved_on + MAX_WAIVER:
            raise ValueError("expires must be after approved_on and within 90 days of it")
        return self


class WaiverFile(BaseModel):
    """The whole waiver file: a list of waivers with no repeated advisory IDs."""

    model_config = ConfigDict(extra="forbid")

    waiver: list[Waiver] = []

    @model_validator(mode="after")
    def unique_ids(self) -> "WaiverFile":
        """Reject a file that waives the same advisory twice."""
        ids = [waiver.id for waiver in self.waiver]
        repeated = sorted({advisory for advisory in ids if ids.count(advisory) > 1})
        if repeated:
            raise ValueError(f"each advisory may appear once, repeated: {', '.join(repeated)}")
        return self


def load_waivers(path: Path, today: date) -> tuple[list[Waiver], list[Waiver]]:
    """Read and validate the waiver file, then split it into active and expired waivers.

    Args:
        path: TOML file with a [[waiver]] table per exception.
        today: The date used to decide which waivers have expired.

    Returns:
        Active waivers and expired waivers, in file order.

    Raises:
        ValueError: A waiver breaks the contract. The message names each field at fault.
    """
    if not path.exists():
        return [], []
    try:
        waivers = WaiverFile.model_validate(tomllib.loads(path.read_text())).waiver
    except ValidationError as error:
        problems = "; ".join(
            f"{' → '.join(str(part) for part in issue['loc']) or 'file'}: {issue['msg']}"
            for issue in error.errors()
        )
        raise ValueError(f"Invalid waiver file. {problems}") from None
    active = [waiver for waiver in waivers if waiver.expires >= today]
    expired = [waiver for waiver in waivers if waiver.expires < today]
    return active, expired


def ignore_flags(waivers: list[Waiver]) -> list[str]:
    """Translate active waivers into uv audit arguments."""
    flags = []
    for waiver in waivers:
        flags += ["--ignore-until-fixed" if waiver.until_fixed else "--ignore", waiver.id]
    return flags


def write_summary(active: list[Waiver], expired: list[Waiver], passed: bool) -> None:
    """Show the audit result and every waiver in the GitHub Actions run summary."""
    lines = [f"### Dependency audit {'passed' if passed else 'failed'}", ""]
    if active:
        lines += [
            "| Waived advisory | Package | Until | Approved by | Reason |",
            "| --- " * 5 + "|",
        ]
        lines += [
            f"| {w.id} | {w.package} | {w.expires} | {w.approved_by} | {w.reason} |" for w in active
        ]
    else:
        lines.append("No waivers are active.")
    lines += [f"- Expired waiver no longer applied: {w.id} ({w.expires})" for w in expired]
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as handle:
            handle.write("\n".join(lines) + "\n")


def main() -> int:
    """Run uv audit with the active waivers and return its exit code."""
    try:
        active, expired = load_waivers(WAIVERS, datetime.now(UTC).date())
    except ValueError as error:
        print(f"::error title=Audit waivers::{error}")
        return 2
    for waiver in active:
        print(f"Waiving {waiver.id} ({waiver.package}) until {waiver.expires}.")
    for waiver in expired:
        print(f"::warning title=Expired waiver::{waiver.id} expired on {waiver.expires}.")
    command = ["uv", "audit", "--locked", "--preview-features", "audit", *ignore_flags(active)]
    sys.stdout.flush()  # Print the waiver lines before uv writes its report.
    passed = subprocess.run(command, check=False).returncode == 0
    write_summary(active, expired, passed)
    if not passed:
        print("::error title=Dependency audit::Upgrade the package or add a reviewed waiver.")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
