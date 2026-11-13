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

WAIVERS = Path(__file__).with_name("audit-waivers.toml")
MAX_WAIVER = timedelta(days=90)
FIELDS = {
    "id": str,
    "package": str,
    "reason": str,
    "approved_by": str,
    "approved_on": date,
    "expires": date,
    "until_fixed": bool,
}


def load_waivers(path: Path, today: date) -> tuple[list[dict], list[dict]]:
    """Read the waiver file and split it into active and expired waivers.

    Args:
        path: TOML file with a [[waiver]] table per exception.
        today: The date used to decide which waivers have expired.

    Returns:
        Active waivers and expired waivers, in file order.

    Raises:
        ValueError: A waiver is incomplete, repeats an ID, or lasts longer than 90 days.
    """
    if not path.exists():
        return [], []
    active, expired, seen = [], [], set()
    for number, waiver in enumerate(tomllib.loads(path.read_text()).get("waiver", []), 1):
        for field, kind in FIELDS.items():
            if not isinstance(waiver.get(field), kind) or waiver[field] in ("", None):
                raise ValueError(f"Waiver {number}: set {field} ({kind.__name__}).")
        if len(waiver["reason"].strip()) < 20:
            raise ValueError(f"Waiver {number}: explain the reason in a full sentence.")
        if not waiver["approved_on"] < waiver["expires"] <= waiver["approved_on"] + MAX_WAIVER:
            raise ValueError(f"Waiver {number}: expire within 90 days after approval.")
        if waiver["id"] in seen:
            raise ValueError(f"Waiver {number}: {waiver['id']} is listed twice.")
        seen.add(waiver["id"])
        (active if waiver["expires"] >= today else expired).append(waiver)
    return active, expired


def ignore_flags(waivers: list[dict]) -> list[str]:
    """Translate active waivers into uv audit arguments."""
    flags = []
    for waiver in waivers:
        flags += ["--ignore-until-fixed" if waiver["until_fixed"] else "--ignore", waiver["id"]]
    return flags


def write_summary(active: list[dict], expired: list[dict], passed: bool) -> None:
    """Show the audit result and every waiver in the GitHub Actions run summary."""
    lines = [f"### Dependency audit {'passed' if passed else 'failed'}", ""]
    if active:
        lines += [
            "| Waived advisory | Package | Until | Approved by | Reason |",
            "| --- " * 5 + "|",
        ]
        lines += [
            f"| {w['id']} | {w['package']} | {w['expires']} | {w['approved_by']} | {w['reason']} |"
            for w in active
        ]
    else:
        lines.append("No waivers are active.")
    lines += [f"- Expired waiver no longer applied: {w['id']} ({w['expires']})" for w in expired]
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
        print(f"Waiving {waiver['id']} ({waiver['package']}) until {waiver['expires']}.")
    for waiver in expired:
        print(f"::warning title=Expired waiver::{waiver['id']} expired on {waiver['expires']}.")
    command = ["uv", "audit", "--locked", "--preview-features", "audit", *ignore_flags(active)]
    sys.stdout.flush()  # Print the waiver lines before uv writes its report.
    passed = subprocess.run(command, check=False).returncode == 0
    write_summary(active, expired, passed)
    if not passed:
        print("::error title=Dependency audit::Upgrade the package or add a reviewed waiver.")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
