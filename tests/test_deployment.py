"""Guard the release configuration that CI cannot exercise without Prefect Cloud."""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

from market_data.cli import make_parser

ROOT = Path(__file__).resolve().parents[1]


def test_managed_pool_pins_match_the_lockfile():
    """The Managed pool installs prefect.yaml's pins, so they must match uv.lock."""
    deployment = yaml.safe_load((ROOT / "prefect.yaml").read_text())["deployments"][0]
    job = deployment["work_pool"]["job_variables"]
    locked = {
        package["name"]: package["version"]
        for package in tomllib.loads((ROOT / "uv.lock").read_text())["package"]
    }

    for pin in job["pip_packages"]:
        name, version = re.fullmatch(r"([\w-]+)(?:\[[\w,]+\])?==(\S+)", pin).groups()
        assert locked.get(name) == version, f"Update {pin} in prefect.yaml to match uv.lock."

    python = (ROOT / ".python-version").read_text().strip()
    assert job["image"].endswith(f"python{python}")


def test_hourly_command_accepts_one_database_mode():
    parser = make_parser()
    assert parser.parse_args(["hourly", "--require-db"]).require_db
    assert parser.parse_args(["hourly", "--skip-db"]).skip_db
    with pytest.raises(SystemExit):
        parser.parse_args(["hourly", "--skip-db", "--require-db"])
