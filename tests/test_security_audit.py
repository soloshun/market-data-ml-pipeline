"""Check the rules that decide which dependency-audit waivers CI applies."""

import importlib.util
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("security_audit", ROOT / "security" / "audit.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)

TODAY = date(2026, 11, 13)


def waiver_file(tmp_path, **overrides):
    fields = {
        "id": '"GHSA-aaaa-bbbb-cccc"',
        "package": '"example"',
        "reason": '"Only affects a parser this project never calls."',
        "approved_by": '"reviewer"',
        "approved_on": "2026-11-01",
        "expires": "2027-01-15",
        "until_fixed": "false",
        **overrides,
    }
    path = tmp_path / "waivers.toml"
    path.write_text("[[waiver]]\n" + "".join(f"{k} = {v}\n" for k, v in fields.items()))
    return path


def test_committed_waiver_file_is_valid():
    audit.load_waivers(ROOT / "security" / "audit-waivers.toml", TODAY)


def test_active_waivers_become_uv_audit_flags(tmp_path):
    active, expired = audit.load_waivers(waiver_file(tmp_path), TODAY)
    assert expired == []
    assert audit.ignore_flags(active) == ["--ignore", "GHSA-aaaa-bbbb-cccc"]

    active, _ = audit.load_waivers(waiver_file(tmp_path, until_fixed="true"), TODAY)
    assert audit.ignore_flags(active) == ["--ignore-until-fixed", "GHSA-aaaa-bbbb-cccc"]


def test_expired_waiver_is_not_applied(tmp_path):
    active, expired = audit.load_waivers(waiver_file(tmp_path), date(2027, 1, 16))
    assert active == []
    assert [waiver["id"] for waiver in expired] == ["GHSA-aaaa-bbbb-cccc"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"reason": '""'}, "reason"),
        ({"reason": '"not a risk"'}, "full sentence"),
        ({"approved_by": "42"}, "approved_by"),
        ({"expires": "2027-03-01"}, "90 days"),
        ({"expires": "2026-10-01"}, "90 days"),
    ],
)
def test_incomplete_or_open_ended_waivers_are_rejected(tmp_path, overrides, message):
    with pytest.raises(ValueError, match=message):
        audit.load_waivers(waiver_file(tmp_path, **overrides), TODAY)
