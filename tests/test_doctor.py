"""Unit tests for scripts/doctor.py (no live DB required).

Run:
    python tests/test_doctor.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import doctor  # noqa: E402


def test_sanitize_db_url_strips_credentials() -> None:
    url = "postgresql://alice:super_secret_pw@127.0.0.1:5432/secondbrain"
    label = doctor.sanitize_db_url(url)
    assert "super_secret_pw" not in label, label
    assert "alice" not in label, label
    assert label == "postgresql://127.0.0.1:5432/secondbrain", label

    assert doctor.sanitize_db_url("") == "<unset>"
    assert doctor.sanitize_db_url(None) == "<unset>"


def test_env_and_safety_checks() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)

        # 1) Default empty env warns on DATABASE_URL and mcp_scope, OK on gate
        checks = {c["name"]: c for c in doctor._check_env_and_safety({}, root)}
        assert checks["database_url"]["status"] == "WARN"
        assert checks["mcp_scope"]["status"] == "WARN"
        assert checks["external_send_gate"]["status"] == "OK"

        # 2) write scope without token FAILS
        checks = {
            c["name"]: c
            for c in doctor._check_env_and_safety(
                {"SECONDBRAIN_MCP_SCOPE": "write"}, root
            )
        }
        assert checks["mcp_scope"]["status"] == "FAIL"

        # 3) write scope with token PASSES and never leaks token
        secret = "tok_very_secret_value_12345"
        checks = {
            c["name"]: c
            for c in doctor._check_env_and_safety(
                {
                    "SECONDBRAIN_MCP_SCOPE": "write",
                    "SECONDBRAIN_WRITE_TOKEN": secret,
                    "DATABASE_URL": "postgresql://u:pw_leak_test@127.0.0.1:5432/secondbrain",
                },
                root,
            )
        }
        assert checks["mcp_scope"]["status"] == "OK"
        assert secret not in checks["mcp_scope"]["detail"]
        assert "pw_leak_test" not in checks["database_url"]["detail"]

        # 4) Non-loopback EXTRACT_API_URL without gate WARNS; with gate OK
        checks = {
            c["name"]: c
            for c in doctor._check_env_and_safety(
                {"EXTRACT_API_URL": "https://api.openai.com/v1/chat/completions"},
                root,
            )
        }
        assert checks["external_send_gate"]["status"] == "WARN"

        checks = {
            c["name"]: c
            for c in doctor._check_env_and_safety(
                {
                    "EXTRACT_API_URL": "https://api.openai.com/v1/chat/completions",
                    "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL": "1",
                },
                root,
            )
        }
        assert checks["external_send_gate"]["status"] == "OK"

        # 5) Invalid scope FAILS
        checks = {
            c["name"]: c
            for c in doctor._check_env_and_safety(
                {"SECONDBRAIN_MCP_SCOPE": "admin"}, root
            )
        }
        assert checks["mcp_scope"]["status"] == "FAIL"


def test_cli_json_no_db() -> None:
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env["SECONDBRAIN_MCP_SCOPE"] = "read"
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "doctor.py"), "--no-db", "--json"],
        capture_output=True,
        text=True,
        env=env,
        timeout=15,
        check=False,
    )
    payload = json.loads(proc.stdout)
    assert "ok" in payload
    assert "summary" in payload
    assert isinstance(payload["checks"], list) and len(payload["checks"]) >= 6
    by_name = {c["name"]: c for c in payload["checks"]}
    assert by_name["database_connection"]["status"] == "WARN"
    assert "skipped" in by_name["database_connection"]["detail"]
    assert by_name["mcp_scope"]["status"] == "OK"


def main() -> int:
    tests = [
        test_sanitize_db_url_strips_credentials,
        test_env_and_safety_checks,
        test_cli_json_no_db,
    ]
    for fn in tests:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"ALL {len(tests)} DOCTOR TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
