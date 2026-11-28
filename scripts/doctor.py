#!/usr/bin/env python3
"""Second Brain — Read-only local environment & database health check.

Usage:
    python scripts/doctor.py            # full check (env, packages, Node/MCP/Dropbox, DB)
    python scripts/doctor.py --no-db    # skip Postgres connection check
    python scripts/doctor.py --json     # machine-readable JSON report

Exit codes:
    0  all required checks passed (warnings allowed)
    1  one or more required checks failed

This script is strictly read-only: it never mutates the database or writes files,
and it never prints credentials, passwords, or API keys.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent

CORE_PACKAGES = [
    ("psycopg", "psycopg[binary]"),
    ("pgvector", "pgvector"),
    ("sentence_transformers", "sentence-transformers"),
    ("numpy", "numpy"),
    ("requests", "requests"),
    ("bs4", "beautifulsoup4"),
    ("pypdf", "pypdf"),
]

OPTIONAL_PACKAGES = [
    ("docling", "docling (rich PDF parsing; pypdf fallback works without it)"),
    ("faster_whisper", "faster-whisper (local audio transcription)"),
    ("yt_dlp", "yt-dlp (YouTube caption extraction)"),
    ("PIL", "Pillow (image handling)"),
    ("pytesseract", "pytesseract (image OCR fallback)"),
]

LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def sanitize_db_url(url: str | None) -> str:
    """Return a credential-free display label for a Postgres URL."""
    if not url or not url.strip():
        return "<unset>"
    try:
        parsed = urlparse(url.strip())
        scheme = parsed.scheme or "postgresql"
        host = parsed.hostname or "<no-host>"
        port = f":{parsed.port}" if parsed.port else ""
        dbname = parsed.path or ""
        return f"{scheme}://{host}{port}{dbname}"
    except Exception:
        return "<unparseable>"


def _is_non_loopback_url(url: str | None) -> bool:
    if not url or not url.strip():
        return False
    try:
        host = (urlparse(url.strip()).hostname or "").lower()
        return bool(host) and host not in LOOPBACK_HOSTS
    except Exception:
        return True


def _check_python_version() -> dict[str, Any]:
    ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    ok = sys.version_info >= (3, 10)
    return {
        "name": "python_version",
        "status": "OK" if ok else "FAIL",
        "detail": f"Python {ver}" + ("" if ok else " (requires >=3.10)"),
    }


def _check_packages(
    packages: list[tuple[str, str]],
    *,
    required: bool,
    name: str,
) -> dict[str, Any]:
    missing = [label for mod, label in packages if importlib.util.find_spec(mod) is None]
    if not missing:
        return {
            "name": name,
            "status": "OK",
            "detail": f"all {len(packages)} packages importable",
        }
    return {
        "name": name,
        "status": "FAIL" if required else "WARN",
        "detail": f"missing: {', '.join(missing)}",
    }


def _check_node_and_frontends(repo_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    node_bin = shutil.which("node")
    if not node_bin:
        results.append({
            "name": "node_runtime",
            "status": "WARN",
            "detail": "node not found on PATH (needed for mcp-server/ and dropbox/)",
        })
    else:
        try:
            proc = subprocess.run(
                [node_bin, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            ver = (proc.stdout or proc.stderr or "").strip()
            results.append({
                "name": "node_runtime",
                "status": "OK" if proc.returncode == 0 else "WARN",
                "detail": f"node {ver}" if ver else "node present",
            })
        except Exception as exc:
            results.append({
                "name": "node_runtime",
                "status": "WARN",
                "detail": f"failed to query node version: {type(exc).__name__}",
            })

    mcp_dist = repo_root / "mcp-server" / "dist" / "index.js"
    results.append({
        "name": "mcp_server_build",
        "status": "OK" if mcp_dist.is_file() else "WARN",
        "detail": (
            "mcp-server/dist/index.js present"
            if mcp_dist.is_file()
            else "not built yet — run: cd mcp-server && npm install && npm run build"
        ),
    })

    dropbox_nm = repo_root / "dropbox" / "node_modules"
    results.append({
        "name": "dropbox_deps",
        "status": "OK" if dropbox_nm.is_dir() else "WARN",
        "detail": (
            "dropbox/node_modules present"
            if dropbox_nm.is_dir()
            else "not installed yet — run: cd dropbox && npm install"
        ),
    })

    return results


def _check_env_and_safety(env: dict[str, str], repo_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    env_file = repo_root / ".env"
    db_url = (env.get("DATABASE_URL") or "").strip()
    if db_url:
        results.append({
            "name": "database_url",
            "status": "OK",
            "detail": f"DATABASE_URL set ({sanitize_db_url(db_url)})",
        })
    else:
        hint = "export DATABASE_URL or source .env" if env_file.is_file() else "cp .env.example .env and export DATABASE_URL"
        results.append({
            "name": "database_url",
            "status": "WARN",
            "detail": f"DATABASE_URL is unset ({hint})",
        })

    scope = (env.get("SECONDBRAIN_MCP_SCOPE") or "").strip().lower()
    if not scope:
        results.append({
            "name": "mcp_scope",
            "status": "WARN",
            "detail": "SECONDBRAIN_MCP_SCOPE is unset (legacy default exposes write tools without a token; set read or write)",
        })
    elif scope == "read":
        results.append({
            "name": "mcp_scope",
            "status": "OK",
            "detail": "SECONDBRAIN_MCP_SCOPE=read (least-privilege read-only)",
        })
    elif scope == "write":
        has_token = bool((env.get("SECONDBRAIN_WRITE_TOKEN") or "").strip())
        results.append({
            "name": "mcp_scope",
            "status": "OK" if has_token else "FAIL",
            "detail": (
                "SECONDBRAIN_MCP_SCOPE=write with SECONDBRAIN_WRITE_TOKEN set"
                if has_token
                else "SECONDBRAIN_MCP_SCOPE=write requires non-empty SECONDBRAIN_WRITE_TOKEN"
            ),
        })
    else:
        results.append({
            "name": "mcp_scope",
            "status": "FAIL",
            "detail": f"invalid SECONDBRAIN_MCP_SCOPE={scope!r} (must be 'read' or 'write')",
        })

    ext_urls = [
        name for name in ("EXTRACT_API_URL", "VISION_API_URL")
        if _is_non_loopback_url(env.get(name))
    ]
    gate_val = (env.get("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL") or "").strip().lower()
    gate_enabled = gate_val in {"1", "true", "yes", "on"}
    if ext_urls and not gate_enabled:
        results.append({
            "name": "external_send_gate",
            "status": "WARN",
            "detail": (
                f"non-loopback {', '.join(ext_urls)} configured without "
                "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL=1"
            ),
        })
    else:
        results.append({
            "name": "external_send_gate",
            "status": "OK",
            "detail": (
                "external-send approval gate enabled"
                if gate_enabled
                else "no non-loopback LLM/Vision endpoints configured"
            ),
        })

    return results


def _check_database(db_url: str) -> dict[str, Any]:
    try:
        import psycopg  # type: ignore[import-not-found]
    except ImportError:
        return {
            "name": "database_connection",
            "status": "FAIL",
            "detail": "psycopg is not installed; cannot test DB connection",
        }

    label = sanitize_db_url(db_url)
    try:
        with psycopg.connect(db_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT extname FROM pg_extension WHERE extname IN ('vector', 'pg_trgm')"
                )
                exts = {row[0] for row in cur.fetchall()}
                missing_exts = sorted({"vector", "pg_trgm"} - exts)

                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' "
                    "AND table_name IN ('knowledge_items', 'relationships')"
                )
                tables = {row[0] for row in cur.fetchall()}
                missing_tables = sorted({"knowledge_items", "relationships"} - tables)

                if missing_exts or missing_tables:
                    problems = []
                    if missing_exts:
                        problems.append(f"missing extensions: {', '.join(missing_exts)}")
                    if missing_tables:
                        problems.append(f"missing tables: {', '.join(missing_tables)}")
                    return {
                        "name": "database_connection",
                        "status": "FAIL",
                        "detail": f"{label} connected, but {'; '.join(problems)} (apply db/schema.sql)",
                    }

                cur.execute("SELECT count(*) FROM knowledge_items")
                items_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM relationships")
                rels_count = cur.fetchone()[0]

                return {
                    "name": "database_connection",
                    "status": "OK",
                    "detail": (
                        f"{label} ready (extensions: vector, pg_trgm; "
                        f"knowledge_items={items_count}, relationships={rels_count})"
                    ),
                }
    except Exception as exc:
        return {
            "name": "database_connection",
            "status": "FAIL",
            "detail": f"failed to connect to {label}: {type(exc).__name__}",
        }


def run_checks(
    *,
    env: dict[str, str] | None = None,
    repo_root: Path = REPO_ROOT,
    skip_db: bool = False,
) -> dict[str, Any]:
    """Run all read-only readiness checks and return a structured report."""
    active_env = dict(os.environ if env is None else env)
    checks: list[dict[str, Any]] = []

    checks.append(_check_python_version())
    checks.append(_check_packages(CORE_PACKAGES, required=True, name="python_core_packages"))
    checks.append(_check_packages(OPTIONAL_PACKAGES, required=False, name="python_optional_packages"))
    checks.extend(_check_node_and_frontends(repo_root))
    checks.extend(_check_env_and_safety(active_env, repo_root))

    db_url = (active_env.get("DATABASE_URL") or "").strip()
    if skip_db:
        checks.append({
            "name": "database_connection",
            "status": "WARN",
            "detail": "skipped (--no-db)",
        })
    elif not db_url:
        checks.append({
            "name": "database_connection",
            "status": "WARN",
            "detail": "skipped (DATABASE_URL not set)",
        })
    else:
        checks.append(_check_database(db_url))

    fail_count = sum(1 for c in checks if c["status"] == "FAIL")
    warn_count = sum(1 for c in checks if c["status"] == "WARN")
    ok_count = sum(1 for c in checks if c["status"] == "OK")

    return {
        "ok": fail_count == 0,
        "summary": {"ok": ok_count, "warn": warn_count, "fail": fail_count},
        "checks": checks,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only local readiness check for Second Brain.",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip connecting to PostgreSQL even if DATABASE_URL is set.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the report as JSON.",
    )
    args = parser.parse_args(argv)

    report = run_checks(skip_db=args.no_db)
    if args.as_json:
        print(json.dumps(report, indent=2))
    else:
        print("Second Brain Doctor")
        print("=" * 60)
        for check in report["checks"]:
            print(f"[{check['status']:4}] {check['name']}: {check['detail']}")
        s = report["summary"]
        print("-" * 60)
        print(f"Summary: {s['ok']} OK, {s['warn']} WARN, {s['fail']} FAIL")

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
