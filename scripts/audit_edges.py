#!/usr/bin/env python
"""audit_edges.py — edge precision audit tool (Phase 3, ROADMAP "50-edge audit").

Samples N relationships from the knowledge graph deterministically and exports
each sampled edge together with BOTH endpoint items, so a reviewer can judge
edge precision (>90% target) by hand.

Usage (from the repo root):

    python scripts/audit_edges.py [--n 50] [--out edges_audit.json] [--seed N]

Connection URL comes from the environment ONLY (never a CLI flag, so no
credential can end up in a process list / shell history):

    TEST_DATABASE_URL first (tests point it at a throwaway test DB),
    then DATABASE_URL (real ops use — same convention as scripts/backup.sh).

Sampling is deterministic: seeded RNG picks an OFFSET into the id-ordered
relationships table; the query is `ORDER BY id LIMIT <n> OFFSET %s` — every
value is parameterized except LIMIT, which is a literal int AFTER validation.
Same seed + same table contents => identical sample.

Output JSON: {"sampled_at", "n", "seed", "db", "edges": [...]} where each edge
is {"relationship": <full row>, "from_item": {...}, "to_item": {...}} and each
endpoint item carries id, title, content_snippet (first 500 chars), source_uri.

The tool also prints a summary (counts by type, counts by context bucket
llm-extracted vs auto vs other, average strength) and writes the JSON to --out
(default edges_audit_<UTC ts>.json in the CWD). Audit outputs are REVIEW
ARTIFACTS: they are intentionally NOT added to .gitignore (a note is printed).

Exit codes: 0 = ok, 1 = fatal (bad args, no URL, DB error). The connection URL
is never printed — only host/dbname, credentials redacted (backup.sh style).

The audit is READ-ONLY: it only ever runs SELECTs, so (like backup.sh) it
needs no test-dbname guard.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

SNIPPET_CHARS = 500
MAX_N = 1_000_000
DEFAULT_N = 50
DEFAULT_SEED = 0

USAGE = ("usage: python scripts/audit_edges.py [--n 50] [--out edges_audit.json] "
         "[--seed N]\n"
         "  reads TEST_DATABASE_URL (tests) or DATABASE_URL (ops) from the env; "
         "writes the sampled edges + endpoints as JSON and prints a summary")


# ---------------------------------------------------------------------------
# Connection URL resolution + credential-safe description (backup.sh style)
# ---------------------------------------------------------------------------


def resolve_url() -> str:
    url = (os.environ.get("TEST_DATABASE_URL") or "").strip()
    if not url:
        url = (os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        raise RuntimeError(
            "no database URL — set TEST_DATABASE_URL (tests) or DATABASE_URL "
            "(ops), e.g. postgresql://postgres@127.0.0.1:5433/secondbrain"
        )
    return url


def describe_conninfo(url: str) -> str:
    """Redacted description: host + dbname ONLY — the URL may embed credentials."""
    try:
        from psycopg import conninfo
        d = conninfo.conninfo_to_dict(url)
        return f"{d.get('host') or ''}/{d.get('dbname') or ''}"
    except Exception:
        m = re.search(r"://(?:[^@/]*@)?([^/:?]+)(?::\d+)?/([^/?]+)", url)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
        return "<unparseable>"


# ---------------------------------------------------------------------------
# Argument parsing / validation (BEFORE any DB access)
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="audit_edges.py",
        description="Sample N relationships deterministically and export edges + "
                    "both endpoints for a precision audit.",
        add_help=False,
    )
    p.add_argument("--n", type=str, default=str(DEFAULT_N),
                   help=f"sample size (int >= 1, default {DEFAULT_N})")
    p.add_argument("--out", type=str, default=None,
                   help="output JSON path (default edges_audit_<UTC ts>.json)")
    p.add_argument("--seed", type=str, default=str(DEFAULT_SEED),
                   help=f"RNG seed for the sample offset (int, default {DEFAULT_SEED})")
    args = p.parse_args(argv)

    def _int(name: str, raw: str) -> int:
        try:
            v = int(str(raw).strip())
        except (TypeError, ValueError):
            raise SystemExit(f"ERROR: --{name} must be an integer, got {raw!r}")
        return v

    args.n = _int("n", args.n)
    if args.n < 1:
        raise SystemExit(f"ERROR: --n must be >= 1, got {args.n}")
    if args.n > MAX_N:
        raise SystemExit(f"ERROR: --n must be <= {MAX_N}, got {args.n}")
    args.seed = _int("seed", args.seed)
    if args.out is not None and not str(args.out).strip():
        raise SystemExit("ERROR: --out must be a non-empty path when given")
    return args


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


def _context_bucket(context) -> str:
    """Bucket the free-text context into llm-extracted vs auto vs other."""
    if context == "llm-extracted":
        return "llm-extracted"
    if isinstance(context, str) and context.startswith("auto-linked"):
        return "auto"
    return "other"


def sample_edges(conn, n: int, seed: int) -> tuple[list[dict], int, int]:
    """Deterministically sample up to n relationship rows (ORDER BY id, seeded
    offset). Returns (rows, total, offset). LIMIT is a validated literal int;
    the offset is a parameter."""
    total = conn.execute("SELECT count(*) AS total FROM relationships").fetchone()["total"]
    rng = random.Random(seed)
    offset = rng.randrange(total) if total > 0 else 0
    if total > n:
        offset = min(offset, total - n)  # keep the sampled window full
    else:
        offset = 0
    # n is a validated int (1..MAX_N) -> safe as a literal; everything else is %s.
    rows = conn.execute(
        f"SELECT id, from_id, to_id, type, strength, direction, context, created_at "
        f"FROM relationships ORDER BY id LIMIT {int(n)} OFFSET %s",
        (offset,),
    ).fetchall()
    return [dict(r) for r in rows], int(total), offset


def fetch_endpoints(conn, item_ids: list) -> dict:
    """Fetch id, title, content snippet (500 chars), source_uri for the given ids."""
    if not item_ids:
        return {}
    rows = conn.execute(
        "SELECT id, title, content, source_uri FROM knowledge_items "
        "WHERE id = ANY(%s::uuid[])",
        ([str(i) for i in item_ids],),
    ).fetchall()
    out = {}
    for r in rows:
        out[str(r["id"])] = {
            "id": str(r["id"]),
            "title": r["title"],
            "content_snippet": (r["content"] or "")[:SNIPPET_CHARS],
            "source_uri": r["source_uri"],
        }
    return out


def build_audit(conn, n: int, seed: int, db_desc: str) -> dict:
    rel_rows, total, offset = sample_edges(conn, n, seed)
    endpoints = fetch_endpoints(conn, [r["from_id"] for r in rel_rows] +
                                [r["to_id"] for r in rel_rows])
    edges = []
    for r in rel_rows:
        rel = {k: (str(v) if k in ("id", "from_id", "to_id") else v)
               for k, v in r.items()}
        edges.append({
            "relationship": rel,
            "from_item": endpoints.get(str(r["from_id"])),
            "to_item": endpoints.get(str(r["to_id"])),
        })
    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "n": len(edges),
        "seed": seed,
        "db": db_desc,
        "edges": edges,
        "_total": total,
        "_offset": offset,
    }


def summarize(audit: dict) -> dict:
    edges = audit["edges"]
    by_type: dict[str, int] = {}
    by_context: dict[str, int] = {}
    strengths = []
    for e in edges:
        rel = e["relationship"]
        by_type[rel["type"]] = by_type.get(rel["type"], 0) + 1
        bucket = _context_bucket(rel.get("context"))
        by_context[bucket] = by_context.get(bucket, 0) + 1
        if rel.get("strength") is not None:
            strengths.append(float(rel["strength"]))
    avg = round(sum(strengths) / len(strengths), 4) if strengths else None
    return {"total": len(edges), "by_type": by_type, "by_context": by_context,
            "avg_strength": avg}


def main(argv: list[str]) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as e:
        # argparse usage errors exit 2 and our validation raises SystemExit(str);
        # both are fatal here — message to stderr, exit code 1. (--help exits 0.)
        if e.code == 0:
            raise
        if isinstance(e.code, str):
            print(e.code, file=sys.stderr)
        else:
            print("ERROR: invalid arguments (see --help)", file=sys.stderr)
        return 1

    try:
        url = resolve_url()
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    db_desc = describe_conninfo(url)

    out_path = args.out or f"edges_audit_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.json"

    try:
        conn = psycopg.connect(url, row_factory=dict_row, connect_timeout=10)
    except Exception as exc:
        print(f"ERROR: cannot connect to {db_desc} (credentials redacted): "
              f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    try:
        with conn:
            audit = build_audit(conn, args.n, args.seed, db_desc)
    except Exception as exc:
        print(f"ERROR: audit query failed on {db_desc}: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1
    finally:
        conn.close()

    summary = summarize(audit)
    total = audit.pop("_total")
    offset = audit.pop("_offset")

    payload = json.dumps(audit, indent=2, default=str, ensure_ascii=False)
    try:
        Path(out_path).write_text(payload + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"ERROR: cannot write output {out_path!r}: {exc}", file=sys.stderr)
        return 1

    print(f"audit: db      = {db_desc} (host/dbname only — credentials redacted)")
    print(f"audit: sampled = {summary['total']} edge(s) "
          f"(requested n={args.n}, seed={args.seed}, offset={offset}, total={total})")
    print(f"audit: output  = {out_path}")
    print("audit: note    = audit JSON outputs are review artifacts and are "
          "intentionally NOT added to .gitignore")
    print(f"summary: total={summary['total']}")
    print(f"summary: by_type={json.dumps(summary['by_type'], sort_keys=True)}")
    print(f"summary: by_context={json.dumps(summary['by_context'], sort_keys=True)}")
    print(f"summary: avg_strength={summary['avg_strength']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
