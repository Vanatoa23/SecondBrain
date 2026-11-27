"""Tests for scripts/curator.py (Phase 4 curator core: staleness rescan,
contradiction detection, gap detection, review queue merge, soft invalidation).

Guard pattern (locked, same as tests/test_e2e.py / tests/test_audit_edges.py):

    TEST_DATABASE_URL only (never DATABASE_URL) — apply_schema() drops the
    two tables, so it must never point at a non-test database. The dbname
    must match the delimited (^|_)test(_|$) pattern or the run REFUSES with
    exit 1 BEFORE any connection. Unset or unreachable -> "SKIP" + exit 0.

    # against the local trusted test server (127.0.0.1:5433, trust auth):
    createdb curator_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/curator_test
    python tests/test_curator.py

The DB tests seed items via direct PARAMETERIZED inserts (no embedding model
needed — the curator imports only psycopg), then run scripts/curator.py as a
SUBPROCESS with TEST_DATABASE_URL in its env and a temp queue file:

  - one stale item (update_period=1, created_at 10 days ago),
  - one permanent item (update_period=-1), linked to the stale one,
  - one contradiction pair linked 'contradicts' (older has the HIGHER
    distillation_score, so created_at must decide the invalidation loser),
  - one isolated item (zero relationships),
  - one two-version source_uri (different metadata.document_hash) linked by a
    'supersedes' edge (older version flagged, never auto-invalidated).

Assertions: queue JSON carries the right (id, reason) flags as pending; the
non-dry run sets metadata.review_status="invalidated-pending-review" on the
older contradiction endpoint ONLY (content and all other columns unchanged);
--dry-run writes neither the queue nor any UPDATE; a re-run merges without
duplicating pending entries; decided (approved/dismissed) queue entries are
preserved and respected (a decided contradiction is not re-invalidated).

All SQL is parameterized. The queue file lives in a TemporaryDirectory.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
CURATOR_SCRIPT = REPO_ROOT / "scripts" / "curator.py"

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

INVALIDATED = "invalidated-pending-review"
URI_PREFIX = "curator://"


# ---------------------------------------------------------------------------
# Guard helpers (locked pattern)
# ---------------------------------------------------------------------------


def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname)."""
    try:
        from psycopg import conninfo
        d = conninfo.conninfo_to_dict(url)
        d.pop("password", None)
        host = d.get("host") or "localhost"
        port = d.get("port") or "5432"
        dbname = d.get("dbname") or ""
        return f"{host}:{port}/{dbname}"
    except Exception:
        return "<unparseable conninfo>"


def db_name(url: str) -> str:
    try:
        from psycopg import conninfo
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def db_reachable(url: str) -> bool:
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def apply_schema(url: str) -> None:
    """Idempotently apply db/schema.sql to the TEST database (drops tables)."""
    schema_path = REPO_ROOT / "db" / "schema.sql"
    schema_sql = schema_path.read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run([psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(schema_path)],
                       check=True, capture_output=True, text=True)
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)  # no params -> simple protocol, multi-statement ok


# ---------------------------------------------------------------------------
# Subprocess helper for the curator script
# ---------------------------------------------------------------------------


def run_curator(url: str | None, queue_path, extra_args: list[str]) -> subprocess.CompletedProcess:
    """Run scripts/curator.py as a subprocess.

    url=None -> strip every DB URL from the env (no-DB arg-validation tests).
    """
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env.pop("TEST_DATABASE_URL", None)
    if url is not None:
        env["TEST_DATABASE_URL"] = url
    cmd = [sys.executable, str(CURATOR_SCRIPT), "--queue", str(queue_path), *extra_args]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          cwd=str(REPO_ROOT), timeout=120)


# ---------------------------------------------------------------------------
# (a) no-DB tests: CLI validation must fail BEFORE any DB access
# ---------------------------------------------------------------------------


def test_script_missing_url_exit_1():
    with tempfile.TemporaryDirectory() as td:
        cp = run_curator(None, Path(td) / "q.json", [])
    assert cp.returncode == 1, (cp.returncode, cp.stdout, cp.stderr)
    assert "TEST_DATABASE_URL" in cp.stderr and "DATABASE_URL" in cp.stderr, cp.stderr


def test_script_empty_queue_rejected_without_db():
    # --queue "" is passed explicitly: argparse sees the empty string value
    cp = subprocess.run(
        [sys.executable, str(CURATOR_SCRIPT), "--queue", ""],
        capture_output=True, text=True,
        env={k: v for k, v in os.environ.items()
             if k not in ("DATABASE_URL", "TEST_DATABASE_URL")},
        cwd=str(REPO_ROOT), timeout=120)
    assert cp.returncode == 1, (cp.returncode, cp.stdout, cp.stderr)
    assert "--queue" in cp.stderr, cp.stderr
    # the arg error must be reported before any DB URL error
    assert "TEST_DATABASE_URL" not in cp.stderr, cp.stderr


NO_DB_TESTS = [
    test_script_missing_url_exit_1,
    test_script_empty_queue_rejected_without_db,
]


# ---------------------------------------------------------------------------
# (b) guarded DB tests — own throwaway DB (curator_test), seeded directly
# ---------------------------------------------------------------------------


def insert_item(conn, *, title, content, source_uri, update_period=-1,
                created_at=None, distillation=None, metadata=None) -> str:
    row = conn.execute(
        "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri, "
        "update_period, created_at, distillation_score, metadata) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb) RETURNING id",
        ("note", title, content, "text/plain", source_uri, update_period,
         created_at, distillation, json.dumps(metadata or {})),
    ).fetchone()
    return str(row["id"])


def insert_rel(conn, from_id, to_id, rtype, context, strength=0.9) -> None:
    conn.execute(
        "INSERT INTO relationships (from_id, to_id, type, strength, direction, context) "
        "VALUES (%s, %s, %s, %s, 'one_way', %s)",
        (from_id, to_id, rtype, strength, context),
    )


def seed(ctx: dict) -> None:
    url = ctx["url"]
    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    now = datetime.now(timezone.utc)
    d = timedelta
    with psycopg.connect(url, row_factory=dict_row) as conn:
        ids = {}
        # stale item (update_period=1s, created 10 days ago) — linked to the
        # permanent item so it is NOT flagged isolated
        ids["stale"] = insert_item(
            conn, title="Stale event note", content="curator stale content S",
            source_uri=f"{URI_PREFIX}stale", update_period=1, created_at=now - d(days=10))
        ids["permanent"] = insert_item(
            conn, title="Permanent book note", content="curator permanent content P",
            source_uri=f"{URI_PREFIX}permanent", update_period=-1, created_at=now - d(days=10))
        insert_rel(conn, ids["stale"], ids["permanent"], "relates_to", "companion notes")

        # contradiction pair — the OLDER item has the HIGHER distillation_score,
        # so created_at must decide the invalidation loser
        ids["c_old"] = insert_item(
            conn, title="Old fact", content="old fact content",
            source_uri=f"{URI_PREFIX}fact-old", update_period=-1,
            created_at=now - d(days=5), distillation=0.8)
        ids["c_new"] = insert_item(
            conn, title="New fact", content="new fact content",
            source_uri=f"{URI_PREFIX}fact-new", update_period=-1,
            created_at=now - d(days=1), distillation=0.3)
        insert_rel(conn, ids["c_old"], ids["c_new"], "contradicts",
                   "the two facts disagree on the launch date")

        # isolated item — zero relationships
        ids["isolated"] = insert_item(
            conn, title="Isolated note", content="curator isolated content I",
            source_uri=f"{URI_PREFIX}isolated", update_period=-1, created_at=now)

        # two versions of one source_uri linked by 'supersedes'
        ids["v_old"] = insert_item(
            conn, title="Doc v1", content="curator doc version 1 content",
            source_uri=f"{URI_PREFIX}doc", update_period=-1,
            created_at=now - d(days=4), metadata={"document_hash": "hash-v1"})
        ids["v_new"] = insert_item(
            conn, title="Doc v2", content="curator doc version 2 content",
            source_uri=f"{URI_PREFIX}doc", update_period=-1,
            created_at=now - d(days=2), metadata={"document_hash": "hash-v2"})
        insert_rel(conn, ids["v_new"], ids["v_old"], "supersedes", "v2 replaces v1")

        ctx["ids"] = ids
        ctx["contents_before"] = {
            str(r["id"]): r["content"] for r in conn.execute(
                "SELECT id, content FROM knowledge_items WHERE source_uri LIKE %s",
                (URI_PREFIX + "%",)).fetchall()
        }
    assert len(ctx["contents_before"]) == 7
    print("PASS seed (7 items + 3 relationships)")


def cleanup(ctx: dict) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                     (URI_PREFIX + "%",))  # relationships cascade
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
    assert remaining == 0, remaining
    print("PASS cleanup (seeded rows removed)")


def fetch_meta(conn, item_id: str) -> dict:
    row = conn.execute(
        "SELECT metadata FROM knowledge_items WHERE id = %s", (item_id,)
    ).fetchone()
    return row["metadata"] if row else {}


def expected_flags(ctx: dict) -> set[tuple[str, str]]:
    ids = ctx["ids"]
    return {
        (ids["stale"], "stale"),
        (ids["c_old"], "contradiction"),
        (ids["c_new"], "contradiction"),
        (ids["isolated"], "isolated"),
        (ids["v_old"], "superseded"),
    }


def test_dry_run_writes_nothing(ctx: dict) -> None:
    ids = ctx["ids"]
    with tempfile.TemporaryDirectory() as td:
        queue = Path(td) / "review_queue.json"
        cp = run_curator(ctx["url"], queue, ["--dry-run"])
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
        assert not queue.exists(), "dry-run must not write the queue file"
        assert "dry_run   = true" in cp.stdout, cp.stdout
        assert "would_invalidate=1" in cp.stdout, cp.stdout
        assert ids["c_old"] in cp.stdout, cp.stdout  # the would-be loser is named
        assert "queue NOT written" in cp.stdout, cp.stdout
        # no UPDATE may have run either: nobody carries a review_status
        with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
            for key in ("stale", "permanent", "c_old", "c_new", "isolated", "v_old", "v_new"):
                meta = fetch_meta(conn, ids[key])
                assert "review_status" not in meta, (key, meta)
    print("PASS test_dry_run_writes_nothing (queue + DB untouched, loser reported)")


def test_flags_and_soft_invalidation(ctx: dict) -> None:
    ids = ctx["ids"]
    queue = ctx["queue_path"]  # shared temp queue, reused by the merge tests below
    cp = run_curator(ctx["url"], queue, [])
    assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
    assert "credentials redacted" in cp.stdout, cp.stdout
    assert queue.exists(), cp.stdout
    data = json.loads(queue.read_text(encoding="utf-8"))

    assert isinstance(data.get("updated_at"), str) and data["updated_at"], data.keys()
    flagged = data["flagged"]
    got = {(str(e["id"]), e["reason"]) for e in flagged}
    assert got == expected_flags(ctx), (got, expected_flags(ctx))
    assert len(flagged) == len(got), "no duplicate (id, reason) entries"
    for e in flagged:
        assert {"id", "title", "source_uri", "reason", "evidence", "status"} <= set(e), e
        assert e["status"] == "pending", e
        assert isinstance(e["evidence"], str) and e["evidence"], e
    # the contradiction evidence carries the edge context
    c_old_entry = next(e for e in flagged if str(e["id"]) == ids["c_old"])
    assert "the two facts disagree on the launch date" in c_old_entry["evidence"], c_old_entry
    # the permanent item is not flagged at all
    assert ids["permanent"] not in {str(e["id"]) for e in flagged}

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        # invalidation: ONLY the older contradiction endpoint gets the marker
        assert fetch_meta(conn, ids["c_old"]).get("review_status") == INVALIDATED
        for key in ("stale", "permanent", "c_new", "isolated", "v_old", "v_new"):
            assert "review_status" not in fetch_meta(conn, ids[key]), key
        # content is NEVER touched by the curator
        for row in conn.execute(
                "SELECT id, content FROM knowledge_items WHERE source_uri LIKE %s",
                (URI_PREFIX + "%",)).fetchall():
            assert row["content"] == ctx["contents_before"][str(row["id"])], row["id"]
    print("PASS test_flags_and_soft_invalidation (queue flags + metadata-only marker)")


def test_rerun_merges_without_duplicates(ctx: dict) -> None:
    queue = ctx["queue_path"]
    cp = run_curator(ctx["url"], queue, [])
    assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
    data = json.loads(queue.read_text(encoding="utf-8"))
    flagged = data["flagged"]
    got = [(str(e["id"]), e["reason"]) for e in flagged]
    assert len(got) == len(set(got)), "re-run duplicated pending entries"
    assert set(got) == expected_flags(ctx), (set(got), expected_flags(ctx))
    assert all(e["status"] == "pending" for e in flagged), flagged
    print("PASS test_rerun_merges_without_duplicates (idempotent re-run)")


def test_decisions_preserved_and_respected(ctx: dict) -> None:
    ids = ctx["ids"]
    queue = ctx["queue_path"]
    # simulate human decisions in the queue file
    data = json.loads(queue.read_text(encoding="utf-8"))
    for e in data["flagged"]:
        if (str(e["id"]), e["reason"]) == (ids["stale"], "stale"):
            e["status"] = "dismissed"
        if (str(e["id"]), e["reason"]) == (ids["c_old"], "contradiction"):
            e["status"] = "approved"
    queue.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    # wipe the marker the first run set, to prove a DECIDED contradiction is
    # not re-invalidated by the next run
    with psycopg.connect(ctx["url"]) as conn:
        conn.execute(
            "UPDATE knowledge_items SET metadata = metadata - 'review_status' WHERE id = %s",
            (ids["c_old"],),
        )

    cp = run_curator(ctx["url"], queue, [])
    assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
    data2 = json.loads(queue.read_text(encoding="utf-8"))
    got = [(str(e["id"]), e["reason"]) for e in data2["flagged"]]
    assert len(got) == len(set(got)), "merge duplicated entries"
    assert set(got) == expected_flags(ctx), (set(got), expected_flags(ctx))
    by_key = {(str(e["id"]), e["reason"]): e for e in data2["flagged"]}
    assert by_key[(ids["stale"], "stale")]["status"] == "dismissed"
    assert by_key[(ids["c_old"], "contradiction")]["status"] == "approved"
    assert by_key[(ids["c_new"], "contradiction")]["status"] == "pending"
    assert by_key[(ids["isolated"], "isolated")]["status"] == "pending"

    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        # decided -> skipped: the marker must NOT come back
        assert "review_status" not in fetch_meta(conn, ids["c_old"])
    print("PASS test_decisions_preserved_and_respected (approved/dismissed kept, no re-invalidation)")


DB_TESTS = [
    test_dry_run_writes_nothing,
    test_flags_and_soft_invalidation,
    test_rerun_merges_without_duplicates,
    test_decisions_preserved_and_respected,
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def main() -> None:
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        name = db_name(url)
        if not TEST_DBNAME_RE.search(name):
            print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
                  "point at a test database (delimited 'test' component) — apply_schema() "
                  "drops the knowledge tables.")
            sys.exit(1)

    for fn in NO_DB_TESTS:
        fn()
        print(f"PASS {fn.__name__}")

    if not url:
        print(SKIP_MSG)
        print("SKIP db curator tests (no TEST_DATABASE_URL)")
    elif not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        print("SKIP db curator tests (test database unreachable)")
    else:
        with tempfile.TemporaryDirectory() as td:
            ctx = {"url": url, "queue_path": Path(td) / "review_queue.json"}
            try:
                seed(ctx)
                for fn in DB_TESTS:
                    fn(ctx)
            finally:
                cleanup(ctx)
    print("All tests passed.")


if __name__ == "__main__":
    main()
