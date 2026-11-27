"""Tests for scripts/export_wiki.py (Phase 4 wiki/Markdown export).

Guard pattern (locked, same as tests/test_e2e.py / tests/test_audit_edges.py):

    TEST_DATABASE_URL only (never DATABASE_URL) — apply_schema() drops the
    two tables, so it must never point at a non-test database. The dbname
    must match the delimited (^|_)test(_|$) pattern or the run REFUSES with
    exit 1 BEFORE any connection. Unset or unreachable -> "SKIP" + exit 0.

    # against the local trusted test server (127.0.0.1:5433):
    createdb wikiexport_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/wikiexport_test
    python tests/test_export_wiki.py

The DB tests seed 3 items + 1 relationship via direct parameterized inserts
(no embedding model needed), then run scripts/export_wiki.py as a SUBPROCESS
with TEST_DATABASE_URL in its env and a temp out dir, and assert:

  * index.md exists with 3 rows (one per item, linking to its page) and the
    stale flag (update_period vs age) is correct for a 10-day-old item with
    update_period=1s;
  * 3 item pages exist with frontmatter (id/type/source_uri/created_at/
    distillation_score/update_period/metadata) + fenced content;
  * the seeded relationship is listed as outgoing on item 1's page and as
    incoming on item 2's page, with type/strength and a page link;
  * re-running the exporter into the same dir produces byte-identical files
    (sha256 of every file before == after);
  * markdown injection: item content containing ``` fences is safely fenced
    with ~~~~ and the content survives verbatim;
  * --base-url prefixes every page link;
  * credentials never appear in stdout/stderr or any output file.

All SQL is parameterized. Seeded rows are removed in a finally block.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
EXPORT_SCRIPT = REPO_ROOT / "scripts" / "export_wiki.py"

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

# Seeded fixture: 3 items + 1 relationship. Item 2's content deliberately
# contains a ``` fence (markdown-injection probe); item 3 is 10 days old
# with update_period=1s -> stale.
INJECT_CONTENT = "Intro text\n\n```python\nprint('injected')\n```\n\noutro"
# (type, title, content, mime_type, source_uri, distillation_score, update_period, metadata_json, age_days)
SEED_ITEMS = [
    ("note", "Alpha note", "Alpha content about pgvector HNSW.", "text/plain",
     "wikiexport://item-1", 0.5, -1, '{"origin": "seed"}', 0),
    ("document", "Beta doc", INJECT_CONTENT, "text/markdown",
     "wikiexport://item-2", 0.1, -1, "{}", 0),
    ("note", "Stale note", "Old fact that has gone stale.", "text/plain",
     "wikiexport://item-3", 0.9, 1, "{}", 10),
]
SEED_REL = (0, 1, "relates_to", 0.9, "one_way", "seeded link")  # from_idx, to_idx, ...

REDACT_PW = "wiki-secret-pw-789"


# ---------------------------------------------------------------------------
# Guard helpers (locked pattern)
# ---------------------------------------------------------------------------


def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname).

    Parsed with psycopg.conninfo.conninfo_to_dict and stripped of the password:
    keyword-form conninfo has no '@' at all, so a naive split on the '@'
    separator would echo the whole conninfo — credentials included.
    """
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
# Subprocess helper for the exporter
# ---------------------------------------------------------------------------


def run_export(url: str | None, out: str, extra_args: list[str] | None = None
               ) -> subprocess.CompletedProcess:
    """Run scripts/export_wiki.py as a subprocess.

    url=None -> strip every DB URL from the env (no-DB arg-validation tests).
    """
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env.pop("TEST_DATABASE_URL", None)
    if url is not None:
        env["TEST_DATABASE_URL"] = url
    cmd = [sys.executable, str(EXPORT_SCRIPT), "--out", str(out), *(extra_args or [])]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          cwd=str(REPO_ROOT), timeout=120)


def hash_tree(root: Path) -> dict:
    """relpath -> sha256 for every file under root (idempotency check)."""
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*")) if p.is_file()
    }


# ---------------------------------------------------------------------------
# (a) no-DB tests: CLI arg validation (must fail BEFORE any DB access)
# ---------------------------------------------------------------------------


def test_missing_url_exit_1():
    with tempfile.TemporaryDirectory() as td:
        cp = run_export(None, str(Path(td) / "wiki"))
    assert cp.returncode == 1, (cp.returncode, cp.stdout, cp.stderr)
    assert "DATABASE_URL" in cp.stderr, cp.stderr


def test_empty_out_rejected_without_db():
    cp = run_export(None, "")
    assert cp.returncode == 1, (cp.returncode, cp.stdout, cp.stderr)
    assert "--out" in cp.stderr, cp.stderr
    # no URL was set either — the arg error must be the reported one
    assert "DATABASE_URL" not in cp.stderr, cp.stderr


NO_DB_TESTS = [
    test_missing_url_exit_1,
    test_empty_out_rejected_without_db,
]


# ---------------------------------------------------------------------------
# (b) guarded DB tests — own throwaway DB (wikiexport_test), seeded directly
# ---------------------------------------------------------------------------


def seed(ctx: dict) -> None:
    url = ctx["url"]
    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        ids = []
        for (typ, title, content, mime, uri, score, period, meta, days) in SEED_ITEMS:
            row = conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri,"
                " distillation_score, update_period, created_at, metadata)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s,"
                " now() - make_interval(days => %s), %s::jsonb) RETURNING id",
                (typ, title, content, mime, uri, score, period, days, meta),
            ).fetchone()
            ids.append(row["id"])
        f, t, rtype, strength, direction, context = SEED_REL
        conn.execute(
            "INSERT INTO relationships (from_id, to_id, type, strength, direction, context)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (ids[f], ids[t], rtype, strength, direction, context),
        )
        ctx["ids"] = [str(i) for i in ids]
    assert len(ctx["ids"]) == 3, ctx["ids"]
    print("PASS seed_items_and_relationship (3 items + 1 relationship)")


def cleanup(ctx: dict) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                     ("wikiexport://%",))  # relationships cascade
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            ("wikiexport://%",),
        ).fetchone()["n"]
    assert remaining == 0, remaining
    print("PASS cleanup (seeded rows removed)")


def test_export_full(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "wiki"
        cp = run_export(ctx["url"], str(out))
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)

        index = out / "index.md"
        assert index.is_file(), sorted(p.name for p in out.iterdir())
        index_text = index.read_text(encoding="utf-8")

        # 3 data rows, one per seeded item, each linking to its page
        rows = [ln for ln in index_text.splitlines() if ln.startswith("| [")]
        assert len(rows) == 3, rows
        for item_id in ctx["ids"]:
            assert f"](<{item_id}.md>)" in index_text, item_id
        for title in ("Alpha note", "Beta doc", "Stale note"):
            assert title in index_text, title
        assert "wikiexport://item-1" in index_text
        assert "summary: items=3 relationships=1 pages=3 index=1" in cp.stdout, cp.stdout

        # stale flag: item-3 (10 days old, update_period=1s) is stale, others not
        row3 = next(ln for ln in rows if f"](<{ctx['ids'][2]}.md>)" in ln)
        assert row3.endswith("| yes |"), row3
        for i in (0, 1):
            row = next(ln for ln in rows if f"](<{ctx['ids'][i]}.md>)" in ln)
            assert row.endswith("| no |"), row

        # 3 item pages with frontmatter + content + relationships
        pages = {}
        for item_id in ctx["ids"]:
            p = out / f"{item_id}.md"
            assert p.is_file(), item_id
            pages[item_id] = p.read_text(encoding="utf-8")

        p1 = pages[ctx["ids"][0]]
        assert p1.startswith("---\n"), p1[:80]
        fm = p1.split("---")[1]
        assert f'id: "{ctx["ids"][0]}"' in fm, fm
        assert 'type: "note"' in fm, fm
        assert 'source_uri: "wikiexport://item-1"' in fm, fm
        assert "created_at: " in fm and "distillation_score: 0.5" in fm, fm
        assert "update_period: -1" in fm, fm
        assert 'metadata: {"origin": "seed"}' in fm, fm
        assert "Alpha content about pgvector HNSW." in p1

        # relationship listed outgoing on item 1 -> page of item 2
        assert "### Outgoing" in p1 and "### Incoming" in p1
        assert "relates_to" in p1 and "0.9" in p1 and "seeded link" in p1
        assert f"](<{ctx['ids'][1]}.md>)" in p1

        # and incoming on item 2's page, linking back to item 1
        p2 = pages[ctx["ids"][1]]
        assert "### Incoming" in p2
        assert "relates_to" in p2 and f"](<{ctx['ids'][0]}.md>)" in p2

        # item 3 has no relationships
        p3 = pages[ctx["ids"][2]]
        assert "### Outgoing" in p3 and "_None._" in p3

        # markdown injection: content containing ``` must be fenced with ~~~~
        # (the ``` lines inside the content stay inside the wider fence)
        assert "## Content\n\n~~~~\n" in p2, p2[:400]
        assert f"~~~~\n{INJECT_CONTENT}\n~~~~" in p2, p2


def test_rerun_byte_identical(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "wiki"
        cp1 = run_export(ctx["url"], str(out))
        assert cp1.returncode == 0, (cp1.stdout, cp1.stderr)
        before = hash_tree(out)
        assert len(before) == 4, sorted(before)  # index.md + 3 item pages
        cp2 = run_export(ctx["url"], str(out))
        assert cp2.returncode == 0, (cp2.stdout, cp2.stderr)
        after = hash_tree(out)
    assert before == after, (
        f"file set changed: {set(before) ^ set(after)}; "
        f"content changed: {[k for k in before if k in after and before[k] != after[k]]}"
    )


def test_base_url_prefixes_links(ctx: dict) -> None:
    base = "https://example.org/brain/wiki/"
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "wiki"
        cp = run_export(ctx["url"], str(out), ["--base-url", base])
        assert cp.returncode == 0, (cp.stdout, cp.stderr)
        index_text = (out / "index.md").read_text(encoding="utf-8")
        for item_id in ctx["ids"]:
            assert f"](<{base}{item_id}.md>)" in index_text, item_id
        p1 = (out / f"{ctx['ids'][0]}.md").read_text(encoding="utf-8")
        assert f"](<{base}{ctx['ids'][1]}.md>)" in p1


def test_never_prints_credentials(ctx: dict) -> None:
    from psycopg import conninfo
    d = conninfo.conninfo_to_dict(ctx["url"])
    d["password"] = REDACT_PW
    pw_url = conninfo.make_conninfo(**d)  # server is trust on 127.0.0.1:5433
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "wiki"
        cp = run_export(pw_url, str(out))
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
        assert REDACT_PW not in cp.stdout, cp.stdout
        assert REDACT_PW not in cp.stderr, cp.stderr
        assert "credentials redacted" in cp.stdout, cp.stdout
        for p in out.rglob("*"):
            if p.is_file():
                assert REDACT_PW not in p.read_text(encoding="utf-8", errors="replace")


DB_TESTS = [
    test_export_full,
    test_rerun_byte_identical,
    test_base_url_prefixes_links,
    test_never_prints_credentials,
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
        print("SKIP db wiki-export tests (no TEST_DATABASE_URL)")
    elif not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        print("SKIP db wiki-export tests (test database unreachable)")
    else:
        ctx = {"url": url}
        try:
            seed(ctx)
            for fn in DB_TESTS:
                fn(ctx)
                print(f"PASS {fn.__name__}")
        finally:
            cleanup(ctx)
    print("All tests passed.")


if __name__ == "__main__":
    main()
