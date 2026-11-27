"""Tests for scripts/audit_edges.py (Phase 3 edge precision audit tool) plus
no-DB regression asserts for the pipeline/ingest.py reviewer LOW fixes
(Phase 3 batch 1 findings: outer extraction-hook catch must _sanitize;
rel_count must count only actually-inserted rows via cursor.rowcount).

Guard pattern (locked, same as tests/test_e2e.py / tests/test_extract.py):

    TEST_DATABASE_URL only (never DATABASE_URL) — apply_schema() drops the
    two tables, so it must never point at a non-test database. The dbname
    must match the delimited (^|_)test(_|$) pattern or the run REFUSES with
    exit 1 BEFORE any connection. Unset or unreachable -> "SKIP" + exit 0.

    # against the local trusted test server (127.0.0.1:5433):
    createdb edgeaudit_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/edgeaudit_test
    python tests/test_audit_edges.py

The DB tests seed 3 items + 5 relationships via direct parameterized inserts
(no embedding model needed), then run scripts/audit_edges.py as a SUBPROCESS
with TEST_DATABASE_URL in its env and check the exported JSON + printed
summary. All SQL is parameterized. The no-DB tests always run.
"""

import contextlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np  # noqa: E402
import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

import pipeline.ingest  # noqa: E402  (binds the submodule in sys.modules)
ingest_mod = sys.modules["pipeline.ingest"]  # pipeline/__init__ re-exports ingest()

REPO_ROOT = Path(__file__).resolve().parent.parent
AUDIT_SCRIPT = REPO_ROOT / "scripts" / "audit_edges.py"

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

# Seeded fixture: 3 items + 5 relationships (types/contexts/strengths chosen so
# the expected summary is exact).
SEED_ITEMS = [
    ("note", "Alpha note", "A" * 1200, "text/plain", "edgeaudit://item-1"),
    ("note", "Beta note",
     "Beta content about pgvector HNSW indexes and cosine distance.",
     "text/plain", "edgeaudit://item-2"),
    ("note", "Gamma note", "Gamma content about the audit tool itself.",
     "text/plain", "edgeaudit://item-3"),
]
# (from_idx, to_idx, type, strength, direction, context)
SEED_RELS = [
    (0, 1, "relates_to", 0.9, "one_way", "llm-extracted"),
    (1, 2, "relates_to", 0.8, "one_way", "llm-extracted"),
    (0, 2, "uses", 0.7, "one_way", "auto-linked (sim=0.7000)"),
    (1, 0, "depends_on", 0.6, "bidirectional", "auto-linked (sim=0.6000)"),
    (2, 0, "applies_to", 0.5, "one_way", "manual review note"),
]
EXPECTED_BY_TYPE = {"relates_to": 2, "uses": 1, "depends_on": 1, "applies_to": 1}
EXPECTED_BY_CONTEXT = {"llm-extracted": 2, "auto": 2, "other": 1}
EXPECTED_AVG_STRENGTH = 0.7  # (0.9+0.8+0.7+0.6+0.5)/5

REDACT_PW = "audit-secret-pw-123"


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
# Subprocess helper for the audit tool
# ---------------------------------------------------------------------------


def run_audit(url: str | None, out_path, extra_args: list[str]) -> subprocess.CompletedProcess:
    """Run scripts/audit_edges.py as a subprocess.

    url=None -> strip every DB URL from the env (no-DB arg-validation tests).
    """
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env.pop("TEST_DATABASE_URL", None)
    if url is not None:
        env["TEST_DATABASE_URL"] = url
    cmd = [sys.executable, str(AUDIT_SCRIPT), "--out", str(out_path), *extra_args]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          cwd=str(REPO_ROOT), timeout=120)


def parse_summary(stdout: str) -> dict:
    out = {}
    for line in stdout.splitlines():
        if line.startswith("summary: "):
            k, _, v = line[len("summary: "):].partition("=")
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# (a) no-DB tests: CLI arg validation (must fail BEFORE any DB access)
# ---------------------------------------------------------------------------


def test_args_invalid_n_rejected_without_db():
    for bad in ("0", "-3", "abc", "2.5", ""):
        with tempfile.TemporaryDirectory() as td:
            cp = run_audit(None, Path(td) / "out.json", ["--n", bad])
        assert cp.returncode == 1, (bad, cp.returncode, cp.stdout, cp.stderr)
        assert "--n" in cp.stderr, (bad, cp.stderr)
        # no URL was set either — the arg error must be the reported one
        assert "DATABASE_URL" not in cp.stderr, (bad, cp.stderr)


def test_args_missing_url_exit_1():
    with tempfile.TemporaryDirectory() as td:
        cp = run_audit(None, Path(td) / "out.json", ["--n", "5"])
    assert cp.returncode == 1, (cp.returncode, cp.stdout, cp.stderr)
    assert "DATABASE_URL" in cp.stderr, cp.stderr


def test_args_seed_must_be_int():
    with tempfile.TemporaryDirectory() as td:
        cp = run_audit(None, Path(td) / "out.json", ["--seed", "notanint"])
    assert cp.returncode == 1, (cp.returncode, cp.stderr)
    assert "--seed" in cp.stderr, cp.stderr


# ---------------------------------------------------------------------------
# (b) no-DB regression asserts for the pipeline/ingest.py LOW fixes
# ---------------------------------------------------------------------------


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConn:
    """Minimal stand-in for a psycopg connection (INSERTs answer with ids)."""

    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []
        self._next_id = 0

    def execute(self, sql, params=None):
        self.calls.append((sql, tuple(params or ())))
        if sql.strip().upper().startswith("INSERT INTO KNOWLEDGE_ITEMS"):
            self._next_id += 1
            return _Rows([{"id": f"fake-id-{self._next_id}"}])
        return _Rows([])


@contextlib.contextmanager
def patched_ingest_pipeline(fake_conn):
    """Make pipeline.ingest.ingest() run without a DB or embedding model."""
    dedup_mod = sys.modules["pipeline.dedup"]

    @contextlib.contextmanager
    def fake_get_connection():
        yield fake_conn

    emb = np.full(1024, 0.001, dtype=np.float32)
    with mock.patch.object(ingest_mod, "get_connection", lambda: fake_get_connection()), \
         mock.patch.object(ingest_mod, "embed_texts", lambda texts: [emb for _ in texts]), \
         mock.patch.object(ingest_mod, "embed_text", lambda text: emb), \
         mock.patch.object(dedup_mod, "embed_text", lambda text: emb):
        yield


def test_ingest_outer_hook_except_is_sanitized():
    """Reviewer LOW: the outer `except Exception` around _run_llm_extraction in
    ingest() must pass the message through pipeline.extract._sanitize before
    printing — the raw exception text may embed EXTRACT_API_KEY."""
    key = "dummy-audit-regression-key-456"
    fake_conn = FakeConn()

    def boom(text, source_uri, conn, doc_id=None):
        raise RuntimeError(f"endpoint said: Bearer {key} (key={key})")

    with mock.patch.dict(os.environ, {"EXTRACT_ENABLED": "1", "EXTRACT_API_KEY": key}), \
         mock.patch.object(ingest_mod, "_run_llm_extraction", boom), \
         patched_ingest_pipeline(fake_conn):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            res = ingest_mod.ingest("text", "tiny audit note",
                                    source_uri="edgeaudit://hook-1")
    assert res["status"] == "inserted", res  # hook failure never breaks ingest
    err = buf.getvalue()
    assert "llm extraction hook failed" in err, err
    assert key not in err, err                       # API key never printed
    assert "<redacted" in err, err                   # _sanitize ran
    assert f"Bearer {key}" not in err, err


class FakeResult:
    def __init__(self, rows=None, rowcount=-1):
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class RowcountConn:
    """Fake conn whose relationships INSERTs report scripted rowcounts
    (1 = inserted, 0 = ON CONFLICT DO NOTHING skip)."""

    def __init__(self, rel_rowcounts):
        self._rel_rowcounts = list(rel_rowcounts)
        self._n = 0

    def execute(self, sql, params=None):
        s = sql.strip().upper()
        if s.startswith("INSERT INTO KNOWLEDGE_ITEMS"):
            self._n += 1
            return FakeResult([{"id": f"ent-{self._n}"}], rowcount=1)
        if s.startswith("INSERT INTO RELATIONSHIPS"):
            rc = self._rel_rowcounts.pop(0) if self._rel_rowcounts else 1
            return FakeResult([], rowcount=rc)
        return FakeResult([], rowcount=-1)  # SELECTs find nothing new


def test_store_extracted_rel_count_uses_rowcount():
    """Reviewer LOW: rel_count must count only actually-inserted relations
    (cursor.rowcount after INSERT ... ON CONFLICT), not every attempt."""
    knowledge = {
        "entities": [{"name": "Alpha", "type": "concept"},
                     {"name": "Beta", "type": "concept"}],
        "relations": [
            {"from": "Alpha", "to": "Beta", "type": "uses", "confidence": 0.9},
            {"from": "Alpha", "to": "Beta", "type": "relates_to", "confidence": 0.4},
        ],
    }
    res = ingest_mod._store_extracted_knowledge(RowcountConn([1, 1]), knowledge, "src://a")
    assert res == {"entities": 2, "relations": 2}, res
    res = ingest_mod._store_extracted_knowledge(RowcountConn([1, 0]), knowledge, "src://a")
    assert res == {"entities": 2, "relations": 1}, res  # second was a conflict skip
    res = ingest_mod._store_extracted_knowledge(RowcountConn([0, 0]), knowledge, "src://a")
    assert res == {"entities": 2, "relations": 0}, res


NO_DB_TESTS = [
    test_args_invalid_n_rejected_without_db,
    test_args_missing_url_exit_1,
    test_args_seed_must_be_int,
    test_ingest_outer_hook_except_is_sanitized,
    test_store_extracted_rel_count_uses_rowcount,
]


# ---------------------------------------------------------------------------
# (c) guarded DB tests — own throwaway DB (edgeaudit_test), seeded directly
# ---------------------------------------------------------------------------


def seed(ctx: dict) -> None:
    url = ctx["url"]
    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")
    with psycopg.connect(url, row_factory=dict_row) as conn:
        ids = []
        for item in SEED_ITEMS:
            row = conn.execute(
                "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                " VALUES (%s, %s, %s, %s, %s) RETURNING id",
                item,
            ).fetchone()
            ids.append(row["id"])
        for from_i, to_i, rtype, strength, direction, context in SEED_RELS:
            conn.execute(
                "INSERT INTO relationships (from_id, to_id, type, strength, direction, context)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                (ids[from_i], ids[to_i], rtype, strength, direction, context),
            )
        ctx["items"] = [
            {"id": str(i), "title": t, "content": c, "source_uri": u}
            for i, (_ty, t, c, _m, u) in zip(ids, SEED_ITEMS)
        ]
        ctx["rel_ids"] = sorted(
            str(r["id"]) for r in
            conn.execute("SELECT id FROM relationships").fetchall()
        )
    assert len(ctx["items"]) == 3 and len(ctx["rel_ids"]) == 5
    print("PASS seed_items_and_relationships (3 items + 5 relationships)")


def cleanup(ctx: dict) -> None:
    with psycopg.connect(ctx["url"], row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                     ("edgeaudit://%",))  # relationships cascade
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            ("edgeaudit://%",),
        ).fetchone()["n"]
    assert remaining == 0, remaining
    print("PASS cleanup (seeded rows removed)")


def test_audit_full_export(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "audit_full.json"
        cp = run_audit(ctx["url"], out, ["--n", "50", "--seed", "7"])
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
        data = json.loads(out.read_text(encoding="utf-8"))

    assert {"sampled_at", "n", "seed", "db", "edges"} <= set(data), data.keys()
    assert data["n"] == 5 and len(data["edges"]) == 5, data["n"]
    assert data["seed"] == 7
    assert "/" in data["db"] and "@" not in data["db"], data["db"]  # redacted desc

    items_by_id = {it["id"]: it for it in ctx["items"]}
    seen_rel_ids = set()
    for edge in data["edges"]:
        rel = edge["relationship"]
        assert {"id", "from_id", "to_id", "type", "strength", "direction",
                "context", "created_at"} <= set(rel), rel
        seen_rel_ids.add(rel["id"])
        fi, ti = edge["from_item"], edge["to_item"]
        assert fi is not None and ti is not None, edge  # both endpoints resolved
        assert fi["id"] == rel["from_id"] and ti["id"] == rel["to_id"]
        for endpoint, item_id in ((fi, rel["from_id"]), (ti, rel["to_id"])):
            assert set(endpoint) == {"id", "title", "content_snippet", "source_uri"}
            exp = items_by_id[item_id]
            assert endpoint["title"] == exp["title"]
            assert endpoint["source_uri"] == exp["source_uri"]
            assert endpoint["content_snippet"] == exp["content"][:500]
            assert len(endpoint["content_snippet"]) <= 500
    assert seen_rel_ids == set(ctx["rel_ids"])  # all 5 sampled (n=50 > total=5)
    # item-1 has 1200 chars of content -> snippet must be the 500-char prefix
    alpha = next(e for e in data["edges"]
                 if e["from_item"] and e["from_item"]["source_uri"] == "edgeaudit://item-1")
    assert len(alpha["from_item"]["content_snippet"]) == 500
    # the .gitignore note is printed (audit outputs are review artifacts)
    assert ".gitignore" in cp.stdout, cp.stdout


def test_audit_n_limits_sample(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "audit_n2.json"
        cp = run_audit(ctx["url"], out, ["--n", "2", "--seed", "3"])
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
        data = json.loads(out.read_text(encoding="utf-8"))
    assert data["n"] == 2 and len(data["edges"]) == 2, data["n"]
    assert {e["relationship"]["id"] for e in data["edges"]} <= set(ctx["rel_ids"])


def test_audit_seed_reproducible(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out1 = Path(td) / "a.json"
        out2 = Path(td) / "b.json"
        cp1 = run_audit(ctx["url"], out1, ["--n", "3", "--seed", "42"])
        cp2 = run_audit(ctx["url"], out2, ["--n", "3", "--seed", "42"])
        assert cp1.returncode == 0 and cp2.returncode == 0, (cp1.stderr, cp2.stderr)
        ids1 = [e["relationship"]["id"] for e in json.loads(out1.read_text())["edges"]]
        ids2 = [e["relationship"]["id"] for e in json.loads(out2.read_text())["edges"]]
    assert ids1 == ids2, (ids1, ids2)  # same seed -> identical sample
    assert len(ids1) == 3
    assert set(ids1) <= set(ctx["rel_ids"])


def test_audit_summary_counts(ctx: dict) -> None:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "audit_summary.json"
        cp = run_audit(ctx["url"], out, ["--n", "50", "--seed", "7"])
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
    s = parse_summary(cp.stdout)
    assert s["total"] == "5", s
    assert json.loads(s["by_type"]) == EXPECTED_BY_TYPE, s
    assert json.loads(s["by_context"]) == EXPECTED_BY_CONTEXT, s
    assert abs(float(s["avg_strength"]) - EXPECTED_AVG_STRENGTH) < 1e-9, s


def test_audit_never_prints_credentials(ctx: dict) -> None:
    from psycopg import conninfo
    d = conninfo.conninfo_to_dict(ctx["url"])
    d["password"] = REDACT_PW
    pw_url = conninfo.make_conninfo(**d)  # server is trust on 127.0.0.1:5433
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "audit_redact.json"
        cp = run_audit(pw_url, out, ["--n", "1", "--seed", "0"])
        assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
        assert REDACT_PW not in cp.stdout, cp.stdout
        assert REDACT_PW not in cp.stderr, cp.stderr
        assert "credentials redacted" in cp.stdout, cp.stdout
        out_text = out.read_text(encoding="utf-8")
    assert REDACT_PW not in out_text, out_text


DB_TESTS = [
    test_audit_full_export,
    test_audit_n_limits_sample,
    test_audit_seed_reproducible,
    test_audit_summary_counts,
    test_audit_never_prints_credentials,
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
        print("SKIP db audit tests (no TEST_DATABASE_URL)")
    elif not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        print("SKIP db audit tests (test database unreachable)")
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
