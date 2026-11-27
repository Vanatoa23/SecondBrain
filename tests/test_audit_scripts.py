"""Tests for the W3 audit fixes across bridge.py, pipeline/extractors.py and
scripts/ (export_chat, restore.sh, audit_edges, sync_notes).

Covers (external audit findings, worker W3):

  1. bridge review_decision — updates the repo-root review_queue.json entry
     (keyed by id+reason) AND the DB: "approved" rewrites an existing
     "invalidated*" metadata.review_status to "invalidated-approved" (no-op
     otherwise); "dismissed" removes the review_status key. ValueError
     envelopes on missing queue / no matching entry / bad input.
  2. path confinement — a file in the PLAIN system temp dir is REFUSED, one in
     <tempdir>/secondbrain-staging is ACCEPTED.
  3. store_raw for a pdf path stores the file BYTES (raw/<2>/<sha> content ==
     the file bytes).
  4. export_chat parse_export fixtures — ChatGPT "mapping", Claude
     "chat_messages", and the legacy "messages" shape all parse; unknown
     shapes raise a clear ValueError.
  5. restore.sh source contains --single-transaction, --exit-on-error and
     python3 (with a python fallback).
  6. audit_edges sample_edges SQL uses ORDER BY md5(...) with the seed as a
     parameter and no OFFSET.
  7. sync_notes find_md_files skips .obsidian / .trash / .git directories.
  8. extract_url pinned resolution — socket.getaddrinfo is consulted exactly
     once per HTTP request (the guard) and the fetch dials ONLY the pinned
     IP; a simulated DNS rebind (public -> 127.0.0.1) is never dialed.

Guard pattern (locked — same invariant as tests/test_bridge.py):

  * DB access ONLY via TEST_DATABASE_URL, and only when its dbname matches
    (^|_)test(_|$) case-insensitively — otherwise the script prints REFUSING
    and exits 1 BEFORE any connection. Unset or unreachable -> the DB tests
    print SKIP and the script exits 0 (the no-DB tests always run).
  * DATABASE_URL is set to the guarded test URL only for spawned bridge
    subprocesses (bridge.py reads DATABASE_URL).
  * The DB tests use this suite's own rows only (source_uri LIKE
    'test://auditw3/%'), deleted in a finally block (parameterized), so an
    abort leaves nothing behind.

Run from the repo root:

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/audit_w3_test \
        python tests/test_audit_scripts.py
"""

from __future__ import annotations

import importlib.util
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)
URI_PREFIX = "test://auditw3/"

# A minimal valid one-page PDF (pypdf can extract "Hello W3" from it).
MINI_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 44>>stream\n"
    b"BT /F1 24 Tf 100 700 Td (Hello W3) Tj ET\n"
    b"endstream\nendobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"trailer<</Root 1 0 R>>\n"
)


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# guard helpers (test_bridge.py pattern)
# ---------------------------------------------------------------------------

def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL.
    return os.environ.get("TEST_DATABASE_URL")


def db_name(url: str) -> str:
    try:
        from psycopg import conninfo
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def is_test_dbname(name: str) -> bool:
    return bool(TEST_DBNAME_RE.search(name or ""))


def db_reachable(url: str) -> bool:
    try:
        import psycopg
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
        return True
    except Exception:
        return False


def run_bridge(command, args, url=None, timeout=600, extra_env=None):
    """Run one bridge command as a subprocess; assert the one-JSON-line
    envelope contract; return the envelope."""
    env = {k: v for k, v in os.environ.items()
           if k not in ("DATABASE_URL", "TEST_DATABASE_URL", "BRIDGE_PATH_ALLOWLIST")}
    if url:
        env["DATABASE_URL"] = url  # bridge.py reads DATABASE_URL
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", command, json.dumps(args)],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=env,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, (
        f"expected exactly 1 stdout line, got {len(lines)}: {proc.stdout[:400]!r}"
    )
    env_json = json.loads(lines[0])
    assert isinstance(env_json, dict) and isinstance(env_json.get("ok"), bool), env_json
    if env_json["ok"]:
        assert proc.returncode == 0, (proc.returncode, env_json)
    else:
        assert isinstance(env_json.get("error"), str) and env_json["error"], env_json
        assert proc.returncode == 1, (proc.returncode, env_json)
    return env_json


def _db():
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(os.environ["TEST_DATABASE_URL"], row_factory=dict_row)


def cleanup_test_rows() -> None:
    with _db() as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                     (URI_PREFIX + "%",))


# ---------------------------------------------------------------------------
# 1. review_decision — queue + DB (needs the guarded test DB)
# ---------------------------------------------------------------------------

QUEUE_PATH = ROOT / "review_queue.json"


def _seed_item(conn, review_status: str | None) -> str:
    meta = {} if review_status is None else {"review_status": review_status}
    row = conn.execute(
        """
        INSERT INTO knowledge_items (type, title, content, source_uri, metadata)
        VALUES (%s, %s, %s, %s, %s::jsonb) RETURNING id
        """,
        ("note", "audit w3 review item", f"audit w3 review content {uuid.uuid4().hex}",
         URI_PREFIX + uuid.uuid4().hex, json.dumps(meta)),
    ).fetchone()
    return str(row["id"])


def _write_queue(entries: list[dict]) -> None:
    QUEUE_PATH.write_text(
        json.dumps({"updated_at": None, "flagged": entries}, indent=2) + "\n",
        encoding="utf-8",
    )


def _read_queue() -> dict:
    return json.loads(QUEUE_PATH.read_text(encoding="utf-8"))


def test_review_decision_queue_and_db():
    url = os.environ["TEST_DATABASE_URL"]
    item_id = None
    backup = QUEUE_PATH.read_bytes() if QUEUE_PATH.exists() else None
    try:
        with _db() as conn:
            item_id = _seed_item(conn, "invalidated-pending-review")
        # (the with-block commits the seed before the bridge subprocess runs)
        _write_queue([{
            "id": item_id, "title": "audit w3 review item",
            "source_uri": URI_PREFIX, "reason": "contradiction",
            "evidence": "edge ctx", "status": "pending",
        }])

        # --- approved: invalidated* marker -> invalidated-approved ---
        env = run_bridge("review_decision", {
            "id": item_id, "reason": "contradiction", "decision": "approved",
        }, url=url)
        assert env["ok"] is True, env
        assert env["result"]["queue_updated"] is True, env["result"]
        assert env["result"]["db_updated"] == 1, env["result"]
        q = _read_queue()
        entry = [e for e in q["flagged"] if e["id"] == item_id][0]
        assert entry["status"] == "approved", entry
        assert entry.get("updated_at"), entry
        assert q.get("updated_at"), q
        with _db() as conn:
            rs = conn.execute(
                "SELECT metadata->>'review_status' AS rs FROM knowledge_items WHERE id = %s",
                (item_id,),
            ).fetchone()["rs"]
        assert rs == "invalidated-approved", rs

        # --- approved on a NON-invalidated marker is a DB no-op ---
        with _db() as conn:
            conn.execute(
                "UPDATE knowledge_items SET metadata = %s::jsonb WHERE id = %s",
                (json.dumps({"review_status": "pending-review"}), item_id),
            )
        env = run_bridge("review_decision", {
            "id": item_id, "reason": "contradiction", "decision": "approved",
        }, url=url)
        assert env["ok"] is True and env["result"]["db_updated"] == 0, env
        with _db() as conn:
            rs = conn.execute(
                "SELECT metadata->>'review_status' AS rs FROM knowledge_items WHERE id = %s",
                (item_id,),
            ).fetchone()["rs"]
        assert rs == "pending-review", rs  # untouched
        # queue still updated (the decision itself is recorded)
        assert [e for e in _read_queue()["flagged"] if e["id"] == item_id][0]["status"] == "approved"

        # --- dismissed: review_status key removed from metadata ---
        with _db() as conn:
            conn.execute(
                "UPDATE knowledge_items SET metadata = %s::jsonb WHERE id = %s",
                (json.dumps({"review_status": "invalidated-pending-review", "keep": 1}), item_id),
            )
        env = run_bridge("review_decision", {
            "id": item_id, "reason": "contradiction", "decision": "dismissed",
        }, url=url)
        assert env["ok"] is True and env["result"]["db_updated"] == 1, env
        with _db() as conn:
            row = conn.execute(
                "SELECT metadata FROM knowledge_items WHERE id = %s", (item_id,)
            ).fetchone()
        assert "review_status" not in (row["metadata"] or {}), row["metadata"]
        assert row["metadata"].get("keep") == 1, row["metadata"]  # rest of jsonb intact
        assert [e for e in _read_queue()["flagged"] if e["id"] == item_id][0]["status"] == "dismissed"

        # --- error envelopes ---
        # bad decision value
        env = run_bridge("review_decision", {
            "id": item_id, "reason": "contradiction", "decision": "bogus",
        }, url=url)
        assert env["ok"] is False and "decision" in env["error"], env
        # no matching entry (unknown id)
        env = run_bridge("review_decision", {
            "id": str(uuid.uuid4()), "reason": "contradiction", "decision": "approved",
        }, url=url)
        assert env["ok"] is False and "no review queue entry" in env["error"], env
        # missing queue file
        QUEUE_PATH.unlink()
        env = run_bridge("review_decision", {
            "id": item_id, "reason": "contradiction", "decision": "approved",
        }, url=url)
        assert env["ok"] is False and "review queue not found" in env["error"], env
    finally:
        if backup is not None:
            QUEUE_PATH.write_bytes(backup)
        elif QUEUE_PATH.exists():
            QUEUE_PATH.unlink()  # we created the fixture — remove it again
        try:
            cleanup_test_rows()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 2. path confinement — plain tempdir REFUSED, staging dir ACCEPTED (no DB)
# ---------------------------------------------------------------------------

def test_path_confinement_staging_only():
    fd, plain = tempfile.mkstemp(prefix="w3_plain_", suffix=".pdf")
    os.close(fd)
    staging = os.path.join(tempfile.gettempdir(), "secondbrain-staging")
    os.makedirs(staging, exist_ok=True)
    staged = os.path.join(staging, f"w3_staged_{uuid.uuid4().hex[:8]}.pdf")
    try:
        with open(plain, "wb") as fh:
            fh.write(MINI_PDF)
        with open(staged, "wb") as fh:
            fh.write(MINI_PDF)

        # plain temp dir -> refused (no DATABASE_URL needed: refusal precedes DB)
        env = run_bridge("ingest", {"source_type": "pdf", "payload": plain})
        assert env["ok"] is False and "outside allowed roots" in env["error"], env

        # staging dir -> accepted: the error must NOT be the confinement refusal
        env = run_bridge("ingest", {"source_type": "pdf", "payload": staged})
        assert env["ok"] is False, env  # fails later (no DB here) ...
        assert "outside allowed roots" not in env["error"], env["error"]  # ... but passed the path check
    finally:
        os.unlink(plain)
        os.unlink(staged)


# ---------------------------------------------------------------------------
# 3. store_raw for a pdf path stores the file BYTES (needs the test DB + model)
# ---------------------------------------------------------------------------

def test_store_raw_pdf_stores_file_bytes():
    url = os.environ["TEST_DATABASE_URL"]
    staging = os.path.join(tempfile.gettempdir(), "secondbrain-staging")
    os.makedirs(staging, exist_ok=True)
    pdf_path = os.path.join(staging, f"w3_raw_{uuid.uuid4().hex[:8]}.pdf")
    raw_obj = None
    try:
        with open(pdf_path, "wb") as fh:
            fh.write(MINI_PDF)
        env = run_bridge("ingest", {
            "source_type": "pdf", "payload": pdf_path, "store_raw": True,
        }, url=url, timeout=900)
        assert env["ok"] is True, env
        res = env["result"]
        assert res["status"] in ("inserted", "duplicate"), res
        sha = res.get("raw_sha256")
        assert sha and re.fullmatch(r"[0-9a-f]{64}", sha), res
        raw_obj = ROOT / "raw" / sha[:2] / sha
        assert raw_obj.is_file(), raw_obj
        assert raw_obj.read_bytes() == MINI_PDF, "raw store must hold the file BYTES"
    finally:
        if raw_obj is not None and raw_obj.exists():
            raw_obj.unlink()
        try:
            with _db() as conn:
                conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                             (os.path.join(tempfile.gettempdir(), "secondbrain-staging",
                                           "w3_raw_%"),))
        except Exception:
            pass
        if os.path.exists(pdf_path):
            os.unlink(pdf_path)


# ---------------------------------------------------------------------------
# 4. export_chat fixtures — ChatGPT mapping + Claude chat_messages + legacy
# ---------------------------------------------------------------------------

def test_export_chat_real_format_fixtures():
    export_chat = _load_script("export_chat")

    chatgpt = [{
        "title": "Borrow checker chat",
        "mapping": {
            "n0": {"message": None, "parent": None, "children": ["n1"]},
            "n1": {"message": {"author": {"role": "user"},
                              "content": {"parts": ["Why does this fail?"]}},
                   "parent": "n0", "children": ["n2"]},
            "n2": {"message": {"author": {"role": "assistant"},
                              "content": {"parts": ["Because ",
                                                    {"type": "text", "text": "it moves."}]}},
                   "parent": "n1", "children": ["n3"]},
            "n3": {"message": {"author": {"role": "system"}, "content": {"parts": ["sys"]}},
                   "parent": "n2", "children": []},
        },
    }]
    claude = [{
        "name": "Tea chat", "uuid": "u-1",
        "chat_messages": [
            {"sender": "human", "text": "How hot for green tea?"},
            {"sender": "assistant", "text": "About 80C."},
            {"sender": "human", "text": ""},
        ],
    }]
    legacy = {"conversations": [
        {"title": "Legacy chat", "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": [{"type": "text", "text": "hello"}]},
        ]},
    ]}
    unknown = [{"something": "else"}]

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "gpt.json"
        p.write_text(json.dumps(chatgpt), encoding="utf-8")
        convs = export_chat.parse_export(p)
        assert len(convs) == 1 and convs[0]["title"] == "Borrow checker chat", convs
        assert convs[0]["n_messages"] == 2, convs  # system message skipped
        assert "user: Why does this fail?" in convs[0]["text"], convs[0]["text"]
        assert "assistant: Because it moves." in convs[0]["text"], convs[0]["text"]

        p = Path(td) / "claude.json"
        p.write_text(json.dumps(claude), encoding="utf-8")
        convs = export_chat.parse_export(p)
        assert convs[0]["title"] == "Tea chat", convs
        assert convs[0]["n_messages"] == 2, convs  # empty human message skipped
        assert "user: How hot for green tea?" in convs[0]["text"], convs[0]["text"]
        assert "assistant: About 80C." in convs[0]["text"], convs[0]["text"]

        p = Path(td) / "legacy.json"
        p.write_text(json.dumps(legacy), encoding="utf-8")
        convs = export_chat.parse_export(p)
        assert convs[0]["title"] == "Legacy chat" and convs[0]["n_messages"] == 2, convs
        assert "assistant: hello" in convs[0]["text"], convs[0]["text"]

        p = Path(td) / "unknown.json"
        p.write_text(json.dumps(unknown), encoding="utf-8")
        try:
            export_chat.parse_export(p)
        except ValueError as e:
            assert "unsupported shape" in str(e), e
        else:
            raise AssertionError("unknown shape did not raise ValueError")


# ---------------------------------------------------------------------------
# 5. restore.sh — single-transaction + exit-on-error + python3 (no DB)
# ---------------------------------------------------------------------------

def test_restore_sh_source_flags():
    src = (ROOT / "scripts" / "restore.sh").read_text(encoding="utf-8")
    assert "--single-transaction" in src, "restore.sh must pass --single-transaction"
    assert "--exit-on-error" in src, "restore.sh must pass --exit-on-error"
    assert "python3" in src, "restore.sh must prefer python3"
    assert re.search(r"command -v python3", src), "python3 probe missing"
    assert re.search(r"PY=python\b", src), "python fallback missing"
    # the pg_restore invocation itself carries both flags
    m = re.search(r"^pg_restore .*$", src, re.MULTILINE)
    assert m and "--single-transaction" in m.group(0) and "--exit-on-error" in m.group(0), m


# ---------------------------------------------------------------------------
# 6. audit_edges sample_edges — md5 ORDER BY, seed parameter, no OFFSET
# ---------------------------------------------------------------------------

def test_audit_edges_md5_sample_sql():
    audit_edges = _load_script("audit_edges")
    import inspect
    src = inspect.getsource(audit_edges.sample_edges)
    assert "md5(" in src and "ORDER BY md5(" in src, src
    assert "OFFSET" not in src.upper(), src
    assert "%s" in src, src  # seed is a parameter

    # functional: a fake connection records the SQL actually executed
    class _Cur:
        def __init__(self, rows):
            self._rows = rows
        def fetchone(self):
            return {"total": 100}
        def fetchall(self):
            return self._rows

    class _Conn:
        def __init__(self):
            self.queries = []
        def execute(self, sql, params=None):
            self.queries.append((sql, params))
            return _Cur([{"id": uuid.uuid4(), "from_id": uuid.uuid4(),
                          "to_id": uuid.uuid4(), "type": "related",
                          "strength": 0.5, "direction": "one_way",
                          "context": None, "created_at": None}])

    conn = _Conn()
    rows, total = audit_edges.sample_edges(conn, 5, 42)
    assert total == 100 and len(rows) == 1, (total, rows)
    sql, params = conn.queries[-1]
    assert "md5(id::text || ':' || %s)" in sql, sql
    assert "OFFSET" not in sql.upper(), sql
    assert "LIMIT 5" in sql, sql
    assert params == ("42",), params
    # deterministic per seed: same call -> same SQL/params
    conn2 = _Conn()
    audit_edges.sample_edges(conn2, 5, 42)
    assert conn2.queries[-1] == (sql, params)


# ---------------------------------------------------------------------------
# 7. sync_notes skips VCS/hidden dirs (no DB)
# ---------------------------------------------------------------------------

def test_sync_notes_skips_hidden_dirs():
    sync_notes = _load_script("sync_notes")
    with tempfile.TemporaryDirectory() as td:
        vault = Path(td)
        (vault / "a.md").write_text("note a", encoding="utf-8")
        (vault / "sub").mkdir()
        (vault / "sub" / "b.md").write_text("note b", encoding="utf-8")
        for hidden in (".obsidian", ".trash", ".git"):
            d = vault / hidden
            d.mkdir()
            (d / "x.md").write_text("hidden", encoding="utf-8")
        (vault / ".obsidian" / "nested").mkdir()
        (vault / ".obsidian" / "nested" / "y.md").write_text("hidden nested", encoding="utf-8")
        found = {p.relative_to(vault).as_posix() for p in sync_notes.find_md_files(vault)}
        assert found == {"a.md", "sub/b.md"}, found


# ---------------------------------------------------------------------------
# 8. extract_url pinned resolution — 1 DNS resolution per request, rebind refused
# ---------------------------------------------------------------------------

def test_extract_url_pinned_resolution():
    from pipeline import extractors

    PUBLIC = "93.184.216.34"
    real_getaddrinfo = socket.getaddrinfo
    calls: list[str] = []

    def rebinding_dns(host, *args, **kwargs):
        calls.append(host)
        if host == "pinned.example.com":
            n = len([c for c in calls if c == host])
            # first resolution: public; every later one: REBIND to loopback
            ip = PUBLIC if n == 1 else "127.0.0.1"
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443))]
        return real_getaddrinfo(host, *args, **kwargs)

    dialed: list = []

    class _Stop(Exception):
        pass

    def fake_connect(self, address):
        dialed.append(address)
        raise _Stop("no real dial in tests")

    with mock.patch("socket.getaddrinfo", side_effect=rebinding_dns), \
         mock.patch("socket.socket.connect", fake_connect):
        try:
            extractors.extract_url("https://pinned.example.com/page")
        except Exception:
            pass  # the aborted dial — what matters is what was dialed

    def _is_ip_literal(h) -> bool:
        try:
            ipaddress.ip_address(h)
            return True
        except ValueError:
            return False

    hostname_resolutions = [c for c in calls if not _is_ip_literal(c)]
    # exactly ONE resolution of the hostname per HTTP request (the guard);
    # the fetch never re-resolves it.
    assert hostname_resolutions == ["pinned.example.com"], calls
    # the request dialed ONLY the pinned (validated) IP — never the rebind target
    assert dialed, "no dial recorded — the pinned adapter was not used"
    assert all(a[0] == PUBLIC for a in dialed), dialed
    assert not any(a[0] == "127.0.0.1" for a in dialed), dialed

    # and the guard still refuses when the FIRST resolution is non-public
    with mock.patch("socket.getaddrinfo",
                    return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))]):
        try:
            extractors._assert_public_url("https://pinned.example.com/")
        except ValueError:
            pass
        else:
            raise AssertionError("loopback resolution was not refused")


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------

NO_DB_TESTS = (
    test_path_confinement_staging_only,
    test_export_chat_real_format_fixtures,
    test_restore_sh_source_flags,
    test_audit_edges_md5_sample_sql,
    test_sync_notes_skips_hidden_dirs,
    test_extract_url_pinned_resolution,
)

DB_TESTS = (
    test_review_decision_queue_and_db,
    test_store_raw_pdf_stores_file_bytes,
)


if __name__ == "__main__":
    failed = 0

    def run(fn):
        global failed
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 — report and continue
            failed += 1
            print(f"FAIL {fn.__name__}: {type(exc).__name__}: {exc}")
        else:
            print(f"PASS {fn.__name__}")

    for fn in NO_DB_TESTS:
        run(fn)

    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        for fn in DB_TESTS:
            print(f"SKIP {fn.__name__} (no TEST_DATABASE_URL)")
    else:
        # Refusal guard BEFORE any connection: the dbname must match
        # (^|_)test(_|$) case-insensitively.
        name = db_name(url)
        if not is_test_dbname(name):
            print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
                  "point at a test database (dbname matching (^|_)test(_|$)) — this "
                  "test writes knowledge_items rows.")
            sys.exit(1)
        if not db_reachable(url):
            print(SKIP_MSG)
            for fn in DB_TESTS:
                print(f"SKIP {fn.__name__} (test database unreachable)")
        else:
            # bridge subprocesses read DATABASE_URL — point it at the guarded
            # test DB only.
            os.environ["DATABASE_URL"] = url
            for fn in DB_TESTS:
                run(fn)

    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
