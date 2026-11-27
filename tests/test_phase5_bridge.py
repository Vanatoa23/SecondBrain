"""Tests for the Phase 5 W1 surface: NEW audit.py at repo root + the new
bridge.py commands (flag_issue, project_context, discover_relevant, log_audit)
and the bridge_write / bridge_serve_start audit events.

Covers:

  1. audit.log_event appends ONE JSON line {ts, actor, event, ...fields} to
     SECONDBRAIN_AUDIT_LOG and NEVER raises (bad path -> stderr + dict anyway);
     audit.args_fingerprint is a stable 16-hex sha256 over sorted args.
  2. audit.check_external_send: no-op when SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL
     is unset or in ("", "0", "false", "no", "off"); any other value turns the
     gate ON (fail closed). When on: a backslash in the raw netloc or a
     urlsplit/urllib3 host disagreement is refused (the audit-B F1 bypass
     string https://evil.com\\@api.example.com/v1), allowlist entries may be
     "host" or "host:port", plain http is refused for non-loopback hosts
     (http://127.0.0.1:11434 stays allowed), and refusals are PermissionError
     with host-only text (never the full URL).
  3. audit.load_json_file / write_json_atomic / locked_json_update: roundtrip,
     default on missing file, RuntimeError on a corrupt existing file (never
     overwritten); write_json_atomic uses a pid+uuid tmp (fsync + chmod 0600 +
     os.replace, tmp unlinked on failure); concurrent locked_json_update from
     2 threads x 25 increments lands exactly 50 with no .tmp/.lock left behind.
     log_event envelope keys ts/actor/event always win over caller fields.
  4. bridge flag_issue (NO DB needed — it must never touch the DB): creates a
     pending quarantine entry in SECONDBRAIN_FLAG_QUEUE via
     audit.locked_json_update (concurrent writers cannot tear the file), is
     idempotent by sha256((item_id or "") + "\\n" + description)[:16] (status
     preserved), validates title/description/severity/item_id, refuses a
     corrupt queue file, and logs bridge_write with args_sha256 + issue_id
     (outcome="ok"; a failed call logs outcome="error" with a sanitized error).
  5. bridge log_audit appends the event (type popped; reserved ts/actor/event
     keys dropped from caller fields) to the audit log.
  6. DB tests (guarded): flag_issue leaves knowledge_items/relationships
     counts unchanged; project_context on a seeded project://<slug> item
     returns profile (no embedding) + graph/vector related + decisions and
     raises "unknown project" otherwise; discover_relevant returns results
     with by_method_counts + suggested; the audit log carries bridge_write
     lines (outcome "ok"/"error") for mutating commands and
     bridge_serve_start (with pid) when `python -m bridge serve` runs —
     including a FAILING mutating command (update_item on a random uuid)
     served over JSON-lines, which logs bridge_write outcome="error".

Guard pattern (locked — same invariant as tests/test_bridge.py):

  * DB access ONLY via TEST_DATABASE_URL, and only when its dbname matches
    (^|_)test(_|$) case-insensitively — otherwise the script prints REFUSING
    and exits 1 BEFORE any connection. Unset or unreachable -> "SKIP" + exit 0
    (the no-DB tests always run). URLs use 127.0.0.1:5433, never `localhost`,
    never the `postgres` maintenance DB.
  * DATABASE_URL is set to the guarded test URL only for spawned bridge
    subprocesses (bridge.py reads DATABASE_URL).
  * SECONDBRAIN_AUDIT_LOG / SECONDBRAIN_FLAG_QUEUE always point at tmp files
    in these tests, so no repo-root runtime artifacts are touched.
  * The DB tests seed only their own rows (source_uri LIKE 'test://w1phase5/%'
    or 'project://w1p<uniq>') and delete them in a finally block
    (parameterized), so an abort leaves nothing behind.

Run from the repo root:

    TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/bridge_test \
        python tests/test_phase5_bridge.py
"""

from __future__ import annotations

import importlib.util
import json
import os
import queue as queue_mod
import re
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import audit  # noqa: E402  (repo-root module under test)

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)
URI_PREFIX = "test://w1phase5/"

MODEL_OK = importlib.util.find_spec("sentence_transformers") is not None


# ---------------------------------------------------------------------------
# guard helpers (test_bridge.py / test_audit_scripts.py pattern)
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


def _db():
    import psycopg
    from psycopg.rows import dict_row
    return psycopg.connect(os.environ["TEST_DATABASE_URL"], row_factory=dict_row)


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
    envelope = json.loads(lines[0])
    assert isinstance(envelope, dict) and isinstance(envelope.get("ok"), bool), envelope
    if envelope["ok"]:
        assert "result" in envelope and "error" not in envelope, envelope
        assert proc.returncode == 0, (proc.returncode, envelope)
    else:
        assert isinstance(envelope.get("error"), str) and envelope["error"], envelope
        assert "result" not in envelope, envelope
        assert proc.returncode == 1, (proc.returncode, envelope)
    return envelope


def _read_audit_log(path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


def cleanup_test_rows(uniq: str) -> None:
    with _db() as conn:
        conn.execute(
            "DELETE FROM knowledge_items WHERE source_uri LIKE %s OR source_uri = %s",
            (URI_PREFIX + uniq + "/%", f"project://w1p{uniq}"),
        )


# ---------------------------------------------------------------------------
# 1. audit.log_event + args_fingerprint (no DB, in-process)
# ---------------------------------------------------------------------------

def test_audit_log_event_and_fingerprint():
    with tempfile.TemporaryDirectory() as td:
        log = os.path.join(td, "audit.jsonl")
        old_log, old_actor = audit.AUDIT_LOG, os.environ.get("SECONDBRAIN_ACTOR")
        audit.AUDIT_LOG = log
        try:
            with mock.patch.dict(os.environ, {"SECONDBRAIN_ACTOR": "w1-test"}):
                ev = audit.log_event("bridge_write", command="ingest", args_sha256="abc123")
            assert ev["event"] == "bridge_write" and ev["command"] == "ingest", ev
            assert ev["actor"] == "w1-test" and ev["args_sha256"] == "abc123", ev
            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T.*\+00:00", ev["ts"]), ev["ts"]
            lines = _read_audit_log(log)
            assert len(lines) == 1 and lines[0] == ev, lines

            # envelope keys win: caller fields cannot spoof ts/actor/event
            with mock.patch.dict(os.environ, {"SECONDBRAIN_ACTOR": "w1-test"}):
                ev_spoof = audit.log_event("real", event="spoofed", ts="x", actor="y")
            assert ev_spoof["event"] == "real", ev_spoof
            assert ev_spoof["ts"] != "x" and re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T.*\+00:00", ev_spoof["ts"]), ev_spoof
            assert ev_spoof["actor"] == "w1-test", ev_spoof
            lines = _read_audit_log(log)
            assert len(lines) == 2 and lines[-1] == ev_spoof, lines

            # appends another line
            audit.log_event("other_event")
            assert len(_read_audit_log(log)) == 3

            # NEVER raises: an unwritable path prints to stderr, returns dict
            audit.AUDIT_LOG = td  # a DIRECTORY as the log path -> open() fails
            ev2 = audit.log_event("still_returned")
            assert ev2["event"] == "still_returned", ev2
        finally:
            audit.AUDIT_LOG = old_log
            if old_actor is None:
                os.environ.pop("SECONDBRAIN_ACTOR", None)
            else:
                os.environ["SECONDBRAIN_ACTOR"] = old_actor

    # fingerprint: 16 hex chars, stable, key-order-insensitive
    fp1 = audit.args_fingerprint({"b": 1, "a": "x", "c": [1, 2]})
    fp2 = audit.args_fingerprint({"c": [1, 2], "a": "x", "b": 1})
    assert re.fullmatch(r"[0-9a-f]{16}", fp1), fp1
    assert fp1 == fp2, (fp1, fp2)
    assert audit.args_fingerprint({"a": 1}) != audit.args_fingerprint({"a": 2})


# ---------------------------------------------------------------------------
# 2. check_external_send gate (no DB, in-process)
# ---------------------------------------------------------------------------

def test_check_external_send_gate():
    base = {k: v for k, v in os.environ.items()
            if k not in ("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL",
                         "SECONDBRAIN_EXTERNAL_ALLOWLIST")}
    # unset -> no-op for ANY host
    with mock.patch.dict(os.environ, base, clear=True):
        audit.check_external_send("https://anything.example.net/x")
    # REQUIRE=1 with an allowlist -> exact matches pass (case/space-insensitive);
    # a no-port entry matches ANY port, but the scheme must still be https for
    # non-loopback hosts (plain http is refused even for an allowlisted host)
    env = dict(base)
    env["SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL"] = "1"
    env["SECONDBRAIN_EXTERNAL_ALLOWLIST"] = " api.Example.COM , example.org "
    with mock.patch.dict(os.environ, env, clear=True):
        audit.check_external_send("https://api.example.com/v1/extract")
        audit.check_external_send("https://example.org:8080/path")
        try:
            audit.check_external_send("http://example.org:8080/path")
        except PermissionError as e:
            assert "not approved" in str(e), e
        else:
            raise AssertionError("plain http to a non-loopback allowlisted host was not refused")
        for bad in ("https://evil.com/x",
                    "https://api.example.com.evil.com/x",   # look-alike subdomain
                    "https://notexample.com/"):              # not in the list
            try:
                audit.check_external_send(bad)
            except PermissionError as e:
                assert "not approved" in str(e), e
            else:
                raise AssertionError(f"{bad} was not gated")
    # REQUIRE=1 with an EMPTY allowlist -> everything refused
    env2 = dict(base)
    env2["SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL"] = "1"
    env2["SECONDBRAIN_EXTERNAL_ALLOWLIST"] = ""
    with mock.patch.dict(os.environ, env2, clear=True):
        try:
            audit.check_external_send("https://api.example.com/")
        except PermissionError:
            pass
        else:
            raise AssertionError("empty allowlist did not refuse")


def test_check_external_send_hardened():
    """audit-B F1 regression + gate-value/host:port/scheme rules (no DB)."""
    base = {k: v for k, v in os.environ.items()
            if k not in ("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL",
                         "SECONDBRAIN_EXTERNAL_ALLOWLIST")}

    def gate_env(require, allowlist):
        e = dict(base)
        e["SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL"] = require
        e["SECONDBRAIN_EXTERNAL_ALLOWLIST"] = allowlist
        return e

    def expect_refuse(url, require, allowlist):
        with mock.patch.dict(os.environ, gate_env(require, allowlist), clear=True):
            try:
                audit.check_external_send(url)
            except PermissionError as e:
                assert "not approved" in str(e), e
                assert url not in str(e), e  # host-only text, never the full URL
            else:
                raise AssertionError(f"{url!r} was not gated (REQUIRE={require!r})")

    def expect_allow(url, require, allowlist):
        with mock.patch.dict(os.environ, gate_env(require, allowlist), clear=True):
            audit.check_external_send(url)

    # the exact audit-B F1 bypass string: the allowlist contains
    # api.example.com, but the raw netloc carries a backslash and urllib3
    # parses the host as evil.com while urlsplit says api.example.com
    bypass = "https://evil.com\\@api.example.com/v1"
    expect_refuse(bypass, "1", "api.example.com")
    with mock.patch.dict(os.environ, gate_env("1", "api.example.com"), clear=True):
        try:
            audit.check_external_send(bypass)
        except PermissionError as e:
            assert "evil.com" not in str(e), e  # host-only, no URL leak
        else:
            raise AssertionError("backslash bypass was not refused")

    # both parsers agree on ordinary URLs -> they pass the cross-check
    expect_allow("https://api.example.com/v1", "1", "api.example.com")
    expect_allow("https://user:pass@api.example.com/v1?x=1#f", "1", "api.example.com")
    expect_allow("https://api.example.com:8443/v1", "1", "api.example.com")  # no-port entry

    # host:port entries allow exactly the listed port
    expect_allow("https://api.example.com:8443/v1", "1", "api.example.com:8443")
    expect_refuse("https://api.example.com:9443/v1", "1", "api.example.com:8443")
    expect_refuse("https://api.example.com/v1", "1", "api.example.com:8443")

    # plain http to a non-loopback host is refused when the gate is on;
    # loopback http (local Ollama-style) stays allowed
    expect_refuse("http://example.org/x", "1", "example.org")
    expect_allow("http://127.0.0.1:11434/v1", "1", "127.0.0.1:11434")
    expect_allow("http://127.0.0.1:11434/v1", "1", "127.0.0.1")
    expect_refuse("http://localhost:11434/v1", "1", "127.0.0.1:11434")  # localhost not listed

    # gate values: any value NOT in ("", "0", "false", "no", "off") turns the
    # gate ON — unrecognized values fail closed; the listed values turn it off
    for req in ("yes", "true", "2", "enabled"):
        expect_refuse("https://api.example.com/v1", req, "")  # gate on, empty allowlist
    for req in ("0", "false", "no", "off", ""):
        expect_allow("https://anything.example.net/x", req, "")  # gate off -> no-op

    # unset -> no-op for ANY url (even the bypass string)
    with mock.patch.dict(os.environ, base, clear=True):
        audit.check_external_send(bypass)
        audit.check_external_send("https://anything.example.net/x")


# ---------------------------------------------------------------------------
# 3. load_json_file / write_json_atomic (no DB, in-process)
# ---------------------------------------------------------------------------

def test_json_file_helpers():
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "q.json")
        # missing -> default
        assert audit.load_json_file(p, {"updated_at": None, "flagged": []}) == \
            {"updated_at": None, "flagged": []}
        # roundtrip (atomic: no .tmp left behind — tmp names are pid+uuid)
        obj = {"updated_at": "now", "flagged": [{"id": "x", "status": "pending"}]}
        audit.write_json_atomic(p, obj)
        assert audit.load_json_file(p, None) == obj
        assert not os.path.exists(p + ".tmp")
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))
        # corrupt existing file -> RuntimeError, file untouched
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        try:
            audit.load_json_file(p, {"flagged": []})
        except RuntimeError as e:
            assert "refusing" in str(e), e
        else:
            raise AssertionError("corrupt file did not raise RuntimeError")
        with open(p, encoding="utf-8") as fh:
            assert fh.read() == "{not json"  # never overwritten

        # locked_json_update on a corrupt existing file -> RuntimeError, file
        # untouched, no tmp/lock left behind
        def _mut(obj):
            obj["touched"] = True
        try:
            with audit.locked_json_update(p, _mut, dict):
                pass
        except RuntimeError as e:
            assert "refusing" in str(e), e
        else:
            raise AssertionError("corrupt file did not raise RuntimeError via locked_json_update")
        with open(p, encoding="utf-8") as fh:
            assert fh.read() == "{not json"
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))

        # concurrent locked_json_update: 2 threads x 25 increments -> counter == 50,
        # the file parses, and no .tmp / .lock is left behind
        c = os.path.join(td, "counter.json")

        def inc(o):
            o["n"] = int(o.get("n", 0)) + 1

        def worker():
            for _ in range(25):
                with audit.locked_json_update(c, inc, lambda: {"n": 0}) as _r:
                    pass

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert json.loads(Path(c).read_text(encoding="utf-8"))["n"] == 50, \
            Path(c).read_text(encoding="utf-8")
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))


# ---------------------------------------------------------------------------
# 4. flag_issue via bridge subprocess — file ONLY, no DB (no DATABASE_URL)
# ---------------------------------------------------------------------------

def test_flag_issue_file_only_no_db():
    with tempfile.TemporaryDirectory() as td:
        flag_q = os.path.join(td, "flagged_issues.json")
        audit_log = os.path.join(td, "audit_log.jsonl")
        extra = {"SECONDBRAIN_FLAG_QUEUE": flag_q, "SECONDBRAIN_AUDIT_LOG": audit_log,
                 "SECONDBRAIN_ACTOR": "w1"}

        # creates a pending entry (severity defaults to medium)
        env = run_bridge("flag_issue", {
            "title": "Suspicious claim", "description": "desc one " + uuid.uuid4().hex,
        }, extra_env=extra)
        assert env["ok"] is True, env
        e1 = env["result"]
        assert re.fullmatch(r"[0-9a-f]{16}", e1["id"]), e1
        assert e1["status"] == "pending" and e1["severity"] == "medium", e1
        assert e1["item_id"] is None and e1.get("reporter") is None, e1
        assert e1["created_at"], e1

        # idempotent: same (item_id, description) -> same id, stored entry returned
        # (status/severity preserved, not overwritten by the second call)
        desc = "desc one " + uuid.uuid4().hex
        args = {"title": "first", "description": desc, "severity": "high",
                "reporter": "w1"}
        a = run_bridge("flag_issue", args, extra_env=extra)["result"]
        b = run_bridge("flag_issue", {"title": "renamed", "description": desc,
                                      "severity": "low"}, extra_env=extra)["result"]
        assert a["id"] == b["id"], (a, b)
        assert b["title"] == "first" and b["severity"] == "high" and b["status"] == "pending", b

        # explicit uuid item_id accepted; bad uuid rejected
        iid = str(uuid.uuid4())
        desc2 = "desc two " + uuid.uuid4().hex
        c = run_bridge("flag_issue", {"item_id": iid, "title": "t", "description": desc2},
                       extra_env=extra)["result"]
        assert c["item_id"] == iid, c
        expect_id = __import__("hashlib").sha256(
            (iid + "\n" + desc2).encode()).hexdigest()[:16]
        assert c["id"] == expect_id, (c["id"], expect_id)
        env = run_bridge("flag_issue", {"item_id": "not-a-uuid", "title": "t",
                                        "description": "d"}, extra_env=extra)
        assert env["ok"] is False and "uuid" in env["error"], env

        # validation errors
        for bad_args, needle in (
            ({"title": "", "description": "d"}, "title"),
            ({"title": "t", "description": "  "}, "description"),
            ({"title": "t", "description": "d", "severity": "critical"}, "severity"),
            ({"description": "d"}, "title"),
        ):
            env = run_bridge("flag_issue", bad_args, extra_env=extra)
            assert env["ok"] is False and needle in env["error"], (bad_args, env)

        # queue file shape + atomicity (no .tmp / .lock left behind)
        q = json.loads(Path(flag_q).read_text(encoding="utf-8"))
        assert isinstance(q["flagged"], list) and len(q["flagged"]) == 3, q
        assert q["updated_at"], q
        assert not os.path.exists(flag_q + ".tmp")
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))

        # audit log: one bridge_write line per SUCCESSFUL call (outcome="ok")
        # with args_sha256 + issue_id; the FAILING mutating calls so far (bad
        # uuid + 4 validation errors = 5) still log bridge_write with
        # outcome="error" and a sanitized error text.
        # successful calls above: e1, a, b, c = 4
        events = [e for e in _read_audit_log(audit_log) if e["event"] == "bridge_write"]
        ok_events = [e for e in events if e.get("outcome") == "ok"]
        assert len(ok_events) == 4, events
        for e in ok_events:
            assert e["command"] == "flag_issue", e
            assert re.fullmatch(r"[0-9a-f]{16}", e["args_sha256"]), e
            assert re.fullmatch(r"[0-9a-f]{16}", e["issue_id"]), e
            assert e["actor"] == "w1", e
        err_events = [e for e in events if e.get("outcome") == "error"]
        assert len(err_events) == 5, events
        for e in err_events:
            assert e["command"] == "flag_issue" and e.get("error"), e
            assert "ValueError" in e["error"] or "RuntimeError" in e["error"], e

        # corrupt queue file -> refused (ok:false, RuntimeError), file untouched,
        # and the failed call is audited with outcome="error" as well
        with open(flag_q, "w", encoding="utf-8") as fh:
            fh.write("{corrupt")
        env = run_bridge("flag_issue", {"title": "t", "description": "new " + uuid.uuid4().hex},
                         extra_env=extra)
        assert env["ok"] is False and "refusing" in env["error"], env
        with open(flag_q, encoding="utf-8") as fh:
            assert fh.read() == "{corrupt"
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))
        events = [e for e in _read_audit_log(audit_log) if e["event"] == "bridge_write"]
        err_events = [e for e in events if e.get("outcome") == "error"]
        assert len(err_events) == 6 and "RuntimeError" in err_events[-1]["error"], events


def test_flag_issue_concurrent_writers():
    """Two concurrent flag_issue writers (separate bridge processes) flag
    different issues -> both entries land in the queue, no torn file, no
    .tmp/.lock left behind (audit.locked_json_update)."""
    with tempfile.TemporaryDirectory() as td:
        flag_q = os.path.join(td, "flagged_issues.json")
        audit_log = os.path.join(td, "audit_log.jsonl")
        extra = {"SECONDBRAIN_FLAG_QUEUE": flag_q, "SECONDBRAIN_AUDIT_LOG": audit_log}
        results = []

        def worker(i):
            results.append(run_bridge("flag_issue", {
                "title": f"concurrent {i}",
                "description": f"desc-{i}-" + uuid.uuid4().hex,
            }, extra_env=extra))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) == 2 and all(r["ok"] for r in results), results
        q = json.loads(Path(flag_q).read_text(encoding="utf-8"))
        ids = {e["id"] for e in q["flagged"]}
        assert len(q["flagged"]) == 2 and ids == {r["result"]["id"] for r in results}, \
            (q, results)
        assert not list(Path(td).glob("*.tmp")) and not list(Path(td).glob("*.lock"))


# ---------------------------------------------------------------------------
# 5. log_audit command (no DB)
# ---------------------------------------------------------------------------

def test_log_audit_command():
    with tempfile.TemporaryDirectory() as td:
        audit_log = os.path.join(td, "audit_log.jsonl")
        extra = {"SECONDBRAIN_AUDIT_LOG": audit_log, "SECONDBRAIN_ACTOR": "mcp-write"}
        env = run_bridge("log_audit", {"event": {"type": "mcp_start", "scope": "write"}},
                         extra_env=extra)
        assert env["ok"] is True and env["result"] == {"logged": True}, env
        events = _read_audit_log(audit_log)
        assert len(events) == 1, events
        assert events[0]["event"] == "mcp_start" and events[0]["scope"] == "write", events[0]
        assert events[0]["actor"] == "mcp-write" and "ts" in events[0], events[0]
        # "type" was consumed as the event name, not logged as a field
        assert "type" not in events[0], events[0]

        # reserved envelope keys (ts/actor/event) are dropped from caller fields
        env = run_bridge("log_audit", {"event": {"type": "spoof", "ts": "x", "actor": "y",
                                                  "event": "z", "note": "n"}},
                         extra_env=extra)
        assert env["ok"] is True, env
        ev = [e for e in _read_audit_log(audit_log) if e["event"] == "spoof"][-1]
        assert ev["event"] == "spoof" and ev.get("note") == "n", ev
        assert ev["ts"] != "x" and re.fullmatch(r"\d{4}-\d{2}-\d{2}T.*\+00:00", ev["ts"]), ev
        assert ev["actor"] == "mcp-write", ev  # not the caller's "y"

        # bad payloads -> ok:false
        for bad in ({}, {"event": "not-a-dict"}, {"event": {"no_type": 1}},
                    {"event": {"type": ""}}):
            env = run_bridge("log_audit", bad, extra_env=extra)
            assert env["ok"] is False, (bad, env)


def test_sanitize_error_redacts_url_query_and_fragment():
    """_sanitize_error must strip URL query strings / fragments (and env
    secrets) from error text before it lands in an envelope or audit event."""
    import bridge
    secret = "s3cr3t-query-value-" + uuid.uuid4().hex[:8]
    with mock.patch.dict(os.environ, {"MY_TEST_TOKEN": secret}):
        out = bridge._sanitize_error(
            f"boom https://api.example.com/v1?token={secret}&x=1#frag {secret}"
        )
    assert secret not in out, out
    assert "https://api.example.com/v1?<redacted>" in out, out
    assert "#frag" not in out and "&x=1" not in out, out
    # credentials are still redacted too
    out2 = bridge._sanitize_error("failed https://user:pw@api.example.com/v1")
    assert "pw" not in out2 and "<redacted>" in out2, out2


# ---------------------------------------------------------------------------
# 6. DB tests
# ---------------------------------------------------------------------------

def test_flag_issue_never_writes_db():
    """flag_issue must leave knowledge_items / relationships counts unchanged."""
    url = os.environ["TEST_DATABASE_URL"]
    with tempfile.TemporaryDirectory() as td:
        extra = {"SECONDBRAIN_FLAG_QUEUE": os.path.join(td, "f.json"),
                 "SECONDBRAIN_AUDIT_LOG": os.path.join(td, "a.jsonl")}
        with _db() as conn:
            before = (conn.execute("SELECT COUNT(*) AS n FROM knowledge_items").fetchone()["n"],
                      conn.execute("SELECT COUNT(*) AS n FROM relationships").fetchone()["n"])
        for i in range(3):
            env = run_bridge("flag_issue", {
                "title": f"db-untouched {i}", "description": f"d{i} " + uuid.uuid4().hex,
                "severity": ("low", "medium", "high")[i],
            }, url=url, extra_env=extra)
            assert env["ok"] is True, env
        with _db() as conn:
            after = (conn.execute("SELECT COUNT(*) AS n FROM knowledge_items").fetchone()["n"],
                     conn.execute("SELECT COUNT(*) AS n FROM relationships").fetchone()["n"])
        assert before == after, (before, after)


def test_project_context_seeded_and_unknown():
    """project_context returns profile+related for a seeded project:// item;
    unknown project -> ok:false 'unknown project'."""
    url = os.environ["TEST_DATABASE_URL"]
    uniq = uuid.uuid4().hex[:8]
    slug = f"w1p{uniq}"
    profile_uri = f"project://{slug}"
    extra = {"SECONDBRAIN_AUDIT_LOG": os.path.join(tempfile.mkdtemp(), "a.jsonl")}
    ids = []
    try:
        # seed: project profile + two related items, one decision-titled
        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": (f"Project {slug} profile: we build a second brain with "
                        f"PostgreSQL and pgvector. Phase5 ctx {uniq}."),
            "title": f"project profile {slug}",
            "source_uri": profile_uri,
        }, url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        proj_id = env["result"]["node_ids"][0]
        ids.append(proj_id)

        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": f"Related infra note {uniq}: the bridge serves JSON-lines commands.",
            "title": f"infra note {uniq}",
            "source_uri": f"{URI_PREFIX}{uniq}/infra",
        }, url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        infra_id = env["result"]["node_ids"][0]
        ids.append(infra_id)

        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": f"Decision record {uniq}: we chose parameterized SQL everywhere.",
            "title": f"decision: parameterized SQL {uniq}",
            "source_uri": f"{URI_PREFIX}{uniq}/decision",
        }, url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        dec_id = env["result"]["node_ids"][0]
        ids.append(dec_id)

        for other in (infra_id, dec_id):
            env = run_bridge("link_items", {
                "from_id": proj_id, "to_id": other, "type": "related",
                "direction": "both", "context": f"w1 phase5 link {uniq}",
            }, url=url, extra_env=extra)
            assert env["ok"] is True, env

        # project_context — call with an UPPERCASE project name to exercise the slug
        env = run_bridge("project_context", {"project": slug.upper()}, url=url,
                         timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        res = env["result"]
        assert res["project"] == slug.upper(), res["project"]
        assert res["profile"]["source_uri"] == profile_uri, res["profile"]
        assert "embedding" not in res["profile"], res["profile"].keys()
        graph_ids = {r["id"] for r in res["graph_related"]}
        assert infra_id in graph_ids and dec_id in graph_ids, (graph_ids, res["graph_related"])
        assert isinstance(res["vector_related"], list), res
        dec_titles = [d.get("title") for d in res["decisions"]]
        assert any("decision" in (t or "").lower() for t in dec_titles), res["decisions"]
        assert isinstance(res["estimated_tokens"], int) and res["estimated_tokens"] >= 0
        assert res["estimated_tokens"] <= 4000, res["estimated_tokens"]

        # small budget caps content
        env = run_bridge("project_context", {"project": slug, "token_budget": 10},
                         url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        assert env["result"]["estimated_tokens"] <= 10, env["result"]["estimated_tokens"]

        # unknown project -> ValueError envelope
        env = run_bridge("project_context", {"project": f"nope{uniq}"}, url=url,
                         extra_env=extra)
        assert env["ok"] is False and "unknown project" in env["error"], env
        assert f"project://nope{uniq}" in env["error"], env["error"]

        # read-only: project_context logged NO audit event
        events = _read_audit_log(extra["SECONDBRAIN_AUDIT_LOG"])
        writes = [e for e in events if e.get("command") in ("project_context",)]
        assert not writes, writes
        # but the seed ingest/link DID log bridge_write lines with args_sha256
        bridge_writes = [e for e in events if e["event"] == "bridge_write"]
        assert any(e["command"] == "ingest" for e in bridge_writes), bridge_writes
        assert any(e["command"] == "link_items" and e.get("item_ids") for e in bridge_writes), \
            bridge_writes
        for e in bridge_writes:
            assert re.fullmatch(r"[0-9a-f]{16}", e["args_sha256"]), e
    finally:
        try:
            cleanup_test_rows(uniq)
        except Exception:
            pass


def test_discover_relevant_returns_results():
    url = os.environ["TEST_DATABASE_URL"]
    uniq = uuid.uuid4().hex[:8]
    marker = f"quokka-w1-{uniq}"
    extra = {"SECONDBRAIN_AUDIT_LOG": os.path.join(tempfile.mkdtemp(), "a.jsonl")}
    try:
        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": (f"Discover probe {marker}: quokkas are small marsupials "
                        f"native to Rottnest Island."),
            "title": f"lesson: quokka facts {uniq}",
            "source_uri": f"{URI_PREFIX}{uniq}/quokka",
        }, url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        item_id = env["result"]["node_ids"][0]

        env = run_bridge("discover_relevant", {"query": f"quokka {uniq}", "top_k": 5},
                         url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env
        res = env["result"]
        assert res["query"] == f"quokka {uniq}", res
        assert isinstance(res["results"], list) and res["results"], res
        assert any(r["id"] == item_id for r in res["results"]), \
            [r["id"] for r in res["results"]]
        assert set(res["by_method_counts"].keys()) == {"vector", "fts", "graph"}, res
        assert all(isinstance(v, int) for v in res["by_method_counts"].values()), res
        assert set(res["suggested"].keys()) == {"decisions", "lessons"}, res
        # the lesson-titled item shows up under suggested
        assert any("quokka" in (t or "") for t in res["suggested"]["decisions"]), res["suggested"]

        # clamps: hops/top_k out of range are clamped, not fatal
        env = run_bridge("discover_relevant", {"query": f"quokka {uniq}",
                                               "hops": 99, "top_k": 0},
                         url=url, timeout=900, extra_env=extra)
        assert env["ok"] is True, env

        # read-only: no audit event for discover_relevant itself
        events = _read_audit_log(extra["SECONDBRAIN_AUDIT_LOG"])
        assert not [e for e in events if e.get("command") == "discover_relevant"], events
    finally:
        try:
            cleanup_test_rows(uniq)
        except Exception:
            pass


def test_serve_start_and_write_audit_events():
    """`python -m bridge serve` logs bridge_serve_start (with pid); mutating
    commands through serve log bridge_write; log_audit through serve appends."""
    url = os.environ["TEST_DATABASE_URL"]
    with tempfile.TemporaryDirectory() as td:
        audit_log = os.path.join(td, "audit_log.jsonl")
        flag_q = os.path.join(td, "flagged_issues.json")
        env = {k: v for k, v in os.environ.items()
               if k not in ("DATABASE_URL", "TEST_DATABASE_URL", "BRIDGE_PATH_ALLOWLIST")}
        env["DATABASE_URL"] = url
        env["SECONDBRAIN_AUDIT_LOG"] = audit_log
        env["SECONDBRAIN_FLAG_QUEUE"] = flag_q

        # a mutating CLI command logs bridge_write with args_sha256
        uniq = uuid.uuid4().hex[:8]
        try:
            env_cli = run_bridge("ingest", {
                "source_type": "text",
                "payload": f"serve-audit probe {uniq}: bridge logs writes.",
                "title": f"serve audit {uniq}",
                "source_uri": f"{URI_PREFIX}{uniq}/serve",
            }, url=url, timeout=900, extra_env={"SECONDBRAIN_AUDIT_LOG": audit_log})
            assert env_cli["ok"] is True, env_cli
            events = _read_audit_log(audit_log)
            ing = [e for e in events if e["event"] == "bridge_write"
                   and e.get("command") == "ingest"]
            assert ing and re.fullmatch(r"[0-9a-f]{16}", ing[-1]["args_sha256"]), events
            assert ing[-1].get("outcome") == "ok", ing[-1]

            # serve mode: log_audit + flag_issue + list_recent over JSON-lines
            proc = subprocess.Popen(
                [sys.executable, "-m", "bridge", "serve"],
                cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, bufsize=1, env=env,
            )
            lines: "queue_mod.Queue" = queue_mod.Queue()

            def reader():
                for line in proc.stdout:
                    lines.put(line)
                lines.put(None)

            def drain():
                for _ in proc.stderr:
                    pass

            threading.Thread(target=reader, daemon=True).start()
            threading.Thread(target=drain, daemon=True).start()

            def send(obj):
                proc.stdin.write(json.dumps(obj) + "\n")
                proc.stdin.flush()

            def recv(timeout=900):
                line = lines.get(timeout=timeout)
                assert line is not None, "serve stdout closed early"
                return json.loads(line)

            send({"id": 1, "command": "log_audit",
                  "args": {"event": {"type": "mcp_write_start", "scope": "write"}}})
            r = recv()
            assert r["id"] == 1 and r["ok"] is True and r["result"] == {"logged": True}, r

            send({"id": 2, "command": "flag_issue",
                  "args": {"title": "serve flag", "description": f"serve {uniq}"}})
            r = recv()
            assert r["id"] == 2 and r["ok"] is True and r["result"]["status"] == "pending", r
            serve_issue_id = r["result"]["id"]

            send({"id": 3, "command": "list_recent", "args": {"limit": 3}})
            r = recv()
            assert r["id"] == 3 and r["ok"] is True and isinstance(r["result"], list), r

            # a FAILING mutating command (update_item on a random uuid) still
            # gets a bridge_write audit event with outcome="error"
            send({"id": 4, "command": "update_item",
                  "args": {"item_id": str(uuid.uuid4()), "title": "nope"}})
            r = recv()
            assert r["id"] == 4 and r["ok"] is False and "not found" in r["error"], r

            proc.stdin.close()
            rc = proc.wait(timeout=120)
            assert rc == 0, f"serve exit code {rc}"

            events = _read_audit_log(audit_log)
            starts = [e for e in events if e["event"] == "bridge_serve_start"]
            assert len(starts) == 1 and starts[0]["pid"] == proc.pid, (starts, proc.pid)
            assert any(e["event"] == "mcp_write_start" and e.get("scope") == "write"
                       for e in events), events
            fw = [e for e in events if e["event"] == "bridge_write"
                  and e.get("command") == "flag_issue" and e.get("issue_id") == serve_issue_id]
            assert fw and re.fullmatch(r"[0-9a-f]{16}", fw[-1]["args_sha256"]), events
            assert fw[-1].get("outcome") == "ok", fw[-1]
            err_fw = [e for e in events if e["event"] == "bridge_write"
                      and e.get("command") == "update_item" and e.get("outcome") == "error"]
            assert err_fw, events
            assert "ValueError" in err_fw[-1].get("error", ""), err_fw[-1]
            assert "not found" in err_fw[-1]["error"], err_fw[-1]
            # list_recent is read-only -> no bridge_write for it
            assert not [e for e in events if e.get("command") == "list_recent"], events
        finally:
            try:
                cleanup_test_rows(uniq)
            except Exception:
                pass


def test_review_fixes_regressions():
    """Regressions for PR #4 review fixes (no DB):
    - audit.check_external_send implicit default ports (443/80), loopback scheme
      enforcement (reject file/ftp on localhost), case/space-insensitive off values
    - audit.locked_json_update / write_json_atomic auto-creating missing parent dirs
    - bridge.cmd_review_decision: UUID + reason='flagged-issue' is queue-only
      (never touches DB) and concurrent updates do not lose writes.
    """
    import concurrent.futures

    base = {k: v for k, v in os.environ.items()
            if k not in ("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL",
                         "SECONDBRAIN_EXTERNAL_ALLOWLIST")}

    # 1. Implicit default ports: api.example.com:443 matches https://api.example.com/v1
    env = dict(base, SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL="1",
               SECONDBRAIN_EXTERNAL_ALLOWLIST="api.example.com:443,localhost:80")
    with mock.patch.dict(os.environ, env, clear=True):
        audit.check_external_send("https://api.example.com/v1")
        audit.check_external_send("http://localhost/v1")
        # Non-HTTP schemes on loopback must still be refused
        for bad_scheme in ("file://localhost/etc/passwd", "ftp://localhost/data"):
            try:
                audit.check_external_send(bad_scheme)
            except PermissionError:
                pass
            else:
                raise AssertionError(f"{bad_scheme!r} was not refused")

    # 2. Case/whitespace-insensitive off values
    for off_val in ("FALSE", " Off ", " 0 "):
        with mock.patch.dict(os.environ, dict(base, SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL=off_val), clear=True):
            audit.check_external_send("https://anything.example.net/x")

    # 3. locked_json_update creates missing parent directories
    with tempfile.TemporaryDirectory() as td:
        nested = os.path.join(td, "sub", "dir", "queue.json")
        with audit.locked_json_update(nested, lambda d: d.setdefault("x", 1), dict):
            pass
        assert json.loads(Path(nested).read_text(encoding="utf-8")) == {"x": 1}

    # 4. bridge.cmd_review_decision: UUID + reason='flagged-issue' is queue-only
    #    (never imports/touches DB) and concurrent decisions on distinct entries
    #    are serialized via audit.locked_json_update with no lost updates.
    import bridge

    with tempfile.TemporaryDirectory() as td:
        qpath = os.path.join(td, "review_queue.json")
        uid1 = str(uuid.uuid4())
        uid2 = str(uuid.uuid4())
        initial = {
            "updated_at": None,
            "flagged": [
                {"id": uid1, "reason": "flagged-issue", "status": "pending"},
                {"id": uid2, "reason": "flagged-issue", "status": "pending"},
            ],
        }
        Path(qpath).write_text(json.dumps(initial, indent=2) + "\n", encoding="utf-8")
        audit_path = os.path.join(td, "audit.jsonl")

        with mock.patch.object(bridge, "_review_queue_path", return_value=qpath), \
             mock.patch.object(audit, "AUDIT_LOG", audit_path):
            def _decide(uid, dec):
                return bridge.cmd_review_decision({
                    "id": uid, "reason": "flagged-issue", "decision": dec,
                })

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                f1 = pool.submit(_decide, uid1, "approved")
                f2 = pool.submit(_decide, uid2, "dismissed")
                r1, r2 = f1.result(), f2.result()

            assert r1["queue_updated"] and r1["db_updated"] == 0, r1
            assert r2["queue_updated"] and r2["db_updated"] == 0, r2
            final_q = json.loads(Path(qpath).read_text(encoding="utf-8"))
            by_id = {e["id"]: e["status"] for e in final_q["flagged"]}
            assert by_id == {uid1: "approved", uid2: "dismissed"}, by_id


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------

NO_DB_TESTS = (
    test_audit_log_event_and_fingerprint,
    test_check_external_send_gate,
    test_check_external_send_hardened,
    test_json_file_helpers,
    test_flag_issue_file_only_no_db,
    test_flag_issue_concurrent_writers,
    test_log_audit_command,
    test_sanitize_error_redacts_url_query_and_fragment,
    test_review_fixes_regressions,
)

DB_TESTS = (
    test_flag_issue_never_writes_db,
    test_project_context_seeded_and_unknown,
    test_discover_relevant_returns_results,
    test_serve_start_and_write_audit_events,
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
                if fn in (test_project_context_seeded_and_unknown,
                          test_discover_relevant_returns_results,
                          test_serve_start_and_write_audit_events) and not MODEL_OK:
                    print(f"SKIP {fn.__name__} (sentence-transformers not installed)")
                    continue
                run(fn)

    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
