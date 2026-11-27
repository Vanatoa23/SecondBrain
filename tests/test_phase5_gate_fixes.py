"""Phase 5 gate-fix tests (fix worker F-B) — audit-A/audit-B findings:

  1. redirect bypass on the EXTRACT_API_URL / VISION_API_URL POSTs
     (allow_redirects=False + manual re-gated redirect loop, max 3 hops),
  2. fail-closed when SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is enabled but
     the audit module is not importable,
  3. external_fetch events log endpoint + url_sha256 only (never the raw
     URL), and error text redacts URL query strings / fragments,
  4. curator queue writes go through audit.write_json_atomic /
     audit.locked_json_update (no torn or lost updates under concurrency),
  5. MCP read scope forces dream dry_run=true; unset scope prints the LOUD
     stderr warning about ungated mutating tools.

Guard pattern (locked — same invariant as tests/test_e2e.py):

    TEST_DATABASE_URL only (never DATABASE_URL). The dbname must match the
    delimited (^|_)test(_|$) pattern case-insensitively, otherwise the script
    REFUSES with exit 1 BEFORE any connection. Unset or unreachable -> the DB
    tests print SKIP and the script still exits 0 (this suite's tests are all
    no-DB, so nothing is skipped in practice). No test ever touches the
    `postgres` database.

Run from the repo root:

    python tests/test_phase5_gate_fixes.py

The MCP cases spawn the BUILT server (mcp-server/dist/index.js) against a
FAKE bridge module (BRIDGE_CWD pointed at a tmp dir whose bridge.py records
the serve requests), so no database and no model load are involved.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import importlib.util
import json
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

EXTRACT_ENV = {
    "EXTRACT_API_URL": "https://llm-allowed.example/v1",
    "EXTRACT_MODEL": "gate-fix-model",
    "EXTRACT_API_KEY": "dummy-extract-key-fixb",
}
VISION_ENV = {
    "VISION_API_URL": "https://vision-allowed.example/v1",
    "VISION_MODEL": "vision-fix-model",
    "VISION_API_KEY": "vk-fixb-secret",
}
ALLOWED = "llm-allowed.example,vision-allowed.example"


def db_name(url: str) -> str:
    from urllib.parse import urlsplit

    try:
        return urlsplit(url).path.lstrip("/").split("/")[0]
    except Exception:
        return ""


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# audit env helper — reload the REAL repo-root audit.py while the SECONDBRAIN_*
# env vars point AUDIT_LOG into a tmp dir (audit computes paths at import).
# ---------------------------------------------------------------------------


def _audit_module():
    import audit

    importlib.reload(audit)
    return audit


@contextlib.contextmanager
def audit_env(tmp_path, *, require_approval=None, allowlist=None):
    log_path = Path(tmp_path) / "audit_log.jsonl"
    patch = {"SECONDBRAIN_AUDIT_LOG": str(log_path)}
    if require_approval is not None:
        patch["SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL"] = require_approval
    if allowlist is not None:
        patch["SECONDBRAIN_EXTERNAL_ALLOWLIST"] = allowlist
    with mock.patch.dict(os.environ, patch):
        if require_approval is None:
            os.environ.pop("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL", None)
        if allowlist is None:
            os.environ.pop("SECONDBRAIN_EXTERNAL_ALLOWLIST", None)
        mod = _audit_module()
        try:
            yield mod, log_path
        finally:
            importlib.reload(mod)  # restore unpatched paths for later tests


def _read_events(log_path: Path) -> list[dict]:
    if not log_path.exists():
        return []
    out = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


# ---------------------------------------------------------------------------
# fake HTTP responses
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status_code=200, headers=None, text="", body=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _ok_extract_resp():
    return _Resp(200, {}, "", {"choices": [{"message": {"content": json.dumps({"entities": [], "relations": []})}}]})


def _redirect_resp(location):
    return _Resp(307, {"Location": location}, "redirect")


def _png_bytes() -> bytes:
    import io

    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (8, 8), (200, 30, 30)).save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# 1. gate: redirect to a non-allowlisted host is refused BEFORE dialing it
# ---------------------------------------------------------------------------


def test_gate_redirect_to_non_allowlisted_refused():
    from pipeline import extract as extract_mod

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td, require_approval="1", allowlist=ALLOWED) as (_m, log_path):
            with mock.patch.dict(os.environ, EXTRACT_ENV), \
                 mock.patch("requests.post", return_value=_redirect_resp("https://evil.example/x")) as post:
                try:
                    extract_mod.extract_knowledge("hello gate")
                except PermissionError:
                    pass  # expected — refused before dialing evil.example
                else:
                    raise AssertionError("307 to non-allowlisted host did not raise PermissionError")
                assert post.call_count == 1, post.call_count
                assert post.call_args.args[0] == "https://llm-allowed.example/v1/chat/completions"
                assert post.call_args.kwargs.get("allow_redirects") is False, post.call_args.kwargs
            events = _read_events(log_path)
            sends = [e for e in events if e.get("event") == "external_send"]
            assert all(e["endpoint"] != "evil.example" for e in sends), sends
            assert not any("evil.example" in json.dumps(e) and e.get("event") == "external_send" for e in events)


def test_gate_redirect_to_allowlisted_followed_with_regate():
    from pipeline import extract as extract_mod

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td, require_approval="1", allowlist=ALLOWED) as (_m, log_path):
            calls = []

            def fake_post(url, **kw):
                calls.append((url, kw.get("allow_redirects")))
                if len(calls) == 1:
                    return _redirect_resp("https://llm-allowed.example/v2/chat/completions")
                return _ok_extract_resp()

            with mock.patch.dict(os.environ, EXTRACT_ENV), \
                 mock.patch("requests.post", side_effect=fake_post):
                result = extract_mod.extract_knowledge("hello")
            assert result == {"entities": [], "relations": []}, result
            assert [c[0] for c in calls] == [
                "https://llm-allowed.example/v1/chat/completions",
                "https://llm-allowed.example/v2/chat/completions",
            ], calls
            assert all(c[1] is False for c in calls), calls  # allow_redirects=False on every POST
            sends = [e for e in _read_events(log_path) if e.get("event") == "external_send"]
            assert len(sends) == 2, _read_events(log_path)  # initial + one re-gated hop
            assert all(e["endpoint"] == "llm-allowed.example" for e in sends), sends


def test_gate_redirect_without_location_and_too_many_hops_raise():
    from pipeline import extract as extract_mod

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td, require_approval="1", allowlist=ALLOWED) as (_m, _log):
            with mock.patch.dict(os.environ, EXTRACT_ENV), \
                 mock.patch("requests.post", return_value=_Resp(307, {}, "no location")):
                try:
                    extract_mod.extract_knowledge("x")
                except ValueError as exc:
                    assert "Location" in str(exc), exc
                else:
                    raise AssertionError("3xx without Location did not raise ValueError")

            # endless redirects -> >3 hops -> ValueError
            with mock.patch.dict(os.environ, EXTRACT_ENV), \
                 mock.patch("requests.post",
                            return_value=_redirect_resp("https://llm-allowed.example/loop")) as post:
                try:
                    extract_mod.extract_knowledge("x")
                except ValueError as exc:
                    assert "redirect" in str(exc).lower(), exc
                else:
                    raise AssertionError("redirect loop did not raise ValueError")
                assert post.call_count == 4, post.call_count  # 1 initial + 3 hops


def test_vision_redirect_to_non_allowlisted_refused():
    from pipeline import extractors

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td, require_approval="1", allowlist=ALLOWED) as (_m, log_path):
            with mock.patch.dict(os.environ, VISION_ENV), \
                 mock.patch("requests.post", return_value=_redirect_resp("https://evil.example/steal")) as post:
                try:
                    extractors._extract_image_vision(_png_bytes())
                except PermissionError:
                    pass
                else:
                    raise AssertionError("vision 307 to non-allowlisted host did not raise PermissionError")
                assert post.call_count == 1, post.call_count
                assert post.call_args.args[0] == "https://vision-allowed.example/v1/chat/completions"
                assert post.call_args.kwargs.get("allow_redirects") is False, post.call_args.kwargs
            sends = [e for e in _read_events(log_path) if e.get("event") == "external_send"]
            assert all(e["endpoint"] != "evil.example" for e in sends), sends


def test_vision_redirect_to_allowlisted_followed_with_regate():
    from pipeline import extractors

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td, require_approval="1", allowlist=ALLOWED) as (_m, log_path):
            calls = []

            def fake_post(url, **kw):
                calls.append((url, kw.get("allow_redirects")))
                if len(calls) == 1:
                    return _redirect_resp("/v2/chat/completions")  # relative Location
                return _Resp(200, {}, "", {"choices": [{"message": {"content": "a red square"}}]})

            with mock.patch.dict(os.environ, VISION_ENV), \
                 mock.patch("requests.post", side_effect=fake_post):
                text = extractors._extract_image_vision(_png_bytes())
            assert text == "a red square", text
            assert [c[0] for c in calls] == [
                "https://vision-allowed.example/v1/chat/completions",
                "https://vision-allowed.example/v2/chat/completions",  # urljoin resolved relative
            ], calls
            assert all(c[1] is False for c in calls), calls
            sends = [e for e in _read_events(log_path) if e.get("event") == "external_send"]
            assert len(sends) == 2 and all(e["endpoint"] == "vision-allowed.example" for e in sends), sends


# ---------------------------------------------------------------------------
# 2. fail closed: REQUIRE enabled + audit module unimportable -> raise
# ---------------------------------------------------------------------------


def test_fail_closed_extract_require_on_audit_missing():
    from pipeline import extract as extract_mod

    with mock.patch.dict(os.environ, {**EXTRACT_ENV, "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL": "1"}), \
         mock.patch.object(extract_mod, "_load_audit", lambda: None), \
         mock.patch("requests.post") as post:
        try:
            extract_mod.extract_knowledge("x")
        except RuntimeError as exc:
            assert "audit module unavailable" in str(exc), exc
        else:
            raise AssertionError("REQUIRE=1 + audit None did not raise")
        assert post.call_count == 0, "no request may be made when failing closed"


def test_fail_closed_vision_require_on_audit_missing():
    from pipeline import extractors

    with mock.patch.dict(os.environ, {**VISION_ENV, "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL": "true"}), \
         mock.patch.object(extractors, "_load_audit", lambda: None), \
         mock.patch("requests.post") as post:
        try:
            extractors._extract_image_vision(_png_bytes())
        except RuntimeError as exc:
            assert "audit module unavailable" in str(exc), exc
        else:
            raise AssertionError("vision REQUIRE=true + audit None did not raise")
        assert post.call_count == 0, post.call_count


def test_require_unset_audit_missing_proceeds():
    from pipeline import extract as extract_mod

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td) as (_m, _log):  # REQUIRE unset
            with mock.patch.dict(os.environ, EXTRACT_ENV), \
                 mock.patch.object(extract_mod, "_load_audit", lambda: None), \
                 mock.patch("requests.post", return_value=_ok_extract_resp()) as post:
                result = extract_mod.extract_knowledge("x")
            assert result == {"entities": [], "relations": []}, result
            assert post.call_count == 1, "REQUIRE unset + audit None stays a no-op"


# ---------------------------------------------------------------------------
# 3. external_fetch: endpoint + url_sha256 only, never the raw URL; error
#    envelopes redact the URL query/fragment
# ---------------------------------------------------------------------------


def test_external_fetch_youtube_logs_no_raw_url():
    from pipeline import extractors

    secret = "YT-SECRET-987"
    yt_url = f"https://www.youtube.com/watch?v=abc123&api_key={secret}"
    with tempfile.TemporaryDirectory() as td:
        with audit_env(td) as (_m, log_path):
            with mock.patch("socket.getaddrinfo",
                            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "",
                                           ("93.184.216.34", 443))]):
                res = extractors.extract_youtube(
                    yt_url, runner=lambda u, w: {"subtitle_path": None,
                                                 "info": {"title": "T", "description": "D"}})
            assert "T" in res.text
            fetches = [e for e in _read_events(log_path) if e.get("event") == "external_fetch"]
            assert len(fetches) == 1, _read_events(log_path)
            ev = fetches[0]
            assert ev.get("endpoint") == "www.youtube.com", ev
            assert ev.get("url_sha256") == hashlib.sha256(yt_url.encode()).hexdigest()[:16], ev
            assert "url" not in ev, ev  # raw url field must be gone
            raw_log = log_path.read_text(encoding="utf-8")
            assert secret not in raw_log, "the secret must not appear in the audit log"
            assert yt_url not in raw_log, "the raw signed URL must not appear in the audit log"


def test_external_fetch_extract_url_and_error_envelope_redacted():
    from pipeline import extractors

    secret = "URL-SECRET-555"
    url = f"https://public.example/page?api_key={secret}&sig=abc#frag"

    class _StreamResp:
        status_code = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}
        encoding = "utf-8"
        apparent_encoding = "utf-8"

        def iter_content(self, chunk_size=65536):
            return [b"<html><body>hello page</body></html>"]

        def raise_for_status(self):
            return None

        def close(self):
            return None

    with tempfile.TemporaryDirectory() as td:
        with audit_env(td) as (_m, log_path):
            with mock.patch.object(extractors, "_assert_public_url", return_value=["93.184.216.34"]), \
                 mock.patch.object(extractors, "_fetch_pinned", return_value=_StreamResp()):
                res = extractors.extract_url(url)
            assert "hello page" in res.text, res.text
            fetches = [e for e in _read_events(log_path) if e.get("event") == "external_fetch"]
            assert len(fetches) == 1, _read_events(log_path)
            ev = fetches[0]
            assert ev.get("endpoint") == "public.example", ev
            assert ev.get("url_sha256") == hashlib.sha256(url.encode()).hexdigest()[:16], ev
            assert "url" not in ev, ev
            raw_log = log_path.read_text(encoding="utf-8")
            assert secret not in raw_log and url not in raw_log, "raw signed URL leaked into the audit log"

            # error envelope: a guard/fetch error echoing the signed URL must
            # be sanitized — the query secret must not survive
            def _boom(u):
                raise ValueError(f"refusing to fetch URL without a hostname: {u!r}")

            with mock.patch.object(extractors, "_assert_public_url", side_effect=lambda u: _boom(u)):
                try:
                    extractors.extract_url(url)
                except ValueError as exc:
                    msg = str(exc)
                    assert secret not in msg, msg
                    assert "api_key" not in msg or "<redacted>" in msg, msg
                    assert "?<redacted>" in msg, msg
                else:
                    raise AssertionError("expected ValueError from the guard")


# ---------------------------------------------------------------------------
# 4. curator: locked_json_update concurrency + back-fill; no torn writes
# ---------------------------------------------------------------------------


def test_curator_locked_json_update_concurrency():
    curator = _load_script("curator")
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        q = td / "review_queue.json"
        q.write_text(json.dumps({"updated_at": None, "flagged": [
            {"id": "base", "reason": "stale", "status": "pending"}]}), encoding="utf-8")
        errors = []
        n_threads = 8

        def worker(n):
            try:
                def mutator(data):
                    data.setdefault("flagged", []).append(
                        {"id": f"item-{n}", "reason": "isolated", "status": "pending"})
                    data["updated_at"] = "now"

                with curator._locked_json_update(q, mutator,
                                                 lambda: {"updated_at": None, "flagged": []}):
                    pass
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        data = json.loads(q.read_text(encoding="utf-8"))  # final file is valid JSON
        ids = sorted(e["id"] for e in data["flagged"])
        assert ids == sorted(["base"] + [f"item-{i}" for i in range(n_threads)]), ids
        leftovers = [p.name for p in td.iterdir() if p.name != "review_queue.json"]
        assert not leftovers, f"tmp/lock leftovers: {leftovers}"
        assert not list(td.glob("*.tmp*")), "no .tmp file may remain"


def test_curator_backfill_uses_locked_update_and_refuses_corrupt():
    from datetime import datetime, timezone

    curator = _load_script("curator")
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        flag_path = td / "flagged_issues.json"
        flag_path.write_text(json.dumps({
            "updated_at": None,
            "flagged": [{"id": "issue-1", "item_id": "item-1", "title": "t",
                         "description": "d", "severity": "high", "reporter": "r",
                         "status": "open", "created_at": "2026-01-01T00:00:00+00:00"}],
        }), encoding="utf-8")
        merged = {"updated_at": None, "flagged": [
            {"id": "item-1", "reason": "flagged-issue", "status": "approved"}]}
        now = datetime.now(timezone.utc)
        n = curator.backfill_flag_queue_decisions(flag_path, merged, now)
        assert n == 1, n
        fq = json.loads(flag_path.read_text(encoding="utf-8"))
        assert fq["flagged"][0]["status"] == "approved", fq
        assert fq["updated_at"] == now.isoformat(), fq

        # corrupt flag queue: refuse (warn + return 0), never overwrite
        flag_path.write_text("{not json", encoding="utf-8")
        n2 = curator.backfill_flag_queue_decisions(flag_path, merged, now)
        assert n2 == 0, n2
        assert flag_path.read_text(encoding="utf-8") == "{not json", "corrupt file must not be overwritten"

        # nothing decided -> no write
        flag_path.write_text(json.dumps({"updated_at": None, "flagged": [
            {"id": "issue-1", "item_id": "item-1", "status": "open"}]}), encoding="utf-8")
        merged_pending = {"updated_at": None, "flagged": [
            {"id": "item-1", "reason": "flagged-issue", "status": "pending"}]}
        assert curator.backfill_flag_queue_decisions(flag_path, merged_pending, now) == 0
        fq2 = json.loads(flag_path.read_text(encoding="utf-8"))
        assert fq2["flagged"][0]["status"] == "open" and fq2["updated_at"] is None, fq2


# ---------------------------------------------------------------------------
# 5. MCP: read scope forces dream dry_run=true; unset scope warns loudly
# ---------------------------------------------------------------------------

FAKE_BRIDGE = r'''
import json, sys, os
RECORD = os.environ.get("FAKE_BRIDGE_RECORD", "")
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except Exception:
        continue
    rid, cmd, args = req.get("id"), req.get("command"), req.get("args") or {}
    if RECORD:
        with open(RECORD, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"command": cmd, "args": args}) + "\n")
    if cmd == "curator":
        result = {"report": {"stale": 0, "not_stale": 1, "contradictions": 0, "isolated": 0,
                             "queue_written": not bool(args.get("dry_run")),
                             "queue_path": None,
                             "dry_run_received": args.get("dry_run")}}
    elif cmd == "log_audit":
        result = {"logged": True}
    else:
        result = {"ok": True}
    sys.stdout.write(json.dumps({"id": rid, "ok": True, "result": result}) + "\n")
    sys.stdout.flush()
'''

READ_TOOLS = ["discover_relevant", "dream", "get_project_context",
              "get_related", "list_recent", "search"]

UNSET_SCOPE_WARNING = ("SECONDBRAIN_MCP_SCOPE is unset — default scope exposes mutating tools "
                       "(add_knowledge/update_item/delete_item/link_items) with NO token gate; "
                       "set SECONDBRAIN_MCP_SCOPE=read for a read-only server")


class _McpProc:
    """Spawn node dist/index.js over stdio (newline-delimited JSON-RPC)."""

    def __init__(self, env: dict):
        entry = ROOT / "mcp-server" / "dist" / "index.js"
        if not entry.exists():
            raise AssertionError(f"built server missing: {entry} — run `npm run build` in mcp-server/ first")
        node = shutil.which("node")
        if not node:
            raise AssertionError("node not found on PATH")
        self.proc = subprocess.Popen(
            [node, str(entry)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, cwd=str(ROOT / "mcp-server"),
        )
        self.q: queue.Queue = queue.Queue()
        self.err_text = ""
        self._next_id = 1

        def _out():
            for line in self.proc.stdout:
                line = line.strip()
                if line:
                    try:
                        self.q.put(json.loads(line))
                    except json.JSONDecodeError:
                        pass

        def _err():
            # iterate by LINES so multi-byte UTF-8 (e.g. the em dash in the
            # warning) is never split across reads
            buf = []
            for line in self.proc.stderr:
                buf.append(line.decode("utf-8", "replace"))
                self.err_text = "".join(buf)

        threading.Thread(target=_out, daemon=True).start()
        threading.Thread(target=_err, daemon=True).start()

    def request(self, method, params=None, timeout=30.0):
        rid = self._next_id
        self._next_id += 1
        self.proc.stdin.write((json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                                           "params": params or {}}) + "\n").encode())
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                msg = self.q.get(timeout=max(0.1, deadline - time.time()))
            except queue.Empty:
                break
            if msg.get("id") == rid:
                return msg
        raise TimeoutError(f"no response to {method} within {timeout}s")

    def close(self):
        try:
            self.proc.kill()
        except Exception:
            pass
        self.proc.wait(timeout=10)


def _mcp_env(tmp: Path, *, scope: str | None):
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)  # no DB for these cases
    env.pop("SECONDBRAIN_WRITE_TOKEN", None)
    env["PYTHON"] = sys.executable
    env["BRIDGE_CWD"] = str(tmp)  # tmp contains the fake bridge.py
    env["SECONDBRAIN_AUDIT_LOG"] = str(tmp / "audit_log.jsonl")
    env["FAKE_BRIDGE_RECORD"] = str(tmp / "bridge_requests.jsonl")
    if scope is None:
        env.pop("SECONDBRAIN_MCP_SCOPE", None)
    else:
        env["SECONDBRAIN_MCP_SCOPE"] = scope
    return env


def test_mcp_read_scope_dream_forces_dry_run():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        (td / "bridge.py").write_text(FAKE_BRIDGE, encoding="utf-8")
        srv = _McpProc(_mcp_env(td, scope="read"))
        try:
            init = srv.request("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "fixb-test", "version": "0.1.0"}})
            assert init["result"]["protocolVersion"], init
            srv.proc.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
            srv.proc.stdin.flush()

            tools = srv.request("tools/list")["result"]["tools"]
            names = sorted(t["name"] for t in tools)
            assert names == READ_TOOLS, names
            dream = next(t for t in tools if t["name"] == "dream")
            assert "dry_run=true" in (dream.get("description") or ""), dream.get("description")

            r1 = srv.request("tools/call", {"name": "dream", "arguments": {}})
            r2 = srv.request("tools/call", {"name": "dream", "arguments": {"dry_run": False}})
            for r in (r1, r2):
                assert not r["result"].get("isError"), r
                payload = json.loads(r["result"]["content"][0]["text"])
                assert payload["report"]["dry_run_received"] is True, payload

            # the fake bridge recorded what the server actually sent
            recs = [json.loads(l) for l in (td / "bridge_requests.jsonl").read_text().splitlines() if l.strip()]
            curator_reqs = [r for r in recs if r["command"] == "curator"]
            assert len(curator_reqs) == 2, recs
            assert all(r["args"].get("dry_run") is True for r in curator_reqs), curator_reqs
        finally:
            srv.close()


def test_mcp_write_tool_audit_is_awaited_before_result():
    """Fix 2 (audit durability): after a MUTATING tool call, the log_audit
    bridge call (mcp_tool_write) must already have been sent BEFORE the tool
    result resolves — not fire-and-forget after it."""
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        (td / "bridge.py").write_text(FAKE_BRIDGE, encoding="utf-8")
        env = _mcp_env(td, scope="write")
        env["SECONDBRAIN_WRITE_TOKEN"] = "fixb-test-token"
        srv = _McpProc(env)
        try:
            init = srv.request("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "fixb-test", "version": "0.1.0"}})
            assert init["result"]["protocolVersion"], init
            srv.proc.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
            srv.proc.stdin.flush()
            r = srv.request("tools/call", {"name": "flag_issue", "arguments": {
                "title": "fixb audit-durability probe",
                "description": "probe: the mcp_tool_write event must be logged before this result resolves",
                "severity": "low", "reporter": "fixb"}})
            assert not r["result"].get("isError"), r
            # by the time the response arrived, the audit call was already sent
            recs = [json.loads(l) for l in (td / "bridge_requests.jsonl").read_text().splitlines() if l.strip()]
            writes = [x for x in recs if x["command"] == "log_audit"
                      and (x["args"].get("event") or {}).get("type") == "mcp_tool_write"]
            assert writes, recs
            ev = writes[0]["args"]["event"]
            assert ev["tool"] == "flag_issue", ev
            assert re.fullmatch(r"[0-9a-f]{16}", ev["args_sha256"]), ev
            audit_entries = [x for x in recs if x["command"] == "log_audit"]
            assert "fixb audit-durability probe" not in json.dumps(audit_entries), \
                "raw args never logged — only the fingerprint"
        finally:
            srv.close()


def test_mcp_unset_scope_loud_stderr_warning():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        (td / "bridge.py").write_text(FAKE_BRIDGE, encoding="utf-8")
        srv = _McpProc(_mcp_env(td, scope=None))
        try:
            deadline = time.time() + 30
            while time.time() < deadline and "stdio server ready" not in srv.err_text:
                time.sleep(0.1)
            assert UNSET_SCOPE_WARNING in srv.err_text, srv.err_text
        finally:
            srv.close()


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------

NO_DB_TESTS = (
    test_gate_redirect_to_non_allowlisted_refused,
    test_gate_redirect_to_allowlisted_followed_with_regate,
    test_gate_redirect_without_location_and_too_many_hops_raise,
    test_vision_redirect_to_non_allowlisted_refused,
    test_vision_redirect_to_allowlisted_followed_with_regate,
    test_fail_closed_extract_require_on_audit_missing,
    test_fail_closed_vision_require_on_audit_missing,
    test_require_unset_audit_missing_proceeds,
    test_external_fetch_youtube_logs_no_raw_url,
    test_external_fetch_extract_url_and_error_envelope_redacted,
    test_curator_locked_json_update_concurrency,
    test_curator_backfill_uses_locked_update_and_refuses_corrupt,
    test_mcp_read_scope_dream_forces_dry_run,
    test_mcp_write_tool_audit_is_awaited_before_result,
    test_mcp_unset_scope_loud_stderr_warning,
)

DB_TESTS = ()  # this suite is no-DB; the guard below still applies


def main() -> None:
    # locked guard: TEST_DATABASE_URL only, delimited dbname, refuse BEFORE any
    # connection (there are no DB tests here, but the guard is enforced anyway).
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        name = db_name(url)
        if not TEST_DBNAME_RE.search(name):
            print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
                  "point at a test database (delimited 'test' component).")
            sys.exit(1)
    else:
        print(SKIP_MSG + " (this suite is no-DB — all tests run anyway)")

    def _safe(exc) -> str:
        return str(exc).encode("ascii", "replace").decode("ascii")

    failed = 0
    for fn in NO_DB_TESTS:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 — report and continue
            failed += 1
            print(f"FAIL {fn.__name__}: {type(exc).__name__}: {_safe(exc)}")
        else:
            print(f"PASS {fn.__name__}")

    for fn in DB_TESTS:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {type(exc).__name__}: {_safe(exc)}")
        else:
            print(f"PASS {fn.__name__}")

    if failed:
        print(f"{failed} test(s) FAILED")
        sys.exit(1)
    print("All tests passed.")


if __name__ == "__main__":
    main()
