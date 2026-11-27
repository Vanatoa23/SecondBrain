"""Tests for the GitHub repo import (Phase 2) — pipeline/github_import.py + the
bridge.py ingest source_type "github" wiring.

No-DB sections (URL parsing, mocked GitHub API, rate limit, bridge envelope)
always run and never touch the network or a database: requests.get inside
pipeline.github_import is mocked, and bridge subprocesses run with
DATABASE_URL removed from the environment.

Guarded DB section (runs last, name test_zz_db_smoke_guarded):
  * TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL fallback.
  * The dbname must match (^|_)test(_|$) case-insensitively, checked BEFORE any
    connection or destructive op; otherwise REFUSING + exit 1.
  * Unset or unreachable -> "SKIP: no test database available (set
    TEST_DATABASE_URL)" + exit 0.
  * With a test DB: schema applied to that DB only, a text-ingest sanity run
    through `python -m bridge ingest`, one github-style insert via the shared
    per-item helper (bridge._ingest_one) with a fake ExtractionResult, row
    verified then cleaned up. An optional live import of octocat/Hello-World
    prints file_count when the network allows, else SKIPs.
"""

import base64
import io
import json
import os
import re
import shutil
import subprocess
import sys
import unittest.mock as mock
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import bridge  # noqa: E402
from pipeline.extractors import ExtractionResult  # noqa: E402
from pipeline.github_import import import_github_repo, parse_github_url  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

REPO_URL = "https://github.com/octocat/Hello-World"

# --- fake GitHub API payloads -------------------------------------------------

README_TEXT = "# Hello World\n" + ("lorem ipsum dolor sit amet\n" * 100)  # ~2.7 KB
MAIN_TEXT = "print('hello')\n"
GUIDE_TEXT = "# Guide\n\nSome docs.\n"
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
LOCK_TEXT = "{" + '"lockfileVersion": 3,' + "}" * 1
BIG_TEXT = "x" * 300_000

FILES = {  # path -> (sha, text)
    "README.md": ("a" * 40, README_TEXT),
    "main.py": ("b" * 40, MAIN_TEXT),
    "docs/guide.md": ("c" * 40, GUIDE_TEXT),
    "logo.png": ("d" * 40, None),          # binary -> skipped by extension
    "package-lock.json": ("e" * 40, LOCK_TEXT),  # lockfile -> skipped by name
    "big.md": ("f" * 40, BIG_TEXT),        # > 200_000 bytes -> skipped by size
}


def _tree(branch_files=None):
    entries = [{"path": "src", "type": "tree", "sha": "0" * 40}]  # non-blob -> skipped
    for path, (sha, text) in FILES.items():
        size = len(text.encode()) if text is not None else len(PNG_BYTES)
        entries.append({"path": path, "type": "blob", "size": size, "sha": sha})
    return {"sha": "t" * 40, "tree": entries, "truncated": False}


class _FakeResp:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text else (json.dumps(payload) if payload is not None else "")

    def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload


def make_router(calls, default_branch="master", tree_branch=None, status=200):
    """A requests.get replacement routing the 3 API calls github_import makes."""
    tree_branch = tree_branch or default_branch

    def router(url, params=None, headers=None, timeout=None):
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        assert timeout == 30, f"timeout must be 30, got {timeout}"
        if status != 200:
            return _FakeResp(status, headers={"X-RateLimit-Remaining": "0"},
                             text="API rate limit exceeded for ...")
        if url == "https://api.github.com/repos/octocat/Hello-World":
            return _FakeResp(200, {"full_name": "octocat/Hello-World",
                                   "default_branch": default_branch})
        if url == f"https://api.github.com/repos/octocat/Hello-World/git/trees/{tree_branch}":
            assert params == {"recursive": "1"}, params
            return _FakeResp(200, _tree())
        if "/git/blobs/" in url:
            sha = url.rsplit("/", 1)[1]
            for path, (fsha, text) in FILES.items():
                if fsha == sha:
                    raw = text.encode() if text is not None else PNG_BYTES
                    return _FakeResp(200, {
                        "encoding": "base64",
                        "content": base64.b64encode(raw).decode(),
                    })
            raise AssertionError(f"unexpected blob sha {sha}")
        raise AssertionError(f"unexpected URL {url}")

    return router


def _patched_get(router):
    return mock.patch("pipeline.github_import.requests.get", side_effect=router)


# --- (a) URL parsing ------------------------------------------------------------

def test_parse_basic():
    assert parse_github_url("https://github.com/octocat/Hello-World") == \
        ("octocat", "Hello-World", None)


def test_parse_trailing_slash_and_http():
    assert parse_github_url("https://github.com/octocat/Hello-World/") == \
        ("octocat", "Hello-World", None)
    assert parse_github_url("http://github.com/o/r") == ("o", "r", None)


def test_parse_tree_branch():
    assert parse_github_url("https://github.com/octocat/Hello-World/tree/master") == \
        ("octocat", "Hello-World", "master")
    assert parse_github_url("https://github.com/octocat/Hello-World/tree/master/") == \
        ("octocat", "Hello-World", "master")
    assert parse_github_url("https://github.com/a-b/c.d_e/tree/feature-x") == \
        ("a-b", "c.d_e", "feature-x")


def _assert_value_error(url):
    try:
        parse_github_url(url)
    except ValueError:
        return
    raise AssertionError(f"parse_github_url({url!r}) did not raise ValueError")


def test_reject_non_github_host():
    _assert_value_error("https://gitlab.com/octocat/Hello-World")
    _assert_value_error("https://github.com.evil.com/o/r")
    _assert_value_error("https://api.github.com/repos/o/r")  # API host is not a web URL
    _assert_value_error("http://127.0.0.1:8080/o/r")
    _assert_value_error("http://169.254.169.254/o/r")


def test_reject_bad_scheme_and_malformed():
    _assert_value_error("ftp://github.com/o/r")
    _assert_value_error("file:///etc/passwd")
    _assert_value_error("git@github.com:o/r.git")
    _assert_value_error("not a url")
    _assert_value_error("")
    _assert_value_error("https://github.com/")           # no owner/repo
    _assert_value_error("https://github.com/onlyowner")  # no repo
    _assert_value_error("https://github.com/o/r/pulls")  # unsupported subpath
    _assert_value_error("https://github.com/o/r/tree")   # tree without branch


def test_reject_path_injection():
    _assert_value_error("https://github.com/o/r/../../x")
    _assert_value_error("https://github.com/o/r/tree/../..")
    _assert_value_error("https://github.com/o/r/tree/..")
    _assert_value_error("https://github.com/../r/x")
    _assert_value_error("https://github.com/o%2F..%2F..%2Fx/y")
    _assert_value_error("https://github.com/o/r/tree/bra%20nch")


# --- (b) mocked GitHub API ------------------------------------------------------

def test_import_mocked_happy_path():
    calls = []
    with _patched_get(make_router(calls)):
        results = import_github_repo(REPO_URL)
    # README.md + main.py + docs/guide.md; png/lockfile/big.md/tree-entry skipped
    assert len(results) == 3, [r.source_uri for r in results]
    by_path = {r.source_uri.split("/blob/master/")[1]: r for r in results}
    assert set(by_path) == {"README.md", "main.py", "docs/guide.md"}, set(by_path)
    for r in results:
        assert isinstance(r, ExtractionResult)
        assert r.mime_type == "text/plain"
        assert r.source_uri.startswith(
            "https://github.com/octocat/Hello-World/blob/master/")
    assert by_path["README.md"].text == README_TEXT
    assert by_path["main.py"].text == MAIN_TEXT
    assert by_path["docs/guide.md"].source_uri == \
        "https://github.com/octocat/Hello-World/blob/master/docs/guide.md"
    assert by_path["docs/guide.md"].text == GUIDE_TEXT
    # metadata + tree + 3 blob calls, all with timeout=30
    urls = [c["url"] for c in calls]
    assert urls[0] == "https://api.github.com/repos/octocat/Hello-World"
    assert any("/git/trees/master" in u for u in urls)
    assert sum("/git/blobs/" in u for u in urls) == 3
    assert all(c["timeout"] == 30 for c in calls)
    # no token in env -> no Authorization header
    assert all("Authorization" not in (c["headers"] or {}) for c in calls)


def test_import_explicit_branch_overrides_default():
    calls = []
    with _patched_get(make_router(calls, default_branch="master", tree_branch="main")):
        results = import_github_repo(REPO_URL + "/tree/main")
    assert len(results) == 3
    assert all("/blob/main/" in r.source_uri for r in results), \
        [r.source_uri for r in results]
    assert any("/git/trees/main" in c["url"] for c in calls)


def test_import_respects_max_files():
    calls = []
    with _patched_get(make_router(calls)):
        results = import_github_repo(REPO_URL, max_files=2)
    assert len(results) == 2
    assert sum("/git/blobs/" in c["url"] for c in calls) == 2  # no extra downloads


def test_import_respects_max_file_bytes():
    calls = []
    with _patched_get(make_router(calls)):
        results = import_github_repo(REPO_URL, max_file_bytes=1000)
    # README.md (~2.7 KB) and big.md (300 KB) exceed the cap -> skipped unfetched
    paths = {r.source_uri.split("/blob/master/")[1] for r in results}
    assert paths == {"main.py", "docs/guide.md"}, paths
    assert all(len(r.text.encode()) <= 1000 for r in results)


def test_import_uses_github_token_header():
    calls = []
    with mock.patch.dict(os.environ, {"GITHUB_TOKEN": "tok123"}):
        with _patched_get(make_router(calls)):
            import_github_repo(REPO_URL, max_files=1)
    assert calls, "no API calls recorded"
    assert all(c["headers"].get("Authorization") == "Bearer tok123" for c in calls)


# --- (c) rate limit -------------------------------------------------------------

def test_rate_limit_403_raises_runtime_error_with_token_hint():
    with _patched_get(make_router([], status=403)):
        try:
            import_github_repo(REPO_URL)
        except RuntimeError as exc:
            assert "GITHUB_TOKEN" in str(exc), str(exc)
            return
        raise AssertionError("403 did not raise RuntimeError")


# --- (d) bridge envelope, no DB -------------------------------------------------

def _no_db_env():
    return {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}


def _run_bridge_subprocess(args, timeout=120):
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "ingest", json.dumps(args)],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout,
        env=_no_db_env(),
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, f"expected exactly 1 stdout line, got {proc.stdout[:400]!r}"
    env = json.loads(lines[0])
    assert isinstance(env, dict) and isinstance(env.get("ok"), bool), env
    return proc.returncode, env


def test_bridge_subprocess_github_invalid_url_envelope():
    # Invalid (non-github) URL fails in URL parsing before any network/DB call.
    code, env = _run_bridge_subprocess(
        {"source_type": "github", "payload": "https://gitlab.com/o/r"})
    assert code == 1, (code, env)
    assert env["ok"] is False and isinstance(env.get("error"), str) and env["error"], env
    assert "result" not in env, env
    assert "github.com" in env["error"], env["error"]


def test_bridge_subprocess_github_nonstring_payload_envelope():
    code, env = _run_bridge_subprocess({"source_type": "github", "payload": 123})
    assert code == 1 and env["ok"] is False and "payload" in env["error"], (code, env)


def _run_main_envelope(args):
    """Run bridge.main in-process, capture the one stdout envelope line + exit code."""
    buf = io.StringIO()
    with mock.patch.dict(os.environ):
        os.environ.pop("DATABASE_URL", None)
        with redirect_stdout(buf):
            code = bridge.main(["bridge", "ingest", json.dumps(args)])
    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    assert len(lines) == 1, f"expected 1 envelope line, got {buf.getvalue()!r}"
    return code, json.loads(lines[0])


def test_bridge_github_mocked_empty_import_is_error_envelope():
    args = {"source_type": "github", "payload": REPO_URL}
    with mock.patch("pipeline.github_import.import_github_repo", return_value=[]):
        code, env = _run_main_envelope(args)
    assert code == 1 and env["ok"] is False, (code, env)
    assert "no files" in env["error"], env["error"]


def test_bridge_github_mocked_import_error_envelope():
    args = {"source_type": "github", "payload": REPO_URL}
    with mock.patch("pipeline.github_import.import_github_repo",
                    side_effect=RuntimeError("GitHub API returned 403 ... set GITHUB_TOKEN ...")):
        code, env = _run_main_envelope(args)
    assert code == 1 and env["ok"] is False, (code, env)
    assert "GITHUB_TOKEN" in env["error"], env["error"]


def test_bridge_github_mocked_one_item_no_db_is_error_envelope():
    item = ExtractionResult(text="hello github", mime_type="text/plain",
                            source_uri="https://github.com/o/r/blob/main/a.md")
    args = {"source_type": "github", "payload": REPO_URL}
    with mock.patch("pipeline.github_import.import_github_repo", return_value=[item]):
        code, env = _run_main_envelope(args)
    # no DATABASE_URL -> the single insert fails -> all-failed -> ok:false exit 1
    assert code == 1 and env["ok"] is False, (code, env)
    assert "DATABASE_URL" in env["error"], env["error"]


def _fake_items(n):
    return [ExtractionResult(text=f"file {i} content", mime_type="text/plain",
                             source_uri=f"https://github.com/o/r/blob/main/f{i}.md")
            for i in range(n)]


def test_bridge_github_status_aggregation():
    args = {"source_type": "github", "payload": REPO_URL}
    with mock.patch("pipeline.github_import.import_github_repo",
                    return_value=_fake_items(3)):
        # all inserted -> "inserted"
        with mock.patch("bridge._ingest_one",
                        side_effect=lambda *a, **k: {"status": "inserted", "node_ids": ["n"] * 1}):
            res = bridge.cmd_ingest(args)
        assert res["status"] == "inserted", res
        assert res["file_count"] == 3 and res["errors"] == [] and len(res["node_ids"]) == 3
        # mix of inserted + duplicate -> "duplicate-mix"
        replies = iter([{"status": "inserted", "node_ids": ["1"]},
                        {"status": "duplicate", "node_ids": ["2"]},
                        {"status": "inserted", "node_ids": ["3"]}])
        with mock.patch("bridge._ingest_one", side_effect=lambda *a, **k: next(replies)):
            res = bridge.cmd_ingest(args)
        assert res["status"] == "duplicate-mix", res
        assert res["node_ids"] == ["1", "2", "3"] and res["file_count"] == 3
        # one insert error -> "partial" with errors
        def flaky(*a, **k):
            if not flaky.done:
                flaky.done = True
                raise RuntimeError("db down")
            return {"status": "inserted", "node_ids": ["9"]}
        flaky.done = False
        with mock.patch("bridge._ingest_one", side_effect=flaky):
            res = bridge.cmd_ingest(args)
        assert res["status"] == "partial", res
        assert len(res["errors"]) == 1 and "db down" in res["errors"][0], res
        assert res["node_ids"] == ["9", "9"] and res["file_count"] == 3
        # all inserts fail -> raises (bridge turns this into ok:false exit 1)
        with mock.patch("bridge._ingest_one", side_effect=RuntimeError("db down")):
            try:
                bridge.cmd_ingest(args)
            except RuntimeError as exc:
                assert "all 3 item inserts failed" in str(exc), str(exc)
            else:
                raise AssertionError("expected RuntimeError when all inserts fail")


def test_bridge_github_passes_caps_through():
    args = {"source_type": "github", "payload": REPO_URL,
            "max_files": 7, "max_file_bytes": 12345}
    seen = {}

    def fake_import(url, max_files=None, max_file_bytes=None):
        seen["url"] = url
        seen["max_files"] = max_files
        seen["max_file_bytes"] = max_file_bytes
        return _fake_items(1)

    with mock.patch("pipeline.github_import.import_github_repo", side_effect=fake_import):
        with mock.patch("bridge._ingest_one",
                        return_value={"status": "inserted", "node_ids": ["x"]}):
            res = bridge.cmd_ingest(args)
    assert seen == {"url": REPO_URL, "max_files": 7, "max_file_bytes": 12345}, seen
    assert res["status"] == "inserted" and res["file_count"] == 1


# --- (e) guarded DB smoke --------------------------------------------------------

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


def _db_name(url):
    try:
        from psycopg import conninfo
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def test_zz_db_smoke_guarded():
    url = os.environ.get("TEST_DATABASE_URL")  # TEST_DATABASE_URL only, no fallback
    if not url:
        print(SKIP_MSG)
        return
    name = _db_name(url)
    if not TEST_DBNAME_RE.search(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (delimited 'test' component) — the smoke "
              "applies/drops the knowledge tables.")
        sys.exit(1)
    import psycopg
    try:
        conn = psycopg.connect(url, connect_timeout=5)
        conn.close()
    except Exception:
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        return

    import psycopg
    from psycopg.rows import dict_row

    # apply schema to the TEST db only (drops the two tables there first)
    schema_sql = (ROOT / "db" / "schema.sql").read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run([psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(ROOT / "db" / "schema.sql")],
                       check=True, capture_output=True, text=True)
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)
    print("PASS db_smoke setup_schema")

    os.environ["DATABASE_URL"] = url  # pipeline reads DATABASE_URL
    try:
        # model check — ingest embeds via bge-m3
        try:
            from pipeline.embed import embed_text
            embed_text("warmup")
            model_ok = True
        except Exception as e:
            model_ok = False
            print(f"SKIP: embedding model unavailable ({type(e).__name__}: {str(e)[:120]}) "
                  "— model-dependent smoke steps skipped")

        if model_ok:
            # sanity: single text ingest through the real bridge subprocess
            proc = subprocess.run(
                [sys.executable, "-m", "bridge", "ingest",
                 json.dumps({"source_type": "text",
                             "payload": "github smoke marker text ingest sanity",
                             "source_uri": "github://smoke/text"})],
                cwd=ROOT, capture_output=True, text=True, timeout=600,
                env={**os.environ, "DATABASE_URL": url},
            )
            lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
            assert len(lines) == 1, proc.stdout[:400]
            env = json.loads(lines[0])
            assert env["ok"] is True and env["result"]["status"] == "inserted", (proc.returncode, env)
            print("PASS db_smoke bridge text ingest sanity")

            # github-style insert via the shared per-item helper with a fake item
            item = ExtractionResult(
                text="fake repo file content for the insert path smoke test",
                mime_type="text/plain",
                source_uri="https://github.com/octocat/Hello-World/blob/master/README.md",
            )
            res = bridge._ingest_one("text", item.text, source_uri=item.source_uri,
                                     title="README.md")
            assert res["status"] == "inserted" and res["node_ids"], res
            with psycopg.connect(url, row_factory=dict_row) as conn:
                row = conn.execute(
                    "SELECT id, type, title, source_uri, metadata->>'document_hash' AS dh "
                    "FROM knowledge_items WHERE source_uri = %s",
                    (item.source_uri,),
                ).fetchone()
            assert row is not None, "inserted row not found"
            assert row["title"] == "README.md" and row["dh"], dict(row)
            print("PASS db_smoke github-style insert via shared helper (row verified)")

        # optional live import (acceptance 4) — real network, no token needed if
        # the unauthenticated rate limit allows; otherwise SKIP with a note.
        # Hello-World's only file is an extensionless "README", which the
        # extension allowlist skips -> file_count=0; Spoon-Knife has README.md
        # + index.html -> a real file_count > 0.
        live = None
        try:
            hw = import_github_repo("https://github.com/octocat/Hello-World", max_files=10)
            live = import_github_repo("https://github.com/octocat/Spoon-Knife", max_files=10)
            print(f"PASS live_import Hello-World file_count={len(hw)} "
                  f"Spoon-Knife file_count={len(live)}")
            assert len(live) > 0, "expected Spoon-Knife to yield text files"
            for r in live:
                assert r.source_uri.startswith(
                    "https://github.com/octocat/Spoon-Knife/blob/"), r.source_uri
        except Exception as e:
            print(f"SKIP live_import (network/rate limit unavailable): "
                  f"{type(e).__name__}: {str(e)[:160]}")

        # real end-to-end when network + model are available: the actual
        # `python -m bridge ingest {"source_type":"github", ...}` subprocess
        # against the test DB (live API, cannot be mocked across processes).
        if live and model_ok:
            proc = subprocess.run(
                [sys.executable, "-m", "bridge", "ingest",
                 json.dumps({"source_type": "github",
                             "payload": "https://github.com/octocat/Spoon-Knife"})],
                cwd=ROOT, capture_output=True, text=True, timeout=600,
                env={**os.environ, "DATABASE_URL": url},
            )
            lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
            assert len(lines) == 1, proc.stdout[:400]
            env = json.loads(lines[0])
            assert env["ok"] is True, (proc.returncode, env, proc.stderr[-400:])
            res = env["result"]
            assert res["source_type"] == "github" and res["file_count"] == len(live), res
            assert res["status"] in ("inserted", "duplicate-mix"), res
            assert res["node_ids"] and res["errors"] == [], res
            with psycopg.connect(url, row_factory=dict_row) as conn:
                n = conn.execute(
                    "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
                    ("https://github.com/octocat/Spoon-Knife/%",),
                ).fetchone()["n"]
            assert n == len(res["node_ids"]), (n, res["node_ids"])
            print(f"PASS db_smoke live bridge ingest github "
                  f"(status={res['status']} file_count={res['file_count']} "
                  f"nodes={len(res['node_ids'])} db_rows={n})")
    finally:
        with psycopg.connect(url) as conn:
            conn.execute(
                "DELETE FROM knowledge_items WHERE source_uri LIKE %s OR source_uri LIKE %s",
                ("https://github.com/%", "github://smoke/%"),
            )
        print("PASS db_smoke cleanup")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except SystemExit:
                raise
            except Exception as exc:  # noqa: BLE001 — report and continue
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"PASS {name}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
