"""Security regression tests for fix worker G1 (second double audit, audit-C/audit-D).

Covers the bridge path-confinement fix (pdf/image/audio payloads are
server-side paths and must live under dropbox/inbox/, the
<tempdir>/secondbrain-staging staging dir, or a BRIDGE_PATH_ALLOWLIST entry)
and the serve-mode decode-robustness fix
(one undecodable stdin line must not kill the persistent loop).

No DB required: every bridge subprocess runs with DATABASE_URL stripped, so
any command that gets past the path check fails later at the DB layer with a
well-formed error envelope — which is exactly what lets us assert "the path
check passed" (the error is NOT 'path outside allowed roots').

    python tests/test_security_g1.py
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

INBOX = ROOT / "dropbox" / "inbox"

# A minimal valid one-page PDF (pypdf can extract "Hello G1" from it) — used
# for the positive path-check cases so the failure happens at the DB layer,
# not in the extractor.
MINI_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 44>>stream\n"
    b"BT /F1 24 Tf 100 700 Td (Hello G1) Tj ET\n"
    b"endstream\nendobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"trailer<</Root 1 0 R>>\n"
)


def run_bridge_no_db(command, args, extra_env=None, timeout=180):
    """Run one bridge command in a subprocess with DATABASE_URL stripped.

    Returns (proc, envelope). Asserts the one-JSON-line stdout contract.
    """
    env = {k: v for k, v in os.environ.items()
           if k not in ("DATABASE_URL", "TEST_DATABASE_URL", "BRIDGE_PATH_ALLOWLIST")}
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
    return proc, env_json


def assert_refused_outside_roots(proc, env_json):
    assert proc.returncode == 1, (proc.returncode, env_json)
    assert env_json["ok"] is False, env_json
    assert "outside allowed roots" in env_json["error"], env_json["error"]


def assert_passed_path_check(proc, env_json):
    """The path check passed: whatever failed later (no DB here), the error
    must NOT be the confinement refusal, and the envelope is well-formed."""
    assert proc.returncode == 1, (proc.returncode, env_json)  # fails later at the DB
    assert env_json["ok"] is False, env_json
    assert isinstance(env_json.get("error"), str) and env_json["error"], env_json
    assert "outside allowed roots" not in env_json["error"], env_json["error"]


# --- refusal: paths outside the allowed roots --------------------------------


def test_ingest_pdf_etc_passwd_refused():
    proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": "/etc/passwd"})
    assert_refused_outside_roots(proc, env)


def test_ingest_audio_outside_roots_refused_before_file_check():
    # Confinement runs BEFORE the audio isfile check: the error must be the
    # confinement refusal, not "not an existing file".
    proc, env = run_bridge_no_db("ingest", {"source_type": "audio", "payload": "/etc/passwd"})
    assert_refused_outside_roots(proc, env)
    assert "not an existing file" not in env["error"], env["error"]


def test_ingest_image_outside_roots_refused():
    proc, env = run_bridge_no_db(
        "ingest", {"source_type": "image", "payload": "C:/Windows/system32/config/sam"}
    )
    assert_refused_outside_roots(proc, env)


def test_ingest_pdf_traversal_through_inbox_refused():
    # A path that textually passes through dropbox/inbox but resolves outside
    # (realpath) must still be refused: inbox/../../bridge.py -> repo root.
    payload = str(INBOX / ".." / ".." / "bridge.py")
    assert (INBOX / ".." / ".." / "bridge.py").resolve().is_file()
    proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": payload})
    assert_refused_outside_roots(proc, env)


# --- allowed roots: staging temp dir + dropbox/inbox ---------------------------
# (audit W3: the whole system temp dir is no longer an allowed root — only
# <tempdir>/secondbrain-staging is, so other temp-dir files cannot be ingested)


def test_ingest_pdf_plain_tmpdir_file_refused():
    fd, name = tempfile.mkstemp(prefix="g1_pdf_", suffix=".pdf")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(MINI_PDF)
        proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": name})
        assert_refused_outside_roots(proc, env)
    finally:
        os.unlink(name)


def test_ingest_pdf_tmpdir_file_passes_path_check():
    staging = os.path.join(tempfile.gettempdir(), "secondbrain-staging")
    os.makedirs(staging, exist_ok=True)
    name = os.path.join(staging, f"g1_pdf_{uuid.uuid4().hex[:8]}.pdf")
    try:
        with open(name, "wb") as fh:
            fh.write(MINI_PDF)
        proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": name})
        assert_passed_path_check(proc, env)
    finally:
        os.unlink(name)


def test_ingest_pdf_inbox_file_passes_path_check():
    INBOX.mkdir(parents=True, exist_ok=True)
    target = INBOX / f"g1_test_{uuid.uuid4().hex[:8]}.pdf"
    target.write_bytes(MINI_PDF)
    try:
        proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": str(target)})
        assert_passed_path_check(proc, env)
    finally:
        target.unlink(missing_ok=True)


# --- BRIDGE_PATH_ALLOWLIST -----------------------------------------------------


def test_bridge_path_allowlist_env_respected():
    # A dir under the repo (NOT inbox, NOT temp) with a pdf in it.
    allow_a = ROOT / "tests" / f".g1allow_a_{uuid.uuid4().hex[:8]}"
    allow_b = ROOT / "tests" / f".g1allow_b_{uuid.uuid4().hex[:8]}"
    for d in (allow_a, allow_b):
        d.mkdir(parents=True)
    pdf_b = allow_b / "doc.pdf"
    pdf_b.write_bytes(MINI_PDF)
    try:
        # Without the allowlist: refused.
        proc, env = run_bridge_no_db("ingest", {"source_type": "pdf", "payload": str(pdf_b)})
        assert_refused_outside_roots(proc, env)
        # With an os.pathsep-separated allowlist covering the SECOND entry:
        # the path check passes.
        proc, env = run_bridge_no_db(
            "ingest", {"source_type": "pdf", "payload": str(pdf_b)},
            extra_env={"BRIDGE_PATH_ALLOWLIST": os.pathsep.join([str(allow_a), str(allow_b)])},
        )
        assert_passed_path_check(proc, env)
    finally:
        shutil.rmtree(allow_a, ignore_errors=True)
        shutil.rmtree(allow_b, ignore_errors=True)


# --- text/url/youtube/github unchanged (no path confinement) ------------------


def test_text_ingest_not_path_confined():
    # source_type text must NOT go through the path check: a payload that is
    # not a path at all still gets a normal (non-confinement) error envelope.
    proc, env = run_bridge_no_db(
        "ingest", {"source_type": "text", "payload": "just some text, not a path"}
    )
    assert proc.returncode == 1 and env["ok"] is False, (proc.returncode, env)
    assert "outside allowed roots" not in env["error"], env["error"]


# --- serve mode: one undecodable line must not kill the loop ------------------


def test_serve_mode_survives_undecodable_line():
    env = {k: v for k, v in os.environ.items()
           if k not in ("DATABASE_URL", "TEST_DATABASE_URL")}
    # Force a STRICT stdin decoder so the invalid bytes raise UnicodeDecodeError
    # in readline() (the default locale decoder may use surrogateescape/replace,
    # in which case the same line is caught by the malformed-JSON branch — the
    # loop survives either way).
    env["PYTHONIOENCODING"] = "utf-8:strict"
    proc = subprocess.Popen(
        [sys.executable, "-m", "bridge", "serve"],
        cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env=env,
    )
    try:
        # invalid UTF-8 line -> {"id": null, "ok": false} error envelope
        proc.stdin.write(b"\xff\xfe\x00 not utf8 \x9c\n")
        # a valid request afterwards -> the loop must still be alive and answer
        proc.stdin.write(json.dumps(
            {"id": 42, "command": "list_recent", "args": {"limit": 1}}
        ).encode("utf-8") + b"\n")
        proc.stdin.flush()
        line1 = proc.stdout.readline()
        line2 = proc.stdout.readline()
        assert line1 and line2, "serve did not answer both lines"
        r1 = json.loads(line1)
        r2 = json.loads(line2)
        assert r1["id"] is None and r1["ok"] is False and "unreadable" in r1["error"], r1
        assert r2["id"] == 42 and r2["ok"] is False, r2  # no DB -> error, but answered
        assert isinstance(r2.get("error"), str) and r2["error"], r2
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
    rc = proc.wait(timeout=180)
    assert rc == 0, f"serve exit code {rc} (expected 0 on stdin EOF)"


# --- H2: SKIP/connect-failed messages must never leak DB credentials ----------


def test_skip_message_safe_db_label_no_password_leak():
    # No DB needed. Keyword-form conninfo has no '@' at all, so a naive split
    # on the '@' separator would echo the whole conninfo — password included.
    # The SKIP/connect-failed message built with _safe_db_label must carry
    # host/dbname and never the password (URI form with an encoded '@' in the
    # password included).
    sys.path.insert(0, str(ROOT / "tests"))
    import test_backup
    import test_e2e
    for mod in (test_e2e, test_backup):
        kw = "host=dbhost.example port=5433 dbname=smoketest_test user=postgres password=secret"
        msg = f"(connect to {mod._safe_db_label(kw)} failed)"
        assert "dbhost.example" in msg and "smoketest_test" in msg, msg
        assert "secret" not in msg, msg
        uri = "postgresql://postgres:p%40ssw0rd@dbhost.example:5433/smoketest_test"
        msg2 = f"(connect to {mod._safe_db_label(uri)} failed)"
        assert "dbhost.example" in msg2 and "smoketest_test" in msg2, msg2
        assert "p%40ssw0rd" not in msg2 and "p@ssw0rd" not in msg2, msg2


# --- runner --------------------------------------------------------------------

if __name__ == "__main__":
    failed = 0
    tests = sorted(
        (name, fn) for name, fn in list(globals().items())
        if name.startswith("test_") and callable(fn)
    )
    for name, fn in tests:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 — report and continue
            failed += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
        else:
            print(f"PASS {name}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
