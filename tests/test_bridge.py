"""Tests for bridge.py — the JSON-lines CLI contract at repo root.

Runs `python -m bridge <command> '<json-args>'` via subprocess from the repo
root and asserts the locked envelope contract:

  * stdout is exactly ONE JSON line: {"ok": true, "result": ...} (exit 0)
    or {"ok": false, "error": "..."} (exit 1)

Guard pattern (locked — same invariant as tests/test_e2e.py):
  * TEST_DATABASE_URL is the ONLY accepted variable for selecting the test
    DB — there is deliberately no DATABASE_URL fallback.
  * The dbname must match (^|_)test(_|$) case-insensitively (stricter than a
    substring check, so e.g. "latest" is refused); otherwise the script
    prints REFUSING and exits 1 BEFORE any connection or destructive op.
  * If TEST_DATABASE_URL is unset or unreachable it prints
    "SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0.
  * DATABASE_URL is then set to the guarded test URL for the spawned bridge
    subprocesses (bridge.py reads DATABASE_URL).

With a test DB available the full round-trip runs:
  ingest -> search -> list_recent -> get_related -> link_items ->
  update_item -> delete_item, plus an error case (delete random uuid),
  and serve mode: spawns `python -m bridge serve`, speaks the JSON-lines
  protocol (ids 1..4: list_recent, ingest marker, search, delete_item
  cleanup), asserts id echo + ok:true, a malformed line -> {id: null,
  ok: false}, and exit 0 on stdin EOF. test_round_trip and test_serve_mode
  delete their own test:// rows in a finally block (parameterized), so an
  abort leaves nothing behind.
"""

import importlib.util
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def run_bridge(command, args, timeout=600):
    """Run one bridge command; assert the envelope contract; return the envelope."""
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", command, json.dumps(args)],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, (
        f"expected exactly 1 stdout line, got {len(lines)}: {proc.stdout[:400]!r}"
    )
    env = json.loads(lines[0])
    assert isinstance(env, dict) and isinstance(env.get("ok"), bool), env
    if env["ok"]:
        assert "result" in env and "error" not in env, env
        assert proc.returncode == 0, (proc.returncode, env)
    else:
        assert isinstance(env.get("error"), str) and env["error"], env
        assert "result" not in env, env
        assert proc.returncode == 1, (proc.returncode, env)
    return env


SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL.
    return os.environ.get("TEST_DATABASE_URL")


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


def cleanup_test_rows(pattern: str) -> None:
    """Delete this test's own rows (parameterized LIKE) so an abort leaves
    nothing behind. Runs against the guarded test DB (DATABASE_URL is set
    from TEST_DATABASE_URL in __main__ only)."""
    import psycopg

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        conn.execute(
            "DELETE FROM knowledge_items WHERE source_uri LIKE %s", (pattern,)
        )


MODEL_OK = importlib.util.find_spec("sentence_transformers") is not None


# --- envelope contract on error paths (no DB required) -----------------------

def test_unknown_command():
    env = run_bridge("bogus_command", {})
    assert env["ok"] is False and "unknown command" in env["error"]


def test_invalid_json_args():
    proc = subprocess.run(
        [sys.executable, "-m", "bridge", "search", "{not json"],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    assert len(lines) == 1, proc.stdout
    env = json.loads(lines[0])
    assert env["ok"] is False and "json" in env["error"]
    assert proc.returncode == 1


def test_missing_required_arg():
    env = run_bridge("search", {})
    assert env["ok"] is False and "query" in env["error"]


def test_empty_query_error():
    env = run_bridge("search", {"query": "   "})
    assert env["ok"] is False and "query" in env["error"]


def test_bad_uuid_error():
    env = run_bridge("delete_item", {"item_id": "not-a-uuid"})
    assert env["ok"] is False and "uuid" in env["error"]


def test_delete_random_uuid_error():
    # With the guarded test DB: "not found". Without a test DB this suite
    # exits SKIP before running anything. Either way the envelope must be
    # ok:false with exit 1.
    env = run_bridge("delete_item", {"item_id": str(uuid.uuid4())})
    assert env["ok"] is False and env["error"]


# --- DB-backed tests ----------------------------------------------------------

def test_list_recent_db():
    env = run_bridge("list_recent", {"limit": 5})
    assert env["ok"] is True, env
    assert isinstance(env["result"], list)
    assert len(env["result"]) <= 5
    for row in env["result"]:
        assert "id" in row and "content" in row and "created_at" in row


def test_get_related_missing_db():
    env = run_bridge("get_related", {"item_id": str(uuid.uuid4())})
    assert env["ok"] is False and "not found" in env["error"]


def _round_trip(uniq):
    # ingest two distinct notes
    env_a = run_bridge("ingest", {
        "source_type": "text",
        "payload": (
            f"The Second Brain stores knowledge in PostgreSQL with pgvector. "
            f"Hybrid retrieval merges vector, full-text and graph search. "
            f"Bridge round-trip note alpha {uniq}."
        ),
        "title": f"bridge round-trip alpha {uniq}",
        "source_uri": f"test://bridge/{uniq}/alpha",
    })
    assert env_a["ok"] is True, env_a
    res_a = env_a["result"]
    assert res_a["status"] in ("inserted", "duplicate"), res_a
    assert res_a["node_ids"], res_a
    item_a = res_a["node_ids"][0]

    env_b = run_bridge("ingest", {
        "source_type": "text",
        "payload": (
            f"GraphRAG answers multi-hop associative questions; vector RAG wins "
            f"on direct factoids. Bridge round-trip note beta {uniq}."
        ),
        "title": f"bridge round-trip beta {uniq}",
        "source_uri": f"test://bridge/{uniq}/beta",
    })
    assert env_b["ok"] is True, env_b
    item_b = env_b["result"]["node_ids"][0]
    assert item_b != item_a

    # search (hybrid, all methods)
    env = run_bridge("search", {
        "query": f"multi-hop retrieval methods {uniq}",
        "methods": ["vector", "fts", "graph"],
        "hops": 1, "top_k": 5, "candidate_k": 10,
    })
    assert env["ok"] is True, env
    result = env["result"]
    assert isinstance(result["results"], list) and result["results"], result
    assert set(result["by_method"].keys()) == {"vector", "fts", "graph"}
    assert any(r["id"] in (item_a, item_b) for r in result["results"])

    # list_recent contains the ingested items
    env = run_bridge("list_recent", {"limit": 20})
    assert env["ok"] is True, env
    recent_ids = [r["id"] for r in env["result"]]
    assert item_a in recent_ids and item_b in recent_ids

    # get_related on item_a
    env = run_bridge("get_related", {"item_id": item_a, "hops": 1})
    assert env["ok"] is True, env
    assert env["result"]["item"]["id"] == item_a
    assert isinstance(env["result"]["related"], list)

    # link_items a -> b (bidirectional)
    env = run_bridge("link_items", {
        "from_id": item_a, "to_id": item_b, "type": "related",
        "strength": 0.9, "direction": "both", "context": f"bridge test link {uniq}",
    })
    assert env["ok"] is True, env
    rel = env["result"]
    assert rel["from_id"] == item_a and rel["to_id"] == item_b
    assert rel["type"] == "related" and rel["direction"] == "bidirectional"
    assert abs(rel["strength"] - 0.9) < 1e-6

    # get_related now sees b through the link
    env = run_bridge("get_related", {"item_id": item_a, "hops": 1})
    assert env["ok"] is True, env
    assert any(r["id"] == item_b for r in env["result"]["related"]), env["result"]["related"]

    # update_item: title only (no re-embed needed)
    new_title = f"bridge round-trip alpha UPDATED {uniq}"
    env = run_bridge("update_item", {"item_id": item_a, "title": new_title})
    assert env["ok"] is True, env
    assert env["result"]["title"] == new_title
    assert env["result"]["id"] == item_a

    # update_item: content change -> re-embed
    new_content = f"Rewritten content {uniq}: sleep consolidates memory in the hippocampus."
    env = run_bridge("update_item", {
        "item_id": item_a, "content": new_content,
        "distillation_score": 0.95, "update_period": -1,
    })
    assert env["ok"] is True, env
    row = env["result"]
    assert row["content"] == new_content
    assert abs(row["distillation_score"] - 0.95) < 1e-6
    assert row["update_period"] == -1

    # search still finds the updated item by its new content
    env = run_bridge("search", {"query": f"sleep memory hippocampus {uniq}", "methods": ["vector"]})
    assert env["ok"] is True, env
    assert any(r["id"] == item_a for r in env["result"]["results"])

    # delete_item removes b and its relationships
    env = run_bridge("delete_item", {"item_id": item_b})
    assert env["ok"] is True, env
    assert env["result"] == {"deleted": item_b}

    env = run_bridge("get_related", {"item_id": item_b})
    assert env["ok"] is False and "not found" in env["error"]

    # get_related on a no longer sees b
    env = run_bridge("get_related", {"item_id": item_a})
    assert env["ok"] is True, env
    assert all(r["id"] != item_b for r in env["result"]["related"])

    # cleanup: delete a
    env = run_bridge("delete_item", {"item_id": item_a})
    assert env["ok"] is True and env["result"] == {"deleted": item_a}


def test_round_trip():
    uniq = uuid.uuid4().hex[:8]
    try:
        _round_trip(uniq)
    finally:
        # abort-safe: delete this test's own test:// rows (parameterized)
        cleanup_test_rows(f"test://bridge/{uniq}/%")


def test_delete_random_uuid_not_found_db():
    env = run_bridge("delete_item", {"item_id": str(uuid.uuid4())})
    assert env["ok"] is False and "not found" in env["error"]


def test_get_related_hops_clamped():
    """Fix G1 (audit-C/D): get_related clamps hops to 1..5 like cmd_search and
    exposes the effective value in result["params"]."""
    uniq = uuid.uuid4().hex[:8]
    item = None
    try:
        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": (f"G1 hops-clamp probe {uniq}: a small note about mycelium "
                        f"networks in forest soil."),
            "title": f"g1 hops clamp {uniq}",
            "source_uri": f"test://g1hops/{uniq}",
        })
        assert env["ok"] is True, env
        item = env["result"]["node_ids"][0]

        env = run_bridge("get_related", {"item_id": item, "hops": 999})
        assert env["ok"] is True, env
        assert env["result"]["params"]["hops"] == 5, env["result"].get("params")

        env = run_bridge("get_related", {"item_id": item, "hops": 0})
        assert env["ok"] is True, env
        assert env["result"]["params"]["hops"] == 1, env["result"].get("params")

        env = run_bridge("get_related", {"item_id": item, "hops": 3})
        assert env["ok"] is True, env
        assert env["result"]["params"]["hops"] == 3, env["result"].get("params")
    finally:
        if item:
            run_bridge("delete_item", {"item_id": item})
        cleanup_test_rows(f"test://g1hops/{uniq}%")


def test_update_item_metadata_merge_and_document_hash():
    """Fix G1 (audit-C/D): update_item MERGES metadata (jsonb ||) instead of
    replacing it, and on content change recomputes metadata.document_hash (and
    content_hash) from the new full content."""
    from pipeline.dedup import content_hash

    uniq = uuid.uuid4().hex[:8]
    item = None
    try:
        env = run_bridge("ingest", {
            "source_type": "text",
            "payload": f"G1 metadata-merge doc {uniq}: original content about mycelium networks.",
            "title": f"g1 meta {uniq}",
            "source_uri": f"test://g1meta/{uniq}",
        })
        assert env["ok"] is True, env
        item = env["result"]["node_ids"][0]

        # 1) metadata-only update MERGES: keeps the ingest-time keys, adds ours.
        env = run_bridge("update_item", {"item_id": item, "metadata": {"keep_me": "g1"}})
        assert env["ok"] is True, env
        meta = env["result"]["metadata"]
        assert meta.get("keep_me") == "g1", meta
        assert "document_hash" in meta and "content_hash" in meta, meta

        # 2) content change: metadata merges again (keep_me survives) and the
        #    document_hash version key is recomputed from the new content.
        new_content = (f"G1 metadata-merge doc {uniq}: rewritten content about "
                       f"tardigrade cryptobiosis.")
        env = run_bridge("update_item", {
            "item_id": item, "content": new_content, "metadata": {"second_key": 42},
        })
        assert env["ok"] is True, env
        row = env["result"]
        assert row["content"] == new_content, row["content"]
        meta = row["metadata"]
        assert meta.get("keep_me") == "g1", meta
        assert meta.get("second_key") == 42, meta
        expected = content_hash(new_content)
        assert meta.get("document_hash") == expected, (meta.get("document_hash"), expected)
        assert meta.get("content_hash") == expected, meta
    finally:
        if item:
            run_bridge("delete_item", {"item_id": item})
        cleanup_test_rows(f"test://g1meta/{uniq}%")


# --- serve mode (persistent JSON-lines process) --------------------------------


def test_serve_mode():
    """`python -m bridge serve`: one request per stdin line, one id-matched
    response line each; malformed line -> {"id": null, ok: false}; EOF -> exit 0."""
    import queue
    import threading

    proc = subprocess.Popen(
        [sys.executable, "-m", "bridge", "serve"],
        cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    lines: "queue.Queue" = queue.Queue()

    def reader():
        for line in proc.stdout:
            lines.put(line)
        lines.put(None)  # stdout closed

    def drain_stderr():
        for _ in proc.stderr:
            pass  # keep the stderr pipe from filling up and blocking the child

    threading.Thread(target=reader, daemon=True).start()
    threading.Thread(target=drain_stderr, daemon=True).start()

    def send_raw(text):
        proc.stdin.write(text + "\n")
        proc.stdin.flush()

    def send(obj):
        send_raw(json.dumps(obj))

    def recv(timeout=600):
        try:
            line = lines.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError("serve mode did not respond within %ss" % timeout)
        assert line is not None, "serve stdout closed before a response arrived"
        return json.loads(line)

    uniq = uuid.uuid4().hex[:8]
    marker = f"serve-mode-marker-{uniq}"
    item_id = None
    try:
        # id 1: list_recent
        send({"id": 1, "command": "list_recent", "args": {"limit": 5}})
        r = recv()
        assert r["id"] == 1 and r["ok"] is True and isinstance(r["result"], list), r

        # id 2: ingest a marker doc
        send({"id": 2, "command": "ingest", "args": {
            "source_type": "text",
            "payload": (f"Serve mode marker document {marker}: the persistent bridge "
                        f"process serves many commands after one model load."),
            "title": f"serve mode {uniq}",
            "source_uri": f"test://serve/{uniq}",
        }})
        r = recv()
        assert r["id"] == 2 and r["ok"] is True, r
        assert r["result"]["status"] in ("inserted", "duplicate"), r["result"]
        item_id = r["result"]["node_ids"][0]

        # id 3: search finds the marker doc
        send({"id": 3, "command": "search", "args": {
            "query": marker, "methods": ["vector", "fts"], "top_k": 5,
        }})
        r = recv()
        assert r["id"] == 3 and r["ok"] is True, r
        assert any(row.get("id") == item_id for row in r["result"]["results"]), \
            [row.get("id") for row in r["result"]["results"]]

        # id 4: delete_item cleanup
        send({"id": 4, "command": "delete_item", "args": {"item_id": item_id}})
        r = recv()
        assert r["id"] == 4 and r["ok"] is True and r["result"] == {"deleted": item_id}, r

        # malformed line -> {"id": null, "ok": false}
        send_raw("this is not json {")
        r = recv()
        assert r["id"] is None and r["ok"] is False and isinstance(r.get("error"), str), r
    finally:
        # abort-safe: delete this test's own test:// rows (parameterized)
        try:
            cleanup_test_rows(f"test://serve/{uniq}%")
        except Exception:
            pass
        try:
            proc.stdin.close()
        except Exception:
            pass
    rc = proc.wait(timeout=120)
    assert rc == 0, f"serve mode exit code {rc} (expected 0 on stdin EOF)"


# --- runner (same style as tests/test_pipeline.py) -----------------------------

if __name__ == "__main__":
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection or destructive op: the dbname must
    # match (^|_)test(_|$) case-insensitively (stricter than a substring check).
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — this "
              "test deletes knowledge_items rows.")
        sys.exit(1)

    if not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # bridge.py subprocesses read DATABASE_URL — point it at the guarded
    # test DB only (never at whatever DATABASE_URL the shell may carry).
    os.environ["DATABASE_URL"] = url

    if not MODEL_OK:
        print("SKIP ingest/search round-trip: sentence-transformers not installed")

    tests = sorted(
        (name, fn) for name, fn in list(globals().items())
        if name.startswith("test_") and callable(fn)
    )
    for name, fn in tests:
        if name in ("test_round_trip", "test_serve_mode", "test_get_related_hops_clamped",
                    "test_update_item_metadata_merge_and_document_hash") and not MODEL_OK:
            print(f"SKIP {name}")
            continue
        fn()
        print(f"PASS {name}")
    print("All tests passed.")
