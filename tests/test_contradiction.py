"""Phase 4 headline acceptance e2e: a deliberately ingested contradiction
invalidates the OLD fact (soft, metadata-only) within ONE curator run.

Flow (all writes/reads through ONE persistent `python -m bridge serve` child):

  1. ingest fact A  "The build system is Gradle. marker contradiction-alpha-771"
     with source_uri="fact://build-tool"
  2. ingest fact A' "The build system is Maven. marker contradiction-beta-882"
     with source_uri="fact://build-tool-v2" (different uri, newer created_at)
  3. link_items A' -> A type="contradicts"
  4. bridge "curator" command with dry_run=false
  5. assert: report.contradictions >= 1; the review queue flags BOTH A and A'
     with reason "contradiction"; the OLDER item A gets
     metadata.review_status == "invalidated-pending-review" while A' does not;
     both contents are byte-unchanged (never deleted); retrieval.search still
     RETURNS both items (soft invalidation never hides); a re-run of the curator
     adds no duplicate queue entries and does not re-invalidate A (the marker
     blocks it).
  6. cleanup: delete the fact:// rows (relationships cascade), remove the
     queue file, kill the serve child (all in finally).

Queue location: bridge's curator command accepts only {"dry_run"} — it
delegates to scripts/curator.py main([]), whose default queue path
("review_queue.json") is resolved against the serve child's CWD. So the serve
child is spawned with cwd=<tmpdir> and PYTHONPATH=<repo root>: the queue lands
in the tmpdir and NOTHING is written at the repo root.

Guard pattern (locked, same as tests/test_e2e.py / tests/test_curator.py):

    TEST_DATABASE_URL only (never DATABASE_URL) — apply_schema() drops the
    two tables, so the dbname must match the delimited (^|_)test(_|$) pattern
    or the run REFUSES with exit 1 BEFORE any connection. Unset or unreachable
    -> "SKIP" + exit 0. The target DB (contradiction_test) is created if
    missing. DATABASE_URL is then set to the guarded test URL for the spawned
    serve child (bridge.py reads DATABASE_URL; scripts/curator.py reads
    TEST_DATABASE_URL).

    # against the local trusted test server (127.0.0.1:5433, trust auth):
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/contradiction_test
    python tests/test_contradiction.py

Torch-free test process: this file imports only psycopg + stdlib — all model
loading (bge-m3 embed, bge-reranker) happens inside the one persistent serve
child. The HF-cache check below verifies the models are present WITHOUT
loading them in-process (loading torch here on top of the serve child can
exhaust the Windows commit limit, os error 1455); without them the run SKIPs.

All SQL is parameterized.
"""

import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

INVALIDATED = "invalidated-pending-review"
URI_PREFIX = "fact://"
FACT_A_URI = "fact://build-tool"
FACT_AP_URI = "fact://build-tool-v2"
MARKER_A = "contradiction-alpha-771"
MARKER_AP = "contradiction-beta-882"
# Deliberately different enough to stay under the 0.95 semantic-dedup threshold.
CONTENT_A = ("The build system is Gradle. marker contradiction-alpha-771. "
             "Build logic lives in build.gradle and settings.gradle files.")
CONTENT_AP = ("The build system is Maven. marker contradiction-beta-882. "
              "The project object model is declared in pom.xml.")


# ---------------------------------------------------------------------------
# Guard helpers (locked pattern)
# ---------------------------------------------------------------------------

def _safe_db_label(url: str) -> str:
    """Credential-free label for SKIP/connect-failed messages (host:port/dbname)."""
    try:
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
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


def ensure_database(url: str) -> bool:
    """Create the target DB if missing. Returns False if the server is unreachable."""
    info = conninfo.conninfo_to_dict(url)
    name = info.get("dbname") or ""
    if not name:
        return False
    for maint in ("postgres", "template1"):
        m = dict(info)
        m["dbname"] = maint
        try:
            conn = psycopg.connect(conninfo.make_conninfo(**{k: v for k, v in m.items() if v}),
                                   connect_timeout=5, autocommit=True)
        except Exception:
            continue
        try:
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                print(f"created test database {name!r}")
            return True
        finally:
            conn.close()
    return False


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


def check_models() -> tuple[bool, str]:
    """Check the bge models are in the HF cache WITHOUT loading them in-process.

    This test process must stay torch-free: loading torch + both models here
    (~2 GB) on top of the persistent serve child can exhaust the Windows
    commit limit (os error 1455). The serve child does the actual loading.
    """
    cache = Path.home() / ".cache" / "huggingface" / "hub"
    have_embed = (cache / "models--BAAI--bge-m3").exists()
    have_rerank = (cache / "models--BAAI--bge-reranker-v2-m3").exists()
    if have_embed and have_rerank:
        return True, ""
    missing = []
    if not have_embed:
        missing.append("bge-m3")
    if not have_rerank:
        missing.append("bge-reranker-v2-m3")
    return False, f"not in HF cache: {', '.join(missing)}"


# ---------------------------------------------------------------------------
# Persistent `python -m bridge serve` child (JSON-lines protocol)
# ---------------------------------------------------------------------------

class ServeChild:
    """One persistent `python -m bridge serve` process.

    cwd=queue_dir so the delegated scripts/curator.py default queue
    (review_queue.json, CWD-relative) lands in the tmpdir, not the repo root.
    PYTHONPATH carries the repo root so `python -m bridge` resolves from there.
    """

    def __init__(self, url: str, queue_dir: Path):
        env = dict(os.environ)
        env["DATABASE_URL"] = url        # pipeline.db / retrieval.db
        env["TEST_DATABASE_URL"] = url   # scripts/curator.py delegation in the child
        env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + env["PYTHONPATH"]
                                               if env.get("PYTHONPATH") else "")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(queue_dir), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, bufsize=1,
        )
        self._next_id = 0
        self._lines: "queue.Queue" = queue.Queue()

        def reader():
            for line in self.proc.stdout:
                self._lines.put(line)
            self._lines.put(None)  # stdout closed

        threading.Thread(target=reader, daemon=True).start()

    def call(self, command: str, args: dict, timeout: float = 600) -> dict:
        self._next_id += 1
        rid = self._next_id
        req = {"id": rid, "command": command, "args": args}
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        try:
            line = self._lines.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"serve child did not respond to {command!r} within {timeout}s")
        assert line is not None, f"serve stdout closed before {command!r} was answered"
        resp = json.loads(line)
        assert resp.get("id") == rid, (rid, resp)
        return resp

    def close(self) -> int:
        """EOF -> clean exit 0; kill only as a fallback. Always reaps the child."""
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            return self.proc.wait(timeout=120)
        except Exception:
            self.proc.kill()
            return self.proc.wait(timeout=30)


# ---------------------------------------------------------------------------
# Direct-SQL helpers (test process — parameterized only)
# ---------------------------------------------------------------------------

def fetch_item(conn, item_id: str) -> dict:
    row = conn.execute(
        "SELECT id, content, source_uri, created_at, metadata FROM knowledge_items WHERE id = %s",
        (item_id,),
    ).fetchone()
    assert row is not None, f"item {item_id} missing"
    return dict(row)


def cleanup_fact_rows(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", (URI_PREFIX + "%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
    assert remaining == 0, remaining
    print("PASS cleanup (fact:// rows removed; relationships cascaded)")


# ---------------------------------------------------------------------------
# DB-backed acceptance flow
# ---------------------------------------------------------------------------

def run_db_flow(url: str) -> None:
    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to the contradiction test DB)")

    with tempfile.TemporaryDirectory() as td:
        queue_dir = Path(td)
        serve = ServeChild(url, queue_dir)
        try:
            # -- 1+2: ingest fact A, then fact A' (different uri, newer created_at)
            r = serve.call("ingest", {"source_type": "text", "payload": CONTENT_A,
                                      "source_uri": FACT_A_URI, "title": "Build tool fact A"})
            assert r.get("ok") and r["result"]["status"] == "inserted", r
            id_a = r["result"]["node_ids"][0]
            time.sleep(0.05)  # guarantee A' gets a strictly newer created_at
            r = serve.call("ingest", {"source_type": "text", "payload": CONTENT_AP,
                                      "source_uri": FACT_AP_URI, "title": "Build tool fact A-prime"})
            assert r.get("ok") and r["result"]["status"] == "inserted", r
            id_ap = r["result"]["node_ids"][0]
            assert id_a != id_ap
            print("PASS ingest A + A' via one persistent serve child")

            with psycopg.connect(url, row_factory=dict_row) as conn:
                a = fetch_item(conn, id_a)
                ap = fetch_item(conn, id_ap)
            assert a["content"] == CONTENT_A and ap["content"] == CONTENT_AP
            assert a["source_uri"] == FACT_A_URI and ap["source_uri"] == FACT_AP_URI
            assert a["created_at"] < ap["created_at"], (a["created_at"], ap["created_at"])
            contents_before = {id_a: a["content"], id_ap: ap["content"]}
            print("PASS facts stored (A older than A', distinct source_uri)")

            # -- 3: link A' -> A type="contradicts"
            r = serve.call("link_items", {"from_id": id_ap, "to_id": id_a,
                                          "type": "contradicts", "strength": 0.9,
                                          "context": "A' says Maven, A says Gradle — deliberate contradiction"})
            assert r.get("ok"), r
            assert r["result"]["type"] == "contradicts", r["result"]
            assert str(r["result"]["from_id"]) == id_ap and str(r["result"]["to_id"]) == id_a
            print("PASS link_items A' -> A type=contradicts")

            # -- 4: one curator run via the serve child (dry_run=false)
            r = serve.call("curator", {"dry_run": False})
            assert r.get("ok"), r
            result = r["result"]
            report = result["report"]
            assert result.get("delegated") is True, result
            assert report["contradictions"] >= 1, report
            assert report["queue_written"] is True and report["dry_run"] is False, report
            queue_path = Path(report["queue_path"])
            assert queue_path.name == "review_queue.json", queue_path
            assert queue_path.resolve().parent == queue_dir.resolve(), queue_path
            assert queue_path.exists(), "curator run wrote no queue file"
            print(f"PASS curator run 1 (report.contradictions={report['contradictions']}, "
                  f"queue={queue_path})")

            # -- 5a: queue flags BOTH endpoints with reason "contradiction"
            qdata = json.loads(queue_path.read_text(encoding="utf-8"))
            flagged = qdata["flagged"]
            got = {(str(e["id"]), e["reason"]) for e in flagged}
            assert (id_a, "contradiction") in got, got
            assert (id_ap, "contradiction") in got, got
            for e in flagged:
                if str(e["id"]) in (id_a, id_ap) and e["reason"] == "contradiction":
                    assert e["status"] == "pending", e
                    assert e["source_uri"] in (FACT_A_URI, FACT_AP_URI), e
            assert len(flagged) == len(got), "duplicate (id, reason) queue entries"
            print("PASS queue flags both A and A' with reason=contradiction (pending)")

            # -- 5b: soft invalidation — OLDER item A only; contents byte-unchanged
            with psycopg.connect(url, row_factory=dict_row) as conn:
                a2 = fetch_item(conn, id_a)
                ap2 = fetch_item(conn, id_ap)
            assert (a2["metadata"] or {}).get("review_status") == INVALIDATED, a2["metadata"]
            assert "review_status" not in (ap2["metadata"] or {}), ap2["metadata"]
            assert a2["content"] == contents_before[id_a], "A content changed"
            assert ap2["content"] == contents_before[id_ap], "A' content changed"
            meta_after_run1 = {id_a: a2["metadata"], id_ap: ap2["metadata"]}
            print("PASS soft invalidation (A=invalidated-pending-review, A' untouched, "
                  "contents byte-unchanged)")

            # -- 5c: retrieval.search still RETURNS both items (never hidden)
            r = serve.call("search", {"query": f"build system {MARKER_A}",
                                      "methods": ["vector", "fts"], "top_k": 10})
            assert r.get("ok"), r
            result_ids = {str(row["id"]) for row in r["result"]["results"]}
            assert id_a in result_ids, result_ids  # invalidated-pending is still visible
            assert id_ap in result_ids, result_ids
            print(f"PASS search still returns both items ({len(result_ids)} result(s) for "
                  f"A's marker {MARKER_A!r})")

            # -- 5d: re-run curator -> no duplicate queue entries, A not re-invalidated
            n_flagged_run1 = len(flagged)
            r = serve.call("curator", {"dry_run": False})
            assert r.get("ok") and r["result"]["report"]["contradictions"] >= 1, r
            qdata2 = json.loads(queue_path.read_text(encoding="utf-8"))
            flagged2 = qdata2["flagged"]
            got2 = [(str(e["id"]), e["reason"]) for e in flagged2]
            assert len(got2) == len(set(got2)), "re-run duplicated queue entries"
            assert len(flagged2) == n_flagged_run1, (len(flagged2), n_flagged_run1)
            assert sum(1 for e in flagged2 if str(e["id"]) == id_a
                       and e["reason"] == "contradiction") == 1
            with psycopg.connect(url, row_factory=dict_row) as conn:
                a3 = fetch_item(conn, id_a)
                ap3 = fetch_item(conn, id_ap)
            assert a3["metadata"] == meta_after_run1[id_a], "A re-invalidated / metadata changed"
            assert ap3["metadata"] == meta_after_run1[id_ap], "A' metadata changed on re-run"
            assert a3["content"] == contents_before[id_a] and ap3["content"] == contents_before[id_ap]
            print("PASS curator re-run idempotent (no duplicate flags, marker blocks "
                  "re-invalidation, contents unchanged)")
        finally:
            rc = serve.close()
            assert rc == 0, f"serve child exit code {rc} (expected 0 on stdin EOF)"
            print("PASS serve child shut down (stdin EOF, exit 0)")
            q = queue_dir / "review_queue.json"
            if q.exists():
                q.unlink()
            cleanup_fact_rows(url)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> None:
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        # Refusal guard BEFORE any connection: the dbname must carry a delimited
        # "test" component — apply_schema() drops the knowledge tables.
        name = db_name(url)
        if not TEST_DBNAME_RE.search(name or ""):
            print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
                  "point at a test database (dbname matching (^|_)test(_|$)) — this "
                  "test drops/recreates the knowledge tables.")
            sys.exit(1)

    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    if not ensure_database(url):
        print(SKIP_MSG)
        print(f"(server unreachable for {_safe_db_label(url)})")
        sys.exit(0)

    if not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable in-process check "
              f"({model_err}) — the bridge serve child needs them")
        sys.exit(0)

    # bridge subprocesses read DATABASE_URL — point it at the guarded test DB only.
    os.environ["DATABASE_URL"] = url

    run_db_flow(url)
    print("All tests passed.")


if __name__ == "__main__":
    main()
