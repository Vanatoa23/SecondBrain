"""Phase 4 acceptance e2e — the WHOLE Phase 4 loop end to end on a seeded corpus.

Exercises, in one guarded run against a throwaway test DB:

  1. seed ~8 items via ONE persistent `python -m bridge serve` child
     (cwd=<tmpdir>, PYTHONPATH=<repo root>, so review_queue.json lands in the
     tmpdir, not the repo root):
       - one stale-able item (text about a dated event) whose update_period is
         then forced to 1s via a parameterized SQL UPDATE (created_at backdated
         10 days) so the curator MUST flag it stale,
       - one permanent item (update_period=-1, verified in the DB),
       - one contradiction pair A / A' ingested in that order (A older) and
         linked A' -> A type="contradicts",
       - one isolated item (zero relationships),
       - three filler items (two explicitly linked, one isolated).
  2. bridge "curator" {dry_run:false} through the serve child -> report counts
     (stale>=1, contradictions>=1, isolated>=1), queue file written in the
     tmpdir with pending flags for stale/contradiction/isolated.
  3. assert the OLDER contradiction endpoint A got
     metadata.review_status="invalidated-pending-review" while its content is
     byte-unchanged (soft, metadata-only invalidation; A' untouched).
  4. assert retrieval search still RETURNS the invalidated item (soft — never
     hidden).
  5. run scripts/export_wiki.py as a subprocess with TEST_DATABASE_URL and
     --out <tmpdir>/wiki -> index.md + one page per seeded item, containing the
     item markers; the invalidated item's page carries the review marker.
  6. MCP dream: spawn `node mcp-server/dist/index.js` (build if missing) with
     DATABASE_URL=<test DB> (+TEST_DATABASE_URL), speak MCP JSON-RPC over
     stdio, tools/call dream -> not isError, report fields stale/contradictions/
     isolated are numbers >= 1.
  7. assert the queue JSON shape matches what dropbox GET /api/review-queue
     serves (readQueueFile: {"updated_at": ..., "flagged": [...]}; the
     review-decision endpoint matches on String(e.id)/String(e.reason) and
     writes e.status) — validate the exact keys here.
  8. cleanup in a finally block: delete the seeded rows (relationships
     cascade), kill the serve child + MCP server, remove the tmpdir (and the
     repo-root review_queue.json if the MCP dream run created it — it is a
     gitignored runtime artifact).

Guard pattern (locked, same as tests/test_contradiction.py / test_curator.py /
test_export_wiki.py / test_e2e.py):

    TEST_DATABASE_URL only (never DATABASE_URL) — apply_schema() drops the two
    tables, so the dbname must match the delimited (^|_)test(_|$) pattern or
    the run REFUSES with exit 1 BEFORE any connection. Unset or unreachable
    -> "SKIP" + exit 0 (also SKIP when the HF models or node are missing).
    The target DB (phase4acc_test) is created if missing. DATABASE_URL is set
    to the guarded test URL only for spawned children (bridge.py reads
    DATABASE_URL; scripts/curator.py / export_wiki.py read TEST_DATABASE_URL).

    # against the local trusted test server (127.0.0.1:5433, trust auth):
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/phase4acc_test
    python tests/test_phase4_acceptance.py

Torch-free test process: this file imports only psycopg + stdlib — all model
loading (bge-m3 embed, bge-reranker) happens inside the spawned serve/MCP
children. check_models() verifies the models are in the HF cache WITHOUT
loading them in-process (loading torch here on top of a serve child can
exhaust the Windows commit limit, os error 1455). The serve child is closed
BEFORE the MCP server (with its own bridge child) is spawned, so at most one
torch process is alive at any time. Run suites one at a time with
OMP_NUM_THREADS=2.

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
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
EXPORT_SCRIPT = REPO_ROOT / "scripts" / "export_wiki.py"
MCP_DIST_ENTRY = REPO_ROOT / "mcp-server" / "dist" / "index.js"
REPO_QUEUE = REPO_ROOT / "review_queue.json"

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

INVALIDATED = "invalidated-pending-review"
URI_PREFIX = "phase4acc://"

REQUEST_TIMEOUT_S = 300.0

# --- seeded corpus: 8 items, pairwise topically distinct so bge-m3 cosine
# stays under the 0.75 auto-link threshold (the isolated item must keep zero
# relationships) and under the 0.95 semantic-dedup threshold (all "inserted").
# Each carries a unique marker string for search/wiki assertions.
SEED_ORDER = ["stale", "permanent", "a", "ap", "isolated", "filler1", "filler2", "filler3"]
SEED = {
    "stale": {
        "title": "Hackathon schedule note",
        "source_uri": f"{URI_PREFIX}stale-event",
        "content": ("The campus hackathon starts on March 3 and the team registration "
                    "deadline is February 20. marker p4acc-stale-101. The event venue may "
                    "still change, so re-check the schedule weekly."),
        "marker": "p4acc-stale-101",
    },
    "permanent": {
        "title": "Euclid prime theorem note",
        "source_uri": f"{URI_PREFIX}permanent",
        "content": ("Euclid's theorem from the classic book Elements: there are infinitely "
                    "many prime numbers. marker p4acc-permanent-202. This is a permanent "
                    "mathematical fact, not an event."),
        "marker": "p4acc-permanent-202",
    },
    "a": {
        "title": "Build tool fact A",
        "source_uri": f"{URI_PREFIX}build-tool",
        "content": ("The build system is Gradle. marker p4acc-alpha-303. Build logic "
                    "lives in build.gradle and settings.gradle files."),
        "marker": "p4acc-alpha-303",
    },
    "ap": {
        "title": "Build tool fact A-prime",
        "source_uri": f"{URI_PREFIX}build-tool-v2",
        "content": ("The build system is Maven. marker p4acc-beta-404. The project "
                    "object model is declared in pom.xml."),
        "marker": "p4acc-beta-404",
    },
    "isolated": {
        "title": "Sourdough starter note",
        "source_uri": f"{URI_PREFIX}isolated",
        "content": ("Sourdough starter feeding routine: 50g rye flour plus 50g water at "
                    "room temperature every twelve hours. marker p4acc-iso-505. Kept in "
                    "the kitchen jar labeled jar-seven."),
        "marker": "p4acc-iso-505",
    },
    "filler1": {
        "title": "Kubernetes rollout note",
        "source_uri": f"{URI_PREFIX}k8s-rollout",
        "content": ("Kubernetes rolling update for the payments deployment: maxSurge 25 "
                    "percent and maxUnavailable zero. marker p4acc-k8s-606."),
        "marker": "p4acc-k8s-606",
    },
    "filler2": {
        "title": "Roman concrete note",
        "source_uri": f"{URI_PREFIX}roman-concrete",
        "content": ("Roman concrete uses volcanic ash pozzolana, which lets cracks "
                    "self-heal when seawater enters them. marker p4acc-concrete-707."),
        "marker": "p4acc-concrete-707",
    },
    "filler3": {
        "title": "Guitar practice note",
        "source_uri": f"{URI_PREFIX}guitar",
        "content": ("Guitar practice routine: chromatic warmup across strings one to six, "
                    "then barre chord shapes in first position. marker p4acc-guitar-808."),
        "marker": "p4acc-guitar-808",
    },
}


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
    (~2 GB) on top of a spawned serve child can exhaust the Windows commit
    limit (os error 1455). The children do the actual loading.
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


def ensure_server_build() -> bool:
    """mcp-server/dist must exist; build it if missing. False -> SKIP."""
    if MCP_DIST_ENTRY.exists():
        return True
    npm = shutil.which("npm")
    if not npm:
        print("SKIP: mcp-server/dist/index.js missing and npm unavailable (cannot build)")
        return False
    print("mcp-server/dist missing — running `npm run build` in mcp-server/")
    try:
        r = subprocess.run([npm, "run", "build"], cwd=str(REPO_ROOT / "mcp-server"),
                           capture_output=True, text=True, timeout=300)
    except Exception as e:
        print(f"SKIP: npm run build failed: {type(e).__name__}: {str(e)[:200]}")
        return False
    if r.returncode != 0 or not MCP_DIST_ENTRY.exists():
        print(f"SKIP: npm run build did not produce dist/index.js "
              f"(rc={r.returncode}): {r.stderr[-300:]}")
        return False
    return True


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
        self._err = open(queue_dir / "serve_stderr.log", "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(queue_dir), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self._err,
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
            rc = self.proc.wait(timeout=120)
        except Exception:
            self.proc.kill()
            rc = self.proc.wait(timeout=30)
        try:
            self._err.close()
        except Exception:
            pass
        return rc


# ---------------------------------------------------------------------------
# Minimal MCP stdio client (framing copied from mcp-server/test/smoke.mjs and
# tests/test_mcp_acceptance.py — newline-delimited JSON-RPC 2.0)
# ---------------------------------------------------------------------------

class McpStdioClient:
    def __init__(self, proc):
        self.proc = proc
        self._next_id = 1
        self._pending: dict[int, dict] = {}
        self._lock = threading.Lock()
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue  # non-JSON noise
            if isinstance(msg, dict) and "id" in msg:
                with self._lock:
                    entry = self._pending.pop(msg["id"], None)
                if entry is not None:
                    entry["msg"] = msg
                    entry["event"].set()

    def _send(self, obj):
        self.proc.stdin.write(json.dumps(obj) + "\n")
        self.proc.stdin.flush()

    def notify(self, method: str):
        self._send({"jsonrpc": "2.0", "method": method})

    def request(self, method: str, params: dict, timeout: float = REQUEST_TIMEOUT_S) -> dict:
        rid = self._next_id
        self._next_id += 1
        entry = {"event": threading.Event(), "msg": None}
        with self._lock:
            self._pending[rid] = entry
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        if not entry["event"].wait(timeout):
            with self._lock:
                self._pending.pop(rid, None)
            raise TimeoutError(f"no response to {method!r} within {timeout}s")
        return entry["msg"]


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


def fetch_contents(url: str, ids: dict) -> dict:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT id, content FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchall()
    return {str(r["id"]): r["content"] for r in rows}


def cleanup_rows(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", (URI_PREFIX + "%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
    assert remaining == 0, remaining
    print("PASS cleanup (phase4acc:// rows removed; relationships cascaded)")


# ---------------------------------------------------------------------------
# Seed + legs
# ---------------------------------------------------------------------------

def seed_via_serve(serve: ServeChild) -> dict:
    """Ingest the 8 seed items through the one persistent serve child."""
    ids: dict[str, str] = {}
    for key in SEED_ORDER:
        spec = SEED[key]
        if key == "ap":
            time.sleep(0.05)  # guarantee A' gets a strictly newer created_at than A
        r = serve.call("ingest", {"source_type": "text", "payload": spec["content"],
                                  "source_uri": spec["source_uri"], "title": spec["title"]},
                       timeout=600)
        assert r.get("ok") and r["result"]["status"] == "inserted", (key, r)
        assert len(r["result"]["node_ids"]) == 1, (key, r["result"])
        ids[key] = str(r["result"]["node_ids"][0])
    assert len(set(ids.values())) == len(SEED_ORDER), ids
    return ids


def force_stale(url: str, item_id: str) -> None:
    """Force the dated-event item stale via a parameterized UPDATE:
    update_period=1s with created_at backdated 10 days -> age >> period."""
    with psycopg.connect(url, row_factory=dict_row) as conn:
        row = conn.execute(
            "UPDATE knowledge_items SET update_period = %s, created_at = %s "
            "WHERE id = %s RETURNING id, update_period",
            (1, datetime.now(timezone.utc) - timedelta(days=10), item_id),
        ).fetchone()
        perm = conn.execute(
            "SELECT update_period FROM knowledge_items WHERE source_uri = %s",
            (SEED["permanent"]["source_uri"],),
        ).fetchone()
    assert row is not None and int(row["update_period"]) == 1, row
    assert perm is not None and int(perm["update_period"]) == -1, perm
    print("PASS forced staleness (update_period=1, created_at backdated 10d; "
          "permanent item verified update_period=-1)")


def link_seed(serve: ServeChild, ids: dict) -> None:
    links = [
        (ids["stale"], ids["permanent"], "relates_to", 0.8,
         "schedule note vs permanent fact"),
        (ids["ap"], ids["a"], "contradicts", 0.9,
         "A' says Maven, A says Gradle — deliberate Phase 4 acceptance contradiction"),
        (ids["filler1"], ids["filler2"], "relates_to", 0.5,
         "two infra notes linked for coverage"),
    ]
    for from_id, to_id, rtype, strength, context in links:
        r = serve.call("link_items", {"from_id": from_id, "to_id": to_id,
                                      "type": rtype, "strength": strength,
                                      "context": context})
        assert r.get("ok"), r
        assert r["result"]["type"] == rtype, r["result"]
    print(f"PASS seed links ({len(links)} explicit relationships)")


def run_export_wiki(url: str, out: Path) -> subprocess.CompletedProcess:
    """Run scripts/export_wiki.py as a subprocess with TEST_DATABASE_URL only."""
    env = dict(os.environ)
    env.pop("DATABASE_URL", None)
    env["TEST_DATABASE_URL"] = url
    cmd = [sys.executable, str(EXPORT_SCRIPT), "--out", str(out)]
    return subprocess.run(cmd, capture_output=True, text=True, env=env,
                          cwd=str(REPO_ROOT), timeout=180)


def spawn_mcp(url: str) -> tuple[subprocess.Popen, McpStdioClient]:
    node = shutil.which("node")
    assert node, "node unavailable"
    env = dict(os.environ)
    env["DATABASE_URL"] = url        # the MCP server's bridge child reads this
    env["TEST_DATABASE_URL"] = url   # passed through to bridge/curator subprocesses
    env["PYTHON"] = sys.executable
    env["BRIDGE_CWD"] = str(REPO_ROOT)
    proc = subprocess.Popen(
        [node, "mcp-server/dist/index.js"],
        cwd=str(REPO_ROOT), env=env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )

    def drain_stderr():
        for line in proc.stderr:
            s = line.rstrip()
            if s:
                print(f"[mcp stderr] {s}", file=sys.stderr)

    threading.Thread(target=drain_stderr, daemon=True).start()
    return proc, McpStdioClient(proc)


def validate_queue_shape(qdata: dict, ids: dict) -> None:
    """The queue file must match what dropbox GET /api/review-queue serves:
    readQueueFile() requires a top-level object with a "flagged" array and
    serves it as {ok:true, queue:{...}}; /api/review-decision matches entries
    on String(e.id)/String(e.reason) and writes e.status. scripts/curator.py
    writes exactly {"updated_at": iso, "flagged": [{id,title,source_uri,reason,
    evidence,status}, ...]}."""
    assert isinstance(qdata, dict), type(qdata)
    assert isinstance(qdata.get("updated_at"), str) and qdata["updated_at"], qdata.keys()
    flagged = qdata.get("flagged")
    assert isinstance(flagged, list) and flagged, flagged
    seeded = set(ids.values())
    for e in flagged:
        assert isinstance(e, dict), e
        assert {"id", "title", "source_uri", "reason", "evidence", "status"} <= set(e), e
        assert isinstance(e["id"], str) and e["id"] in seeded, e
        assert isinstance(e["reason"], str) and e["reason"], e
        assert isinstance(e["status"], str) and e["status"], e
    got = {(str(e["id"]), e["reason"]) for e in flagged}
    assert len(flagged) == len(got), "duplicate (id, reason) queue entries"
    for key, reason in (("stale", "stale"), ("a", "contradiction"),
                        ("ap", "contradiction"), ("isolated", "isolated")):
        assert (ids[key], reason) in got, (key, reason, got)
    assert all(e["status"] == "pending" for e in flagged), flagged


# ---------------------------------------------------------------------------
# DB-backed acceptance flow
# ---------------------------------------------------------------------------

def run_db_flow(url: str) -> None:
    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to the phase4acc test DB)")

    with tempfile.TemporaryDirectory() as td:
        work = Path(td)
        queue_dir = work  # serve child cwd -> review_queue.json lands here
        repo_queue_existed = REPO_QUEUE.exists()

        serve: ServeChild | None = None
        mcp_proc: subprocess.Popen | None = None
        try:
            # -- seed via ONE persistent serve child --------------------------
            serve = ServeChild(url, queue_dir)
            ids = seed_via_serve(serve)
            force_stale(url, ids["stale"])
            contents_before = fetch_contents(url, ids)
            assert len(contents_before) == len(SEED_ORDER), contents_before.keys()
            link_seed(serve, ids)
            print(f"PASS seed ({len(SEED_ORDER)} items ingested via one serve child)")

            # -- 1) bridge curator {dry_run:false} ----------------------------
            r = serve.call("curator", {"dry_run": False}, timeout=300)
            assert r.get("ok"), r
            result = r["result"]
            report = result["report"]
            assert result.get("delegated") is True, result
            assert isinstance(report["stale"], int) and report["stale"] >= 1, report
            assert isinstance(report["contradictions"], int) and report["contradictions"] >= 1, report
            assert isinstance(report["isolated"], int) and report["isolated"] >= 1, report
            assert report["queue_written"] is True and report["dry_run"] is False, report
            queue_path = Path(report["queue_path"])
            assert queue_path.name == "review_queue.json", queue_path
            assert queue_path.resolve().parent == queue_dir.resolve(), queue_path
            assert queue_path.exists(), "curator run wrote no queue file in the tmpdir"
            qdata = json.loads(queue_path.read_text(encoding="utf-8"))
            validate_queue_shape(qdata, ids)
            print(f"PASS curator run (stale={report['stale']} "
                  f"contradictions={report['contradictions']} isolated={report['isolated']} "
                  f"queue={queue_path})")

            # -- 2) soft invalidation of the OLDER contradiction endpoint -----
            with psycopg.connect(url, row_factory=dict_row) as conn:
                a = fetch_item(conn, ids["a"])
                ap = fetch_item(conn, ids["ap"])
            assert (a["metadata"] or {}).get("review_status") == INVALIDATED, a["metadata"]
            assert "review_status" not in (ap["metadata"] or {}), ap["metadata"]
            assert a["content"] == contents_before[ids["a"]], "A content changed"
            assert ap["content"] == contents_before[ids["ap"]], "A' content changed"
            print("PASS soft invalidation (older A = invalidated-pending-review, "
                  "A' untouched, contents byte-unchanged)")

            # -- 3) search still returns the invalidated item (soft) ----------
            r = serve.call("search", {"query": f"build system {SEED['a']['marker']}",
                                      "methods": ["vector", "fts"], "top_k": 10},
                           timeout=600)
            assert r.get("ok"), r
            result_ids = {str(row["id"]) for row in r["result"]["results"]}
            assert ids["a"] in result_ids, result_ids
            print(f"PASS search still returns the invalidated item "
                  f"({len(result_ids)} result(s) for A's marker)")

            # Close the serve child BEFORE spawning the MCP server (which runs
            # its own bridge child): one torch process at a time on the 16GB box.
            rc = serve.close()
            serve = None
            assert rc == 0, f"serve child exit code {rc} (expected 0 on stdin EOF)"
            print("PASS serve child shut down (stdin EOF, exit 0)")

            # -- 4) wiki export as a subprocess --------------------------------
            wiki_out = work / "wiki"
            cp = run_export_wiki(url, wiki_out)
            assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
            assert f"summary: items={len(SEED_ORDER)} " in cp.stdout, cp.stdout
            index = wiki_out / "index.md"
            assert index.is_file(), sorted(p.name for p in wiki_out.iterdir())
            index_text = index.read_text(encoding="utf-8")
            rows = [ln for ln in index_text.splitlines() if ln.startswith("| [")]
            assert len(rows) == len(SEED_ORDER), rows
            pages = sorted(p.name for p in wiki_out.glob("*.md"))
            assert len(pages) == len(SEED_ORDER) + 1, pages  # index.md + one page per item
            for key in SEED_ORDER:
                iid = ids[key]
                page = wiki_out / f"{iid}.md"
                assert page.is_file(), iid
                page_text = page.read_text(encoding="utf-8")
                assert SEED[key]["marker"] in page_text, (key, iid)
                assert f"](<{iid}.md>)" in index_text, iid
                assert SEED[key]["title"] in index_text, key
                assert SEED[key]["source_uri"] in index_text, key
            page_a = (wiki_out / f"{ids['a']}.md").read_text(encoding="utf-8")
            assert INVALIDATED in page_a, "export page must carry the review marker"
            print(f"PASS wiki export (index.md + {len(SEED_ORDER)} pages under {wiki_out}; "
                  "markers present; invalidated A carries the review marker)")

            # -- 5) MCP dream via the real stdio server ------------------------
            mcp_proc, client = spawn_mcp(url)
            try:
                init = client.request("initialize", {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "phase4-acceptance", "version": "0.1.0"},
                }, timeout=REQUEST_TIMEOUT_S)
                server_info = (init.get("result") or {}).get("serverInfo") or {}
                assert server_info.get("name") == "secondbrain", server_info
                client.notify("notifications/initialized")
                res = client.request("tools/call", {"name": "dream", "arguments": {}},
                                     timeout=REQUEST_TIMEOUT_S)
                mcp_result = res.get("result")
                assert isinstance(mcp_result, dict), res
                assert not mcp_result.get("isError"), mcp_result
                content = mcp_result.get("content") or []
                assert content and content[0].get("type") == "text", mcp_result
                payload = json.loads(content[0]["text"])
                rep = payload.get("report") or {}
                for k in ("stale", "contradictions", "isolated"):
                    v = rep.get(k)
                    assert isinstance(v, (int, float)) and not isinstance(v, bool), (k, v)
                assert rep["stale"] >= 1, rep
                assert rep["contradictions"] >= 1, rep
                assert rep["isolated"] >= 1, rep
                print(f"PASS MCP dream (not isError; report stale={rep['stale']} "
                      f"contradictions={rep['contradictions']} isolated={rep['isolated']})")
            finally:
                if mcp_proc is not None:
                    mcp_proc.kill()
                    mcp_proc.wait(timeout=30)
                    mcp_proc = None
            print("PASS MCP server shut down (its bridge child exits on stdin EOF)")

            # -- 6) queue JSON shape vs GET /api/review-queue ------------------
            # (dropbox review endpoints are covered by the dropbox smoke; here
            # we validate the file our curator run wrote matches the served shape)
            validate_queue_shape(qdata, ids)
            print("PASS queue JSON shape matches the /api/review-queue contract "
                  "(updated_at + flagged[] with id/title/source_uri/reason/evidence/status, "
                  "all pending, no duplicates)")
        finally:
            if serve is not None:
                serve.close()
            if mcp_proc is not None:
                try:
                    mcp_proc.kill()
                    mcp_proc.wait(timeout=30)
                except Exception:
                    pass
            # the MCP dream run writes review_queue.json at the repo root
            # (its bridge child cwd) — a gitignored runtime artifact; remove it
            # if this run created it
            if REPO_QUEUE.exists() and not repo_queue_existed:
                REPO_QUEUE.unlink()
            cleanup_rows(url)
        # TemporaryDirectory context removes the tmpdir (queue file included)


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
        print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
              "the serve/MCP children need them")
        sys.exit(0)

    if not shutil.which("node"):
        print("SKIP: node unavailable (cannot spawn the MCP server)")
        sys.exit(0)
    if not ensure_server_build():
        sys.exit(0)

    # bridge subprocesses read DATABASE_URL — point it at the guarded test DB only.
    os.environ["DATABASE_URL"] = url

    run_db_flow(url)
    print("All tests passed.")


if __name__ == "__main__":
    main()
