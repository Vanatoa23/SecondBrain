"""Real-corpus MCP acceptance test against a REAL PostgreSQL + pgvector DB.

Phase 1 acceptance (ROADMAP.md): "Coding agent retrieves decision notes via MCP
in <5s". This test seeds ~40 synthetic decision notes into a throwaway test
database through the real pipeline (pipeline.ingest.ingest), spawns the real
MCP server (`node mcp-server/dist/index.js`, stdio), speaks MCP JSON-RPC 2.0
over stdio (newline-delimited, same framing as mcp-server/test/smoke.mjs),
asserts tools/list exposes exactly the 8 locked tools, then runs 5 fixed
search queries (each targeting a known decision note by marker/topic) through
tools/call search and asserts wall-clock latency < 5.0s per call plus the
expected source_uri/title in the top-5 results. Finally one write round-trip:
tools/call add_knowledge -> ok, then search for the unique marker -> note in
results. Per-query latency + PASS/FAIL and a min/p50/max summary are printed.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    # local pgvector server on 127.0.0.1:5433 (TRUST auth), e.g.
    createdb mcpaccept_test
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/mcpaccept_test
    python tests/test_mcp_acceptance.py

If the target database does not exist yet, this script creates it (the name
must already contain "test" — see the guard below).

Guard pattern (same as tests/test_e2e.py / tests/test_load.py):
  - TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops tables and must never be pointed
    at a real database.
  - REFUSE (exit 1) against any database whose name does not match (^|_)test(_|$)
    case-insensitively, checked BEFORE any connection or destructive operation.
  - SKIP (exit 0) when TEST_DATABASE_URL is unset or the server is
    unreachable, when node/npm are unavailable or the MCP server cannot be
    built, or when the bge models cannot be loaded.
  - All SQL is parameterized; cleanup (DELETE ... WHERE source_uri LIKE
    'mcp://%') runs in a finally block and the server process is killed.

The script sets os.environ["DATABASE_URL"] to the test URL for the in-process
pipeline and passes DATABASE_URL=<test url> (plus PYTHON and BRIDGE_CWD) in
the env of the spawned MCP server process.

"""

from __future__ import annotations

import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

N_NOTES = 40
LATENCY_BUDGET_S = 5.0
REQUEST_TIMEOUT_S = 300.0  # per JSON-RPC request (bridge cold start is slow)

EXPECTED_TOOLS = [
    "add_knowledge", "delete_item", "dream", "get_related",
    "link_items", "list_recent", "search", "update_item",
]

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

# 40 synthetic "decision notes": (topic, choice, reason). Each gets a unique
# FTS-friendly marker mcpdq<i>zq in its content.
DECISIONS = [
    ("the vector index for the knowledge base", "HNSW over IVFFlat", "HNSW gives higher recall at personal scale with no training step and simpler tuning"),
    ("the embedding model", "bge-m3 over hosted ada embeddings", "local private inference, 1024 dims, multilingual, no per-query API cost"),
    ("the reranker", "bge-reranker-v2-m3 cross-encoder", "best open-weight rerank quality that still runs on CPU within budget"),
    ("the primary database", "PostgreSQL with pgvector over a separate vector store", "one system of record for relational, graph, FTS and vector data"),
    ("the backup cadence", "nightly pg_dump plus WAL archiving", "cheap logical backup at personal scale with point-in-time recovery"),
    ("the chunk threshold", "whole documents up to 2000 tokens", "focused retrieval units with fewer fragment boundaries"),
    ("the default graph depth", "1 hop from vector seeds", "keeps graph expansion from flooding results with weak links"),
    ("the MCP transport", "stdio over HTTP with API keys", "local-only surface with zero auth management"),
    ("the dedup strategy", "content hash plus source_uri before the semantic check", "exact duplicates are caught cheaply before paying for embeddings"),
    ("staleness handling", "soft score penalty, never hard filtering", "stale knowledge must stay visible, only deprioritized"),
    ("the auto-link threshold", "cosine 0.75", "high enough to keep auto-links meaningful, low enough to catch near-duplicates"),
    ("the MCP tool surface", "eight tools with dream deferred to phase 2", "a small locked surface beats a big speculative one"),
    ("the raw document store", "raw bytes on disk keyed by content hash", "portability first, postgres holds the searchable layer"),
    ("the dropbox UI scope", "localhost-only insert and retrieve pages", "no network exposure for a personal tool"),
    ("the update_period heuristic", "permanent by default with event hints", "most notes do not decay; events do"),
    ("the distillation score default", "0.8 for own text notes, 0.1 for raw sources", "own insights rank above raw ingested material"),
    ("the FTS language config", "english tsvector over content and title", "predictable stemming for a primarily english corpus"),
    ("the vector search candidate pool", "20 candidates per method", "balances latency against recall on small corpora"),
    ("the relationships uniqueness", "unique on from_id, to_id, type", "idempotent auto-linking without duplicate edges"),
    ("the bridge contract", "one JSON line on stdout per command", "a simple testable boundary between TypeScript and python"),
    ("the tool error surface", "isError results, never a crashed server", "a coding agent must always get a well-formed answer"),
    ("the smoke test transport", "spawned over stdio with newline JSON-RPC", "tests the real wire protocol, not a mock"),
    ("the golden set size", "10 questions with recall@5 of at least 80 percent", "enough signal without overfitting the corpus"),
    ("the load test size", "200 documents", "big enough to expose scaling issues, small enough to run often"),
    ("the backup restore rehearsal", "dump, restore, verify counts and an FTS hit", "a backup that was never restored is not a backup"),
    ("the refusal guard in tests", "dbname must contain test before any destructive op", "tests must be physically unable to drop a real database"),
    ("the test database variable", "TEST_DATABASE_URL only, never DATABASE_URL", "one clearly named variable prevents pointing drops at production"),
    ("the embedding dimension", "1024 fixed by bge-m3", "the schema is locked to the model, not a configurable knob"),
    ("the excerpt parent link", "excerpts point at their document node", "keeps provenance from fragment to source"),
    ("the link direction mapping", "out, in, both mapped onto one_way and bidirectional", "one storage enum serves all API directions"),
    ("the reranker context budget", "candidate contents truncated at 4096 chars", "keeps rerank cost bounded regardless of document size"),
    ("the metadata content hash", "stored in metadata jsonb at insert time", "dedup and audit without recomputing"),
    ("the list_recent default", "20 rows newest first", "enough to eyeball recent capture without paging"),
    ("the delete semantics", "cascade relationships with the item", "no orphan edges left behind"),
    ("the graph expansion scoring", "strength times hop decay", "closer and stronger links rank higher"),
    ("the merge score combination", "max score across methods per node", "a node found by two methods keeps its best evidence"),
    ("the stdio framing", "newline-delimited JSON-RPC", "the MCP stdio transport contract"),
    ("the server resilience", "uncaught exceptions logged and ignored", "a hung bridge must not kill the agent's server"),
    ("the acceptance latency budget", "under 5 seconds per MCP search call", "the phase 1 promise to coding agents"),
    ("the candidate pool size before merging", "20 candidates per method", "wide enough for reranker choice, small enough for CPU cost"),
]

# 5 fixed search queries, each targeting a known decision note (index into
# DECISIONS) by topic keywords + its unique marker.
QUERY_TARGETS = [0, 7, 19, 28, 39]

WRITE_MARKER = "mcpacceptwrite9xk"
WRITE_SOURCE_URI = "mcp://acceptance-write"
WRITE_TITLE = "MCP acceptance note"


# --- guards -------------------------------------------------------------------

def get_test_url() -> str | None:
    # TEST_DATABASE_URL only — never fall back to DATABASE_URL: apply_schema()
    # DROPs the two tables and must never touch a non-test database.
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
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        import re
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)


def is_test_dbname(name: str) -> bool:
    return bool(TEST_DBNAME_RE.search(name or ""))


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
    """Idempotently apply db/schema.sql to the test DB (drops the two tables first)."""
    schema_path = REPO_ROOT / "db" / "schema.sql"
    schema_sql = schema_path.read_text()
    with psycopg.connect(url) as conn:
        conn.execute("DROP TABLE IF EXISTS relationships CASCADE")
        conn.execute("DROP TABLE IF EXISTS knowledge_items CASCADE")
    psql = shutil.which("psql")
    if psql:
        subprocess.run(
            [psql, url, "-v", "ON_ERROR_STOP=1", "-f", str(schema_path)],
            check=True, capture_output=True, text=True,
        )
    else:
        with psycopg.connect(url) as conn:
            conn.execute(schema_sql)  # no params -> simple protocol, multi-statement ok


def check_models() -> tuple[bool, str]:
    """Check the bge models are available WITHOUT loading them in-process.

    Loading torch + both models inside this test process (~2 GB) on top of the
    spawned MCP server + persistent bridge child can exhaust the Windows commit
    limit (os error 1455) and make every search fail. So: check the HF cache.
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


def marker_for(i: int) -> str:
    return f"mcpdq{i:02d}zq"


# --- corpus seeding -------------------------------------------------------------

def seed_corpus() -> list[dict]:
    """Seed N_NOTES synthetic decision notes via one-shot `python -m bridge ingest`.

    Seeding through short-lived subprocesses (instead of in-process
    pipeline.ingest) keeps this test process light: the spawned MCP server plus
    its persistent bridge child also load torch + bge models, and loading them
    in three processes at once can exhaust the Windows commit limit (os error
    1455), which surfaces as every `search` call failing with OSError 1455.
    """
    notes = []
    t0 = time.perf_counter()
    # One persistent `bridge serve` process for all ingests: pays the torch/model
    # load once, and keeps this test process itself model-free.
    serve = subprocess.Popen([sys.executable, "-m", "bridge", "serve"],
                             cwd=str(REPO_ROOT), stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    try:
        for i, (topic, choice, reason) in enumerate(DECISIONS):
            marker = marker_for(i)
            text = (f"Decision {i}: for {topic} we chose {choice} because {reason}. "
                    f"This is the canonical recorded decision for {topic}. Marker: {marker}.")
            req = {"id": i + 1, "command": "ingest",
                   "args": {"source_type": "text", "payload": text,
                            "source_uri": f"mcp://decision-{i}", "title": f"Decision {i}: {topic}"}}
            serve.stdin.write(json.dumps(req) + "\n")
            serve.stdin.flush()
            resp = json.loads(serve.stdout.readline())
            assert resp.get("id") == i + 1 and resp.get("ok") and resp["result"]["status"] == "inserted", (i, resp)
            notes.append({"index": i, "marker": marker, "topic": topic,
                          "source_uri": f"mcp://decision-{i}", "title": f"Decision {i}: {topic}"})
            if (i + 1) % 10 == 0:
                print(f"  seed progress {i + 1}/{N_NOTES} ({time.perf_counter() - t0:.1f}s)")
    finally:
        serve.stdin.close()
        serve.wait(timeout=60)
    print(f"PASS seed_corpus ({N_NOTES} decision notes ingested via one persistent "
          f"bridge serve process in {time.perf_counter() - t0:.1f}s)")
    return notes


# --- minimal MCP stdio client (framing copied from mcp-server/test/smoke.mjs) ---

class McpStdioClient:
    """Newline-delimited JSON-RPC 2.0 over the server's stdio pipes."""

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
                continue  # any non-JSON noise
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


def call_tool(client: McpStdioClient, name: str, arguments: dict):
    """tools/call; returns (result, parsed_text_payload_or_None)."""
    res = client.request("tools/call", {"name": name, "arguments": arguments})
    result = res.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"tools/call {name}: malformed response: {str(res)[:300]}")
    content = result.get("content")
    payload = None
    if (isinstance(content, list) and content
            and content[0].get("type") == "text" and isinstance(content[0].get("text"), str)):
        try:
            payload = json.loads(content[0]["text"])
        except json.JSONDecodeError:
            payload = None
    return result, payload


# --- MCP phase ------------------------------------------------------------------

def ensure_server_build() -> bool:
    """mcp-server/dist must exist; build it if missing. False -> SKIP."""
    dist_entry = REPO_ROOT / "mcp-server" / "dist" / "index.js"
    if dist_entry.exists():
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
    if r.returncode != 0 or not dist_entry.exists():
        print(f"SKIP: npm run build failed (rc={r.returncode}): {r.stderr[-300:]}")
        return False
    return True


def run_mcp_phase(url: str, notes: list[dict]) -> list[str]:
    """Spawn the MCP server, handshake, tools/list, 5 timed searches, write
    round-trip. Returns a list of failure messages (empty == all PASS)."""
    failures: list[str] = []

    def check(cond: bool, msg: str) -> bool:
        print(f"{'PASS' if cond else 'FAIL'} {msg}")
        if not cond:
            failures.append(msg)
        return cond

    node = shutil.which("node")
    server_env = {
        **os.environ,
        "DATABASE_URL": url,          # bridge subprocesses read this
        "PYTHON": "python",
        "BRIDGE_CWD": str(REPO_ROOT),
    }
    proc = subprocess.Popen(
        [node, "mcp-server/dist/index.js"],
        cwd=str(REPO_ROOT),
        env=server_env,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )

    def drain_stderr():
        for line in proc.stderr:
            s = line.rstrip()
            if s:
                print(f"[server stderr] {s}", file=sys.stderr)

    threading.Thread(target=drain_stderr, daemon=True).start()
    client = McpStdioClient(proc)
    try:
        # --- initialize + notifications/initialized --------------------------
        t0 = time.perf_counter()
        init = client.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "mcp-acceptance", "version": "0.1.0"},
        })
        dt = time.perf_counter() - t0
        server_info = (init.get("result") or {}).get("serverInfo") or {}
        check(server_info.get("name") == "secondbrain",
              f"initialize handshake ({dt:.3f}s, serverInfo={server_info})")
        client.notify("notifications/initialized")

        # --- tools/list: exactly the 8 locked tools ----------------------------
        lst = client.request("tools/list", {})
        tools = (lst.get("result") or {}).get("tools") or []
        names = sorted(t.get("name") for t in tools)
        check(names == sorted(EXPECTED_TOOLS) and len(tools) == 8,
              f"tools/list has exactly the 8 locked tools (got {len(tools)}: {names})")

        # --- 5 fixed timed search queries --------------------------------------
        latencies: list[float] = []
        for k, idx in enumerate(QUERY_TARGETS, 1):
            note = notes[idx]
            query = f"decision about {note['topic']} {note['marker']}"
            t0 = time.perf_counter()
            result, payload = call_tool(client, "search", {"query": query, "top_k": 5})
            dt = time.perf_counter() - t0
            latencies.append(dt)
            is_err = bool(result.get("isError"))
            results = (payload or {}).get("results") or []
            top5 = results[:5]
            uris = [r.get("source_uri") for r in top5]
            hit = note["source_uri"] in uris
            title_ok = any(r.get("source_uri") == note["source_uri"]
                           and str(r.get("title") or "").startswith(note["title"])
                           for r in top5)
            lat_ok = dt < LATENCY_BUDGET_S
            ok = (not is_err) and hit and title_ok and lat_ok
            verdict = "PASS" if ok else "FAIL"
            if not ok:
                failures.append(f"query {k} (target {note['source_uri']}): "
                                f"isError={is_err} latency={dt:.3f}s recall={hit} title={title_ok}")
            print(f"query {k}/5 target={note['source_uri']} latency={dt:.3f}s "
                  f"(budget {LATENCY_BUDGET_S}s, ok={lat_ok}) recall_top5={hit} "
                  f"title_ok={title_ok} isError={is_err} -> {verdict}")
            print(f"    top5 source_uris={uris}")

        lat = sorted(latencies)
        print(f"SUMMARY search latency min={lat[0]:.3f}s p50={statistics.median(lat):.3f}s "
              f"max={lat[-1]:.3f}s budget={LATENCY_BUDGET_S}s over {len(lat)} queries")

        # --- write round-trip: add_knowledge then search for the marker --------
        payload_text = (f"MCP acceptance write round-trip: this note proves the MCP "
                        f"add_knowledge tool inserts searchable knowledge. Unique marker: "
                        f"{WRITE_MARKER}.")
        t0 = time.perf_counter()
        add_result, add_payload = call_tool(client, "add_knowledge", {
            "source_type": "text",
            "payload": payload_text,
            "source_uri": WRITE_SOURCE_URI,
            "title": WRITE_TITLE,
        })
        add_dt = time.perf_counter() - t0
        add_ok = (not add_result.get("isError")) and (add_payload or {}).get("status") == "inserted"
        check(add_ok, f"add_knowledge write round-trip ({add_dt:.3f}s, "
                      f"isError={add_result.get('isError')}, status={(add_payload or {}).get('status')})")

        t0 = time.perf_counter()
        s_result, s_payload = call_tool(client, "search", {"query": WRITE_MARKER, "top_k": 5})
        s_dt = time.perf_counter() - t0
        s_results = (s_payload or {}).get("results") or []
        found = any(r.get("source_uri") == WRITE_SOURCE_URI or r.get("title") == WRITE_TITLE
                    for r in s_results[:5])
        check((not s_result.get("isError")) and found,
              f"search after write finds the new note ({s_dt:.3f}s, in_top5={found})")
        print(f"    write-search latency={s_dt:.3f}s top5_uris={[r.get('source_uri') for r in s_results[:5]]}")
    finally:
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:
            pass
    return failures


# --- cleanup --------------------------------------------------------------------

def cleanup(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # relationships cascade on knowledge_items delete
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", ("mcp://%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            ("mcp://%",),
        ).fetchone()["n"]
    print(f"PASS cleanup (mcp:// rows remaining: {remaining})")


# --- main -----------------------------------------------------------------------

def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection / destructive op: dbname must match
    # (^|_)test(_|$) case-insensitively (stricter than a substring check).
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    node = shutil.which("node")
    if not node:
        print("SKIP: node unavailable (cannot spawn the MCP server)")
        sys.exit(0)
    if not ensure_server_build():
        sys.exit(0)

    if not ensure_database(url) or not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ read DATABASE_URL — point them at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err})")
        sys.exit(0)
    print("PASS models (bge-m3 + bge-reranker-v2-m3 present in HF cache)")

    failures: list[str] = []
    try:
        notes = seed_corpus()
        with psycopg.connect(url, row_factory=dict_row) as conn:
            n = conn.execute(
                "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
                ("mcp://decision-%",),
            ).fetchone()["n"]
        if n != N_NOTES:
            failures.append(f"seeded row count {n} != {N_NOTES}")
            print(f"FAIL seeded row count {n} != {N_NOTES}")
        else:
            print(f"PASS seeded row count {n}")

        failures.extend(run_mcp_phase(url, notes))
    finally:
        try:
            cleanup(url)
        except Exception as e:
            print(f"WARNING: cleanup failed: {type(e).__name__}: {str(e)[:200]}")

    if failures:
        print(f"\nMCP ACCEPTANCE FAILED ({len(failures)} failure(s)):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nMCP ACCEPTANCE PASSED (all checks green)")
    sys.exit(0)


if __name__ == "__main__":
    main()
