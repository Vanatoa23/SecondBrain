"""Phase 5 acceptance e2e — multi-agent integration on a throwaway test DB.

Exercises, in one guarded run against a throwaway test database:

  1. SEED via ONE persistent `python -m bridge serve` child (models load once;
     DATABASE_URL=<test DB>, SECONDBRAIN_AUDIT_LOG + SECONDBRAIN_FLAG_QUEUE
     pointed at tmp files, cwd=<tmpdir>):
       - a project profile item (source_uri "project://kulgo-test", padded past
         the 2000-token chunk threshold so its row type is "document", content
         carries the project objectives),
       - two decision items + one lesson item (titles carry the
         Decision:/Lesson: markers the project_context decisions filter matches),
       - one contradiction-free baseline item with a unique search marker,
       - all non-profile items linked to the profile via link_items.
  2. TWO MCP server instances (`node mcp-server/dist/index.js` spawned twice):
     A with SECONDBRAIN_MCP_SCOPE=read, B with =write + SECONDBRAIN_WRITE_TOKEN.
     Handshake both; tools/list: A exposes EXACTLY the 6 read tools (search,
     get_related, list_recent, dream, get_project_context, discover_relevant —
     NOT add_knowledge/delete_item/flag_issue); B exposes flag_issue + the
     original 8 + the new read tools. Both call search on the seeded marker ->
     both get the hit (two runtimes retrieve without conflict).
  3. Instance B calls flag_issue twice with identical args -> flagged_issues.json
     has exactly ONE entry (id = sha256((item_id or "")+"\\n"+description)[:16]);
     once more with a different description -> 2 entries; knowledge_items /
     relationships row counts UNCHANGED (quarantine only, never the DB). The
     audit log contains mcp_write_scope_start, mcp_tool_write (flag_issue) and
     bridge_write (flag_issue) events.
  4. Write scope WITHOUT SECONDBRAIN_WRITE_TOKEN: the server process exits
     non-zero quickly (within ~10s, before the transport connects).
  5. get_project_context {project: "kulgo-test"} via B -> profile present +
     decisions/lessons found; discover_relevant {query: <lesson marker>} ->
     the lesson in the results; project_context on an unknown project -> isError.
  6. scripts/curator.py main(["--queue", tmp_queue]) with TEST_DATABASE_URL +
     SECONDBRAIN_FLAG_QUEUE -> the tmp queue gets reason "flagged-issue"
     pending entries; mark one decided in the queue file, re-run, assert the
     quarantine entry is back-filled with the decision.
  7. External gate: SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL=1,
     EXTRACT_API_URL=https://not-allowed.example/v1, EXTRACT_MODEL=x,
     EXTRACT_ENABLED=1 -> pipeline.extract.extract_knowledge(...) refuses
     (PermissionError or a sanitized error envelope), requests.post is NEVER
     called, and NO external_send event is logged for the host; then with
     SECONDBRAIN_EXTERNAL_ALLOWLIST=not-allowed.example and the POST mocked ->
     proceeds and an external_send event IS logged.
  8. Cleanup: delete the seeded rows (relationships cascade), remove the tmp
     files, assert remaining == 0; the throwaway test DB is dropped afterwards.

Guard pattern (locked, same as tests/test_phase4_acceptance.py /
tests/test_mcp_acceptance.py):

    TEST_DATABASE_URL only (never a DATABASE_URL fallback) — apply_schema()
    drops the two tables, so the dbname must match the delimited (^|_)test(_|$)
    pattern or the run REFUSES with exit 1 BEFORE any connection. Unset or
    unreachable -> "SKIP" + exit 0 (also SKIP when the HF models, node or the
    MCP build are missing). The target DB (phase5_test) is created if missing
    and DROPPED again at the end. DATABASE_URL is set to the guarded test URL
    only for spawned children + in-process helpers; the test PG is at
    127.0.0.1:5433 (never `localhost` in URLs) and the `postgres` DB is never
    used as a target (maintenance connections go to template1 first).

    # against the local trusted test server (127.0.0.1:5433, trust auth):
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/phase5_test
    python tests/test_phase5_acceptance.py

Torch discipline: this test process imports only psycopg + stdlib + requests
(pipeline/extract.py is loaded BY FILE PATH so pipeline/__init__ -> ingest ->
torch is never imported here). check_models() verifies the bge models are in
the HF cache WITHOUT loading them. The seed serve child is closed BEFORE the
MCP instances are spawned; instance A is killed right after its search, so at
most two torch processes (the two MCP bridge children, required by the
"two runtimes retrieve without conflict" leg) are alive at once. Run suites
ONE at a time with OMP_NUM_THREADS=2.

All SQL is parameterized.
"""

from __future__ import annotations

import hashlib
import importlib.util
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
MCP_DIST_ENTRY = REPO_ROOT / "mcp-server" / "dist" / "index.js"
CURATOR_SCRIPT = REPO_ROOT / "scripts" / "curator.py"
EXTRACT_SCRIPT = REPO_ROOT / "pipeline" / "extract.py"

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

REQUEST_TIMEOUT_S = 600.0  # per JSON-RPC request (bridge cold start is slow)

URI_PREFIX = "p5acc://"
PROJECT = "kulgo-test"
PROJECT_URI = f"project://{PROJECT}"
WRITE_TOKEN = "phase5-test-token"

# Locked MCP tool surfaces (SECONDBRAIN_MCP_SCOPE; unset = legacy default 8).
ORIGINAL_TOOLS = {
    "search", "add_knowledge", "get_related", "link_items",
    "update_item", "delete_item", "list_recent", "dream",
}
READ_TOOLS = {
    "search", "get_related", "list_recent", "dream",
    "get_project_context", "discover_relevant",
}
WRITE_TOOLS = READ_TOOLS | {
    "add_knowledge", "update_item", "delete_item", "link_items",
    "flag_issue", "review_decision",
}

SEARCH_MARKER = "p5acc-search-marker-42"
PROFILE_MARKER = "p5acc-profile-obj-11"
DEC1_MARKER = "p5acc-dec-1-22"
DEC2_MARKER = "p5acc-dec-2-33"
LESSON_MARKER = "p5acc-lesson-77"
LESSON_PHRASE = "always rerank the retrieved project context before answering"
FLAG_DESC_1 = "p5acc-flag-desc-AAA: the baseline note claims kulgo-test uses SQLite, which is wrong"
FLAG_DESC_2 = "p5acc-flag-desc-BBB: the first decision note omits the pgvector extension requirement"

# --- seed corpus: 5 items, pairwise topically distinct (auto-link threshold
# 0.75 / dedup 0.95 stay clear thanks to unique markers + distinct topics).
# No _EVENT_HINTS words ("event", "deadline", "starts", ...) so every item
# keeps update_period=-1 and the curator leg sees no stale flags; no
# "contradicts" edge anywhere (contradiction-free baseline).
_PROFILE_BODY = (
    "Kulgo-test project profile and objectives. "
    "Objective one: keep a single system of record for relational, graph, FTS and "
    "vector data so the project never splits state across tools. "
    "Objective two: run all embedding and reranking locally for privacy and zero "
    "per-query API cost. Objective three: expose knowledge to coding agents over a "
    "local stdio protocol with read and write scopes. "
    "Objective four: keep every mutation auditable and every external send gated. "
    + ("Planning background paragraph with roadmap context, constraints, and "
       "stakeholder notes repeated with slight variation to keep the document "
       "realistic and long enough to chunk. " * 120)
    + f" Marker {PROFILE_MARKER}."
)

SEED = [
    {
        "key": "profile",
        "title": "Project profile: kulgo-test",
        "source_uri": PROJECT_URI,
        "content": _PROFILE_BODY,
    },
    {
        "key": "decision1",
        "title": "Decision: kulgo-test stores everything in PostgreSQL with pgvector",
        "source_uri": f"{URI_PREFIX}decision-1",
        "content": (
            "Decision for the kulgo-test storage layer: one PostgreSQL database with "
            "the pgvector extension holds documents, relationships, full-text and "
            "vectors together. marker " + DEC1_MARKER + ". Rationale: a single system "
            "of record avoids sync bugs between a relational DB and a separate vector "
            "store."
        ),
    },
    {
        "key": "decision2",
        "title": "Decision: kulgo-test embeds locally with bge-m3 and reranks with the cross-encoder",
        "source_uri": f"{URI_PREFIX}decision-2",
        "content": (
            "Decision for the kulgo-test retrieval stack: embeddings come from the "
            "local bge-m3 model and reranking from the bge-reranker cross-encoder, "
            "both running on CPU. marker " + DEC2_MARKER + ". Rationale: private, "
            "offline, no per-query API cost."
        ),
    },
    {
        "key": "lesson",
        "title": "Lesson: rerank retrieved kulgo-test context before answering",
        "source_uri": f"{URI_PREFIX}lesson-1",
        "content": (
            "Lesson learned on kulgo-test: " + LESSON_PHRASE + ", because the raw "
            "vector ranking alone surfaces stale drafts. marker " + LESSON_MARKER +
            ". Recorded after a review round where an outdated note outranked the "
            "current architecture."
        ),
    },
    {
        "key": "baseline",
        "title": "Kulgo-test baseline architecture note",
        "source_uri": f"{URI_PREFIX}baseline",
        "content": (
            "Baseline architecture note for kulgo-test: the knowledge graph lives in "
            "Postgres, retrieval is hybrid vector plus full-text plus graph, and the "
            "curator pass runs on a schedule. marker " + SEARCH_MARKER + "."
        ),
    },
]

LINKS = [  # (from_key, type, context)
    ("decision1", "applies_to", "storage decision applies to project kulgo-test"),
    ("decision2", "applies_to", "retrieval stack decision applies to project kulgo-test"),
    ("lesson", "relates_to", "lesson learned while working on project kulgo-test"),
    ("baseline", "relates_to", "baseline architecture of project kulgo-test"),
]


# ---------------------------------------------------------------------------
# Guard helpers (locked pattern, copied from test_phase4_acceptance.py)
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


def _maintenance_connect(url: str, dbname: str):
    """Connect to a maintenance DB (template1 first, postgres as fallback —
    the locked pattern) for CREATE/DROP DATABASE. Never the target DB itself."""
    info = conninfo.conninfo_to_dict(url)
    for maint in (dbname, "template1", "postgres"):
        if maint == db_name(url):
            continue
        m = dict(info)
        m["dbname"] = maint
        try:
            return psycopg.connect(
                conninfo.make_conninfo(**{k: v for k, v in m.items() if v}),
                connect_timeout=5, autocommit=True,
            )
        except Exception:
            continue
    return None


def ensure_database(url: str) -> bool:
    """Create the target DB if missing. Returns False if the server is unreachable."""
    name = db_name(url)
    if not name:
        return False
    conn = _maintenance_connect(url, "template1")
    if conn is None:
        return False
    try:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            print(f"created test database {name!r}")
        return True
    finally:
        conn.close()


def drop_database(url: str) -> bool:
    """Drop the throwaway test DB (WITH FORCE). template1 only — the `postgres`
    DB is never touched. Returns False when no maintenance connection works."""
    name = db_name(url)
    if not name:
        return False
    conn = _maintenance_connect(url, "template1")
    if conn is None:
        return False
    try:
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        print(f"dropped test database {name!r}")
        return True
    finally:
        conn.close()


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
    """Check the bge models are in the HF cache WITHOUT loading them in-process."""
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
# Child-process env: test DB + tmp audit/flag files; extraction env removed so
# seeding never attempts an LLM call.
# ---------------------------------------------------------------------------

def child_env(url: str, work: Path, **extra) -> dict:
    env = dict(os.environ)
    env["DATABASE_URL"] = url
    env["TEST_DATABASE_URL"] = url
    env["SECONDBRAIN_AUDIT_LOG"] = str(work / "audit_log.jsonl")
    env["SECONDBRAIN_FLAG_QUEUE"] = str(work / "flagged_issues.json")
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + env["PYTHONPATH"]
                                           if env.get("PYTHONPATH") else "")
    for k in ("EXTRACT_ENABLED", "EXTRACT_API_URL", "EXTRACT_MODEL", "EXTRACT_API_KEY",
              "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL", "SECONDBRAIN_EXTERNAL_ALLOWLIST"):
        env.pop(k, None)
    env.update({k: v for k, v in extra.items() if v is not None})
    return env


# ---------------------------------------------------------------------------
# Persistent `python -m bridge serve` child (JSON-lines protocol)
# ---------------------------------------------------------------------------

class ServeChild:
    """One persistent `python -m bridge serve` process (cwd=work so any
    CWD-relative runtime artifact lands in the tmpdir, not the repo root)."""

    def __init__(self, url: str, work: Path):
        self._err = open(work / "serve_stderr.log", "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(work), env=child_env(url, work),
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
        self.proc.stdin.write(json.dumps({"id": rid, "command": command, "args": args}) + "\n")
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
# Minimal MCP stdio client (newline-delimited JSON-RPC 2.0, same framing as
# mcp-server/test/smoke.mjs and tests/test_mcp_acceptance.py)
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


def call_tool(client: McpStdioClient, name: str, arguments: dict, timeout: float = REQUEST_TIMEOUT_S):
    """tools/call; returns (result, parsed_text_payload_or_None)."""
    res = client.request("tools/call", {"name": name, "arguments": arguments}, timeout=timeout)
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


def spawn_mcp(url: str, work: Path, scope: str | None, token: str | None):
    """Spawn one `node mcp-server/dist/index.js` instance. scope=None leaves
    SECONDBRAIN_MCP_SCOPE unset (legacy default scope)."""
    node = shutil.which("node")
    assert node, "node unavailable"
    env = child_env(url, work,
                    PYTHON=sys.executable,
                    BRIDGE_CWD=str(REPO_ROOT),
                    SECONDBRAIN_MCP_SCOPE=scope,
                    SECONDBRAIN_WRITE_TOKEN=token)
    if scope is None:
        env.pop("SECONDBRAIN_MCP_SCOPE", None)
    if token is None:
        env.pop("SECONDBRAIN_WRITE_TOKEN", None)
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


def handshake(client: McpStdioClient, name: str) -> dict:
    init = client.request("initialize", {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": name, "version": "0.1.0"},
    })
    server_info = (init.get("result") or {}).get("serverInfo") or {}
    assert server_info.get("name") == "secondbrain", server_info
    client.notify("notifications/initialized")
    return server_info


def tool_names(client: McpStdioClient) -> list[str]:
    lst = client.request("tools/list", {})
    tools = (lst.get("result") or {}).get("tools") or []
    return sorted(t.get("name") for t in tools)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def table_counts(url: str) -> tuple[int, int]:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        items = conn.execute("SELECT count(*) AS n FROM knowledge_items").fetchone()["n"]
        rels = conn.execute("SELECT count(*) AS n FROM relationships").fetchone()["n"]
    return int(items), int(rels)


def read_flag_queue(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        flagged = data.get("flagged")
        return list(flagged) if isinstance(flagged, list) else []
    return list(data) if isinstance(data, list) else []


def read_audit_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def event_type(ev: dict) -> str:
    return str(ev.get("event") or ev.get("type") or "")


def wait_for_audit(path: Path, predicate, timeout: float = 15.0) -> list[dict]:
    deadline = time.time() + timeout
    events = []
    while time.time() < deadline:
        events = read_audit_events(path)
        if predicate(events):
            return events
        time.sleep(0.25)
    return events


def load_curator_module():
    spec = importlib.util.spec_from_file_location("sb_phase5_curator", CURATOR_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_extract_module():
    """Load pipeline/extract.py BY FILE PATH so pipeline/__init__ (-> ingest ->
    torch) is never imported into this test process."""
    spec = importlib.util.spec_from_file_location("sb_phase5_extract", EXTRACT_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# The acceptance flow
# ---------------------------------------------------------------------------

def run_db_flow(url: str, work: Path) -> list[str]:
    failures: list[str] = []

    def check(cond: bool, msg: str) -> bool:
        print(f"{'PASS' if cond else 'FAIL'} {msg}")
        if not cond:
            failures.append(msg)
        return cond

    apply_schema(url)
    check(True, "setup_schema (db/schema.sql applied to the phase5 test DB)")

    audit_path = work / "audit_log.jsonl"
    flag_path = work / "flagged_issues.json"
    tmp_queue = work / "review_queue.json"

    serve: ServeChild | None = None
    procs: list[subprocess.Popen] = []
    client_a = client_b = None
    ids: dict[str, str] = {}
    all_node_ids: list[str] = []

    try:
        # ------------------------------------------------------------------
        # Leg 1 — seed via ONE persistent serve child
        # ------------------------------------------------------------------
        try:
            serve = ServeChild(url, work)
            for spec in SEED:
                r = serve.call("ingest", {
                    "source_type": "text", "payload": spec["content"],
                    "source_uri": spec["source_uri"], "title": spec["title"],
                }, timeout=600)
                check(bool(r.get("ok")) and r["result"]["status"] == "inserted",
                      f"leg1 ingest {spec['key']} ({r.get('error', 'ok')})")
                node_ids = [str(n) for n in r["result"]["node_ids"]]
                ids[spec["key"]] = node_ids[0]
                all_node_ids.extend(node_ids)
            for from_key, rtype, context in LINKS:
                r = serve.call("link_items", {
                    "from_id": ids[from_key], "to_id": ids["profile"],
                    "type": rtype, "strength": 0.9, "context": context,
                })
                check(bool(r.get("ok")), f"leg1 link {from_key}->profile ({r.get('error', 'ok')})")
            with psycopg.connect(url, row_factory=dict_row) as conn:
                prow = conn.execute(
                    "SELECT type FROM knowledge_items WHERE source_uri = %s", (PROJECT_URI,)
                ).fetchone()
            check(prow is not None and prow["type"] == "document",
                  f"leg1 profile row type == 'document' (got {prow['type'] if prow else None})")
            counts_before = table_counts(url)
            check(len(ids) == len(SEED) and len(all_node_ids) >= len(SEED),
                  f"leg1 seed ({len(ids)} items, {counts_before[0]} rows, "
                  f"{counts_before[1]} relationships via one serve child)")
            rc = serve.close()
            serve = None
            check(rc == 0, f"leg1 serve child shut down (stdin EOF, exit {rc})")
        except Exception as e:
            check(False, f"leg1 seed failed: {type(e).__name__}: {e}")
            if serve is not None:
                serve.close()
                serve = None

        # ------------------------------------------------------------------
        # Leg 2 — two MCP instances (read scope A, write scope B)
        # ------------------------------------------------------------------
        try:
            proc_a, client_a = spawn_mcp(url, work, scope="read", token=None)
            procs.append(proc_a)
            handshake(client_a, "phase5-acceptance-read")
            check(True, "leg2 instance A (SECONDBRAIN_MCP_SCOPE=read) handshake")

            proc_b, client_b = spawn_mcp(url, work, scope="write", token=WRITE_TOKEN)
            procs.append(proc_b)
            handshake(client_b, "phase5-acceptance-write")
            check(True, "leg2 instance B (SECONDBRAIN_MCP_SCOPE=write + token) handshake")

            names_a = tool_names(client_a)
            check(set(names_a) == READ_TOOLS,
                  f"leg2 tools/list A == exactly the 6 read tools (got {names_a})")
            check(not ({"add_knowledge", "delete_item", "flag_issue"} & set(names_a)),
                  "leg2 tools/list A hides add_knowledge/delete_item/flag_issue")

            names_b = tool_names(client_b)
            check(WRITE_TOOLS <= set(names_b) and ORIGINAL_TOOLS <= set(names_b),
                  f"leg2 tools/list B contains flag_issue + original 8 + new read tools "
                  f"(got {names_b})")

            query = f"{SEARCH_MARKER} kulgo-test baseline architecture"
            for label, client in (("A", client_a), ("B", client_b)):
                result, payload = call_tool(client, "search", {"query": query, "top_k": 5})
                blob = json.dumps(payload or {})
                uris = [r.get("source_uri") for r in (payload or {}).get("results", [])]
                hit = (SEARCH_MARKER in blob) or (f"{URI_PREFIX}baseline" in uris)
                check((not result.get("isError")) and hit,
                      f"leg2 search via instance {label} hits the seeded marker "
                      f"(isError={result.get('isError')}, uris={uris})")
            check(True, "leg2 two MCP runtimes retrieved without conflict")

            # free one torch process before the remaining legs
            proc_a.kill()
            proc_a.wait(timeout=30)
            procs.remove(proc_a)
            client_a = None
            check(True, "leg2 instance A shut down after its search")
        except Exception as e:
            check(False, f"leg2 MCP instances failed: {type(e).__name__}: {e}")

        # ------------------------------------------------------------------
        # Leg 3 — flag_issue idempotency via write-scope instance B
        # ------------------------------------------------------------------
        if client_b is not None:
            try:
                args1 = {"item_id": ids["baseline"], "title": "Wrong storage claim",
                         "description": FLAG_DESC_1, "severity": "high",
                         "reporter": "w4-acceptance"}
                r1, p1 = call_tool(client_b, "flag_issue", args1)
                r2, p2 = call_tool(client_b, "flag_issue", dict(args1))  # identical
                check((not r1.get("isError")) and (not r2.get("isError")),
                      f"leg3 flag_issue x2 identical not isError "
                      f"({r1.get('isError')}, {r2.get('isError')})")
                entries = read_flag_queue(flag_path)
                expected_id = hashlib.sha256(
                    ((ids["baseline"]) + "\n" + FLAG_DESC_1).encode("utf-8")
                ).hexdigest()[:16]
                check(len(entries) == 1 and entries[0].get("id") == expected_id
                      and entries[0].get("status") == "pending",
                      f"leg3 flagged_issues.json has exactly ONE idempotent entry "
                      f"(got {len(entries)}, id={entries[0].get('id') if entries else None}, "
                      f"expected {expected_id})")

                args2 = {"item_id": ids["decision1"], "title": "Missing pgvector detail",
                         "description": FLAG_DESC_2, "severity": "medium",
                         "reporter": "w4-acceptance"}
                r3, p3 = call_tool(client_b, "flag_issue", args2)
                entries = read_flag_queue(flag_path)
                check((not r3.get("isError")) and len(entries) == 2,
                      f"leg3 different description -> 2 quarantine entries (got {len(entries)})")

                counts_after = table_counts(url)
                check(counts_after == counts_before,
                      f"leg3 DB row counts UNCHANGED by flag_issue "
                      f"(items {counts_before[0]}->{counts_after[0]}, "
                      f"rels {counts_before[1]}->{counts_after[1]})")

                def want_events(events):
                    types = [event_type(e) for e in events]
                    has_start = "mcp_write_scope_start" in types
                    has_tool = any(event_type(e) == "mcp_tool_write"
                                   and e.get("tool") == "flag_issue" for e in events)
                    has_bridge = any(event_type(e) == "bridge_write"
                                     and e.get("command") == "flag_issue" for e in events)
                    return has_start and has_tool and has_bridge

                events = wait_for_audit(audit_path, want_events)
                types = [event_type(e) for e in events]
                check(want_events(events),
                      f"leg3 audit log has mcp_write_scope_start + mcp_tool_write(flag_issue) "
                      f"+ bridge_write(flag_issue) (event types seen: {sorted(set(types))})")
            except Exception as e:
                check(False, f"leg3 flag_issue failed: {type(e).__name__}: {e}")
        else:
            check(False, "leg3 skipped: write-scope instance B unavailable")

        # ------------------------------------------------------------------
        # Leg 4 — write scope WITHOUT token must exit non-zero fast
        # ------------------------------------------------------------------
        try:
            node = shutil.which("node")
            env = child_env(url, work, SECONDBRAIN_MCP_SCOPE="write")
            env.pop("SECONDBRAIN_WRITE_TOKEN", None)  # no token on purpose
            env["PYTHON"] = sys.executable
            env["BRIDGE_CWD"] = str(REPO_ROOT)
            proc = subprocess.Popen(
                [node, "mcp-server/dist/index.js"],
                cwd=str(REPO_ROOT), env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1,
            )
            procs.append(proc)
            try:
                rc = proc.wait(timeout=10)
                check(rc != 0, f"leg4 write scope without SECONDBRAIN_WRITE_TOKEN "
                               f"exits non-zero quickly (rc={rc})")
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
                check(False, "leg4 write scope without token did NOT exit within 10s")
            procs.remove(proc)
        except Exception as e:
            check(False, f"leg4 no-token write scope failed: {type(e).__name__}: {e}")

        # ------------------------------------------------------------------
        # Leg 5 — project_context / discover_relevant via instance B
        # ------------------------------------------------------------------
        if client_b is not None:
            try:
                result, payload = call_tool(
                    client_b, "get_project_context",
                    {"project": PROJECT, "token_budget": 16000},
                )
                blob = json.dumps(payload or {})
                ok = (not result.get("isError")) and isinstance(payload, dict)
                profile_ok = ok and isinstance(payload.get("profile"), dict) \
                    and PROFILE_MARKER in blob
                decisions = (payload or {}).get("decisions") if ok else None
                decisions_ok = isinstance(decisions, list) and len(decisions) >= 1 \
                    and DEC1_MARKER in blob and DEC2_MARKER in blob and LESSON_MARKER in blob
                est = (payload or {}).get("estimated_tokens") if ok else None
                check(ok and profile_ok and decisions_ok
                      and isinstance(est, int) and est > 0,
                      f"leg5 get_project_context({PROJECT!r}) -> profile + decisions/lessons "
                      f"(isError={result.get('isError')}, decisions={len(decisions) if isinstance(decisions, list) else decisions}, "
                      f"estimated_tokens={est}, keys={sorted(payload.keys()) if isinstance(payload, dict) else None})")

                result, _ = call_tool(client_b, "get_project_context",
                                      {"project": "no-such-project-zzz"})
                check(bool(result.get("isError")),
                      f"leg5 get_project_context(unknown) -> isError (got isError={result.get('isError')})")

                result, payload = call_tool(
                    client_b, "discover_relevant",
                    {"query": f"{LESSON_MARKER} {LESSON_PHRASE} kulgo-test", "top_k": 10},
                )
                blob = json.dumps(payload or {})
                check((not result.get("isError")) and (LESSON_MARKER in blob),
                      f"leg5 discover_relevant(lesson marker) -> lesson in results "
                      f"(isError={result.get('isError')}, hit={LESSON_MARKER in blob})")
            except Exception as e:
                check(False, f"leg5 project_context/discover_relevant failed: "
                             f"{type(e).__name__}: {e}")
        else:
            check(False, "leg5 skipped: write-scope instance B unavailable")

        # kill B before the in-process legs (one torch process at a time)
        if client_b is not None:
            try:
                procs[-1].kill()
                procs[-1].wait(timeout=30)
            except Exception:
                pass
            for p in list(procs):
                try:
                    p.kill()
                    p.wait(timeout=10)
                except Exception:
                    pass
            procs.clear()
            client_b = None

        # ------------------------------------------------------------------
        # Leg 6 — curator merges flagged_issues.json as "flagged-issue"
        # ------------------------------------------------------------------
        try:
            os.environ["TEST_DATABASE_URL"] = url
            os.environ["SECONDBRAIN_FLAG_QUEUE"] = str(flag_path)
            curator = load_curator_module()
            rc = curator.main(["--queue", str(tmp_queue)])
            check(rc == 0, f"leg6 curator main(['--queue', tmp]) exit 0 (rc={rc})")
            qdata = json.loads(tmp_queue.read_text(encoding="utf-8"))
            flagged = qdata.get("flagged", [])
            fi = [e for e in flagged if e.get("reason") == "flagged-issue"]
            check(len(fi) == 2 and all(e.get("status") == "pending" for e in fi),
                  f"leg6 tmp queue has 2 pending 'flagged-issue' entries "
                  f"(got {[(e.get('id'), e.get('reason'), e.get('status')) for e in flagged]})")
            check({str(e.get("id")) for e in fi} == {ids["baseline"], ids["decision1"]},
                  "leg6 flagged-issue entries keyed by the flagged item ids")

            # mark one decided in the queue file, re-run, expect back-fill
            for e in fi:
                if str(e.get("id")) == ids["baseline"]:
                    e["status"] = "approved"
            tmp_queue.write_text(json.dumps(qdata, indent=2, default=str) + "\n", encoding="utf-8")
            rc = curator.main(["--queue", str(tmp_queue)])
            entries = read_flag_queue(flag_path)
            backfilled = [e for e in entries if e.get("status") in ("approved", "dismissed")]
            issue_id_1 = hashlib.sha256((ids["baseline"] + "\n" + FLAG_DESC_1)
                                        .encode("utf-8")).hexdigest()[:16]
            ok = (rc == 0 and len(backfilled) == 1
                  and (backfilled[0].get("item_id") == ids["baseline"]
                       or backfilled[0].get("id") == issue_id_1)
                  and backfilled[0].get("status") == "approved")
            check(ok, f"leg6 re-run back-fills the quarantine entry with the decision "
                      f"(rc={rc}, entries={[(e.get('id'), e.get('status')) for e in entries]})")
        except Exception as e:
            check(False, f"leg6 curator flag pickup failed: {type(e).__name__}: {e}")

        # ------------------------------------------------------------------
        # Leg 7 — external-send approval gate (in-process, POST mocked)
        # ------------------------------------------------------------------
        try:
            import requests  # noqa: F401 — the module extract.py calls .post on

            extract = load_extract_module()
            real_post = requests.post
            calls: list[dict] = []

            class FakeResp:
                status_code = 200
                text = "ok"

                def json(self):
                    return {"choices": [{"message": {"content":
                                                      '{"entities": [{"name": "Kulgo", "type": "project"}], "relations": []}'}}]}

            def fake_post(url, headers=None, json=None, timeout=None, **kw):
                calls.append({"url": url})
                return FakeResp()

            requests.post = fake_post
            gate_env = {
                "SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL": "1",
                "EXTRACT_API_URL": "https://not-allowed.example/v1",
                "EXTRACT_MODEL": "p5-test-model",
                "EXTRACT_ENABLED": "1",
            }
            saved_env = {k: os.environ.get(k)
                         for k in set(gate_env) | {"SECONDBRAIN_EXTERNAL_ALLOWLIST"}}
            try:
                os.environ.update(gate_env)
                os.environ.pop("SECONDBRAIN_EXTERNAL_ALLOWLIST", None)
                n_events_before = len([e for e in read_audit_events(audit_path)
                                       if event_type(e) == "external_send"])
                refused = None
                try:
                    extract.extract_knowledge("p5acc gate probe text", retries=0, backoff=0.01,
                                              timeout=5)
                except Exception as e:
                    refused = e
                check(refused is not None
                      and isinstance(refused, (PermissionError, RuntimeError))
                      and len(calls) == 0,
                      f"leg7 gate refuses non-allowlisted host before any POST "
                      f"(exc={type(refused).__name__ if refused else None}: "
                      f"{str(refused)[:120] if refused else None}, post_calls={len(calls)})")
                send_events = [e for e in read_audit_events(audit_path)
                               if event_type(e) == "external_send"]
                check(len(send_events) == n_events_before
                      and not any("not-allowed.example" in json.dumps(e) for e in send_events),
                      "leg7 NO external_send event logged for the refused send")

                # allowlisted host + mocked POST -> proceeds + external_send logged
                os.environ["SECONDBRAIN_EXTERNAL_ALLOWLIST"] = "not-allowed.example"
                out = extract.extract_knowledge("p5acc gate probe text", retries=0, backoff=0.01,
                                                timeout=5)
                send_events = [e for e in read_audit_events(audit_path)
                               if event_type(e) == "external_send"
                               and "not-allowed.example" in json.dumps(e)]
                check(isinstance(out, dict) and "entities" in out and len(calls) == 1
                      and len(send_events) >= 1,
                      f"leg7 allowlisted host + mocked POST proceeds and logs external_send "
                      f"(post_calls={len(calls)}, entities={len(out.get('entities', [])) if isinstance(out, dict) else None}, "
                      f"external_send_events={len(send_events)})")
            finally:
                requests.post = real_post
                for k, v in saved_env.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v
        except Exception as e:
            check(False, f"leg7 external gate failed: {type(e).__name__}: {e}")

        # ------------------------------------------------------------------
        # Leg 8 — cleanup seeded rows
        # ------------------------------------------------------------------
        try:
            with psycopg.connect(url, row_factory=dict_row) as conn:
                conn.execute(
                    "DELETE FROM knowledge_items WHERE source_uri LIKE %s "
                    "OR source_uri = %s OR id = ANY(%s::uuid[])",
                    (URI_PREFIX + "%", PROJECT_URI, all_node_ids),
                )
                remaining = conn.execute(
                    "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s "
                    "OR source_uri = %s OR id = ANY(%s::uuid[])",
                    (URI_PREFIX + "%", PROJECT_URI, all_node_ids),
                ).fetchone()["n"]
            check(int(remaining) == 0,
                  f"leg8 cleanup (seeded rows removed, relationships cascaded, "
                  f"remaining={remaining})")
        except Exception as e:
            check(False, f"leg8 cleanup failed: {type(e).__name__}: {e}")

    finally:
        if serve is not None:
            serve.close()
        for p in procs:
            try:
                p.kill()
                p.wait(timeout=10)
            except Exception:
                pass

    return failures


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

    work = Path(tempfile.mkdtemp(prefix="phase5acc_"))
    # in-process helpers (curator leg, extract leg) + all spawned children share
    # the tmp audit log / flag queue / test DB
    os.environ["DATABASE_URL"] = url
    os.environ["TEST_DATABASE_URL"] = url
    os.environ["SECONDBRAIN_AUDIT_LOG"] = str(work / "audit_log.jsonl")
    os.environ["SECONDBRAIN_FLAG_QUEUE"] = str(work / "flagged_issues.json")

    failures: list[str] = []
    try:
        failures = run_db_flow(url, work)
    finally:
        shutil.rmtree(work, ignore_errors=True)
        print("PASS tmp files removed" if not failures else "tmp files removed")
        if not drop_database(url):
            print("WARNING: could not drop the throwaway test DB "
                  f"({_safe_db_label(url)}) — no template1 maintenance connection")

    if failures:
        print(f"\nPHASE5 ACCEPTANCE FAILED ({len(failures)} failure(s)):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nPHASE5 ACCEPTANCE ALL PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
