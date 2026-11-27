"""Guarded e2e: multi-hop GRAPH retrieval beats vector-only on a planted chain.

Phase 3 acceptance (ROADMAP.md): "Graph answers multi-hop questions vector
search fails on". This test plants a 4-node chain A -> B -> C -> D (via the
bridge `link_items` command, type "relates_to", strength 0.9, direction "out")
where each node has DISTINCT vocabulary, then proves:

  1. vector-only search (methods=["vector"], top_k=5) does NOT return D — the
     far node is semantically unreachable from the query — while it DOES
     return A (the near node);
  2. graph search (methods=["graph"], hops=3, top_k=20) DOES return D, reached
     at hop 3 through the planted chain, carrying source_uri/title provenance;
  3. hybrid (methods=["vector","graph"], hops=3, top_k=20) also returns D, and
     D is present in by_method["graph"] (the graph leg contributed it);
  4. control: graph with hops=1 does NOT reach D (the chain is exactly 3 hops
     from A, and no auto-link shortcut exists because the vocab is distinct);
  5. cleanup removes every planted row (0 rows remain).

Filler documents: two reasons. (1) The vector-only leg needs a real top-5
truncation to be meaningful — vector_search's candidate pool is candidate_k
(default 20), so with only 4 rows every doc would be returned and D would
trivially appear in "top-5". (2) Graph expansion seeds from the vector top-5
and EXCLUDES seed nodes from its results, so D must stay reliably outside the
vector top-5 or the graph leg would never surface it. Ten extra docs on A's
topic domain (telescope/astronomy vocabulary, distinct sub-topics) are planted
so D's vector rank sits far below 5 with a wide margin against embedding
jitter. As a belt-and-braces check, D's rank in vector-only (top_k=20) is also
asserted to be strictly worse than its rank in graph mode.

Setup (dedicated throwaway DB on the local pgvector server, trust auth):

    createdb multihop_test            # the test creates it if missing
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/multihop_test
    python tests/test_multihop.py

Guard pattern (locked, same as tests/test_e2e.py / tests/test_mcp_acceptance.py):
  - TEST_DATABASE_URL is the ONLY accepted variable — no DATABASE_URL
    fallback, because apply_schema() drops tables and must never run against
    a real database.
  - REFUSE (exit 1) when the dbname does not match (^|_)test(_|$)
    case-insensitively — checked BEFORE any connection is made.
  - SKIP (exit 0) when TEST_DATABASE_URL is unset, the server is unreachable,
    or the bge models are not in the HF cache.
  - All SQL is parameterized; cleanup (DELETE ... WHERE source_uri LIKE
    'multihop://%') and the serve-child kill run in a finally block.

Torch-free test process: this script never imports pipeline/retrieval/torch.
Model availability is checked via the HF cache only (same as
tests/test_mcp_acceptance.py), and ALL model work (embed/rerank) happens in
one persistent `python -m bridge serve` child spawned for ingest/link/search.

OMP_NUM_THREADS=2 note: the serve child runs with OMP_NUM_THREADS=2 per the
16GB commit-limit invariant (one torch-loading process at a time on this box,
os error 1455 otherwise). Latency budgets are measured separately with default
threads on an idle box — this test measures retrieval correctness, not latency.

Re-running is idempotent: apply_schema() drops and recreates the two tables in
the test DB only, and cleanup deletes the planted rows afterwards.
"""

from __future__ import annotations

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
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg import conninfo, sql  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"

REQUEST_TIMEOUT_S = 600.0  # first ingest/search also loads the bge models on CPU

# Locked guard: a test dbname matches (^|_)test(_|$) case-insensitively —
# stricter than a substring check, so e.g. "latest" is refused.
TEST_DBNAME_RE = re.compile(r"(?:^|_)test(?:_|$)", re.IGNORECASE)

URI_PREFIX = "multihop://"

# --- planted corpus ------------------------------------------------------------
# Chain nodes: four unrelated topics (distinct vocabulary) so vector similarity
# between the query (about A) and D is low. Markers make each doc findable.
CHAIN_DOCS = [
    ("a", "Chain A - telescope collimation", "multihop://chain-a",
     "Collimating an amateur Newtonian telescope means aligning the primary and "
     "secondary mirrors so the focal plane sits centered in the eyepiece drawtube. "
     "Marker alpha-7qk2m. Procedure: center the secondary under the focuser, adjust "
     "the primary tilt screws while watching a defocused star, and iterate until the "
     "diffraction rings look concentric."),
    ("b", "Chain B - sourdough fermentation", "multihop://chain-b",
     "Sourdough fermentation depends on wild yeast and lactobacillus living in the "
     "starter. Marker bravo-4wz9p. Feed the levain with equal parts flour and water, "
     "wait until it doubles and smells pleasantly tangy, then mix it into the "
     "autolyse before adding salt during the bulk rise."),
    ("c", "Chain C - aquarium nitrogen cycle", "multihop://chain-c",
     "The nitrogen cycle in a saltwater aquarium converts toxic ammonia from fish "
     "waste into nitrite and then nitrate. Marker charlie-8t3jv. Seed the tank with "
     "live rock or bottled bacteria, test the water every few days, and only add "
     "livestock once ammonia and nitrite read zero for a full week."),
    ("d", "Chain D - Byzantine mosaic restoration", "multihop://chain-d",
     "Restoring Byzantine mosaics involves stabilizing loose tesserae with lime-based "
     "mortar and lifting centuries of grime with soft brushes and deionized water. "
     "Marker delta-2n6hb. Consolidate flaking gold-leaf backgrounds gently, document "
     "each section photographically, and fill lacunae with visually distinguishable "
     "modern tiles."),
]

# Fillers: on A's topic domain with telescope vocabulary (so they outrank D
# against the query and keep D out of the vector top-5 seed set) but each on a
# distinct sub-topic (so auto_link never bridges them to B/C/D).
FILLER_DOCS = [
    ("f1", "Filler - astrophotography stacking", "multihop://filler-1",
     "Deep-sky astrophotography with a telescope stacks many short exposures to beat "
     "tracking error and sensor noise. Marker fill-11aa. Dither between frames, "
     "calibrate with dark frames and flat fields, then integrate the registered subs "
     "to reveal faint nebulae."),
    ("f2", "Filler - eyepiece selection", "multihop://filler-2",
     "Choosing an eyepiece for a telescope means matching its focal length to the "
     "telescope focal ratio for the desired magnification and exit pupil. Marker "
     "fill-22bb. Wide apparent field designs suit low-power sweeping, while short "
     "focal lengths with good eye relief serve high-power planetary observation."),
    ("f3", "Filler - light pollution filters", "multihop://filler-3",
     "UHC and OIII filters on a telescope block skyglow from street lamps while "
     "passing nebula emission lines. Marker fill-33cc. Thread the filter into the "
     "eyepiece barrel, and expect dimmer stars but much higher contrast on emission "
     "nebulae."),
    ("f4", "Filler - planisphere star chart", "multihop://filler-4",
     "A planisphere is a rotating star chart for telescope observing, showing which "
     "constellations are above the horizon at any date and hour. Marker fill-44dd. "
     "Set the wheel to the current time, hold it overhead facing north, and read off "
     "tonight's visible stars."),
    ("f5", "Filler - observatory dome ventilation", "multihop://filler-5",
     "A small roll-off observatory dome housing a telescope needs ventilation gaps "
     "and a dark interior to shed daytime heat. Marker fill-55ee. Paint the outside "
     "white, orient the slit away from prevailing weather, and add a desiccant box "
     "to protect the optics from humidity."),
    ("f6", "Filler - CCD flat field calibration", "multihop://filler-6",
     "Calibrating a CCD camera on a telescope means subtracting dark frames to remove "
     "thermal signal and dividing by flat fields to correct vignetting and dust "
     "motes. Marker fill-66ff. Shoot flats against an evenly illuminated panel at "
     "the same focus and temperature as the light frames."),
    ("f7", "Filler - telescope mount tracking", "multihop://filler-7",
     "Accurate tracking with an equatorial telescope mount starts with polar "
     "alignment of the mount axis, then periodic error correction and guiding. "
     "Marker fill-77gg. Balance the telescope tube slightly east-heavy so the gears "
     "stay meshed during long exposures."),
    ("f8", "Filler - telescope mirror cleaning", "multihop://filler-8",
     "Cleaning a telescope primary mirror uses distilled water and a drop of mild "
     "dish soap, letting the water lift dust without rubbing the coating. Marker "
     "fill-88hh. Rinse thoroughly, dry with cool air, and recoat the mirror only "
     "when reflectivity has visibly degraded."),
    ("f9", "Filler - barlow lens magnification", "multihop://filler-9",
     "A barlow lens doubles or triples the effective focal length of a telescope, "
     "so the same eyepiece gives higher magnification for planets and the Moon. "
     "Marker fill-99ii. Insert the barlow into the focuser drawtube before the "
     "eyepiece and refocus carefully."),
    ("f10", "Filler - dew heater for telescope optics", "multihop://filler-10",
     "A dew heater strap around the telescope corrector plate or eyepiece keeps the "
     "optics a few degrees above the dew point on humid nights. Marker fill-00jj. "
     "Run it at low power from a 12 volt battery to avoid bloating stars."),
]

ALL_DOCS = CHAIN_DOCS + FILLER_DOCS

QUERY = ("how do I collimate my amateur telescope mirror alignment procedure "
         "alpha-7qk2m")

A_URI = "multihop://chain-a"
D_URI = "multihop://chain-d"


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
    # Pure string parse — conninfo_to_dict does NOT connect.
    try:
        return conninfo.conninfo_to_dict(url).get("dbname", "") or ""
    except Exception:
        m = re.search(r"/([^/?]+)", url)
        return m.group(1) if m else ""


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
    spawned bridge serve child can exhaust the Windows commit limit (os error
    1455). So: check the HF cache — this test process stays torch-free.
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


# --- persistent bridge serve client ---------------------------------------------

class ServeClient:
    """One persistent `python -m bridge serve` child for all ingest/link/search.

    Newline JSON protocol: request {"id", "command", "args"} per stdin line,
    exactly one response line per request on stdout. A reader thread + queue
    gives per-request timeouts (select() does not work on pipes on Windows).
    """

    def __init__(self, env: dict):
        self._err = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bridge", "serve"],
            cwd=str(REPO_ROOT), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self._err, text=True, bufsize=1,
        )
        self._q: queue.Queue = queue.Queue()
        self._next_id = 1
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                self._q.put(json.loads(line))
            except json.JSONDecodeError:
                continue  # non-JSON noise can never appear on the saved stdout fd
        self._q.put(None)  # EOF sentinel: child exited

    def stderr_tail(self, n: int = 2000) -> str:
        try:
            self._err.seek(0)
            return self._err.read().decode("utf-8", "replace")[-n:]
        except Exception:
            return ""

    def request(self, command: str, args: dict, timeout: float = REQUEST_TIMEOUT_S) -> dict:
        rid = self._next_id
        self._next_id += 1
        self.proc.stdin.write(json.dumps({"id": rid, "command": command, "args": args}) + "\n")
        self.proc.stdin.flush()
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no response to {command!r} within {timeout}s")
            try:
                msg = self._q.get(timeout=remaining)
            except queue.Empty:
                raise TimeoutError(f"no response to {command!r} within {timeout}s") from None
            if msg is None:
                raise RuntimeError(
                    f"bridge serve child exited (rc={self.proc.poll()}) during "
                    f"{command!r}; stderr tail:\n{self.stderr_tail()}")
            if msg.get("id") == rid:
                return msg
            # responses are strictly ordered; ignore anything unexpected

    def close(self) -> None:
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=30)
        except Exception:
            try:
                self.proc.kill()
                self.proc.wait(timeout=10)
            except Exception:
                pass
        finally:
            try:
                self._err.close()
            except Exception:
                pass


# --- test body -------------------------------------------------------------------

def run_body(serve: ServeClient, url: str, failures: list[str]) -> None:
    def check(cond: bool, msg: str) -> bool:
        print(f"{'PASS' if cond else 'FAIL'} {msg}")
        if not cond:
            failures.append(msg)
        return cond

    # --- setup: ingest chain + fillers via the one persistent serve child ------
    ids: dict[str, str] = {}
    for key, title, uri, text in ALL_DOCS:
        resp = serve.request("ingest", {"source_type": "text", "payload": text,
                                        "source_uri": uri, "title": title})
        if not check(resp.get("ok") is True, f"ingest {key} ({uri}) ok"):
            failures.append(f"ingest {key} error envelope: {str(resp)[:300]}")
            return
        res = resp.get("result") or {}
        node_ids = res.get("node_ids") or []
        if not check(res.get("status") == "inserted" and len(node_ids) == 1,
                     f"ingest {key}: status={res.get('status')} single node "
                     f"(node_ids={len(node_ids)})"):
            return
        ids[key] = node_ids[0]
    print(f"PASS setup_ingest ({len(ALL_DOCS)} docs ingested via one bridge serve child)")

    # --- plant the chain A -> B -> C -> D ---------------------------------------
    for src, dst in (("a", "b"), ("b", "c"), ("c", "d")):
        resp = serve.request("link_items", {
            "from_id": ids[src], "to_id": ids[dst], "type": "relates_to",
            "strength": 0.9, "direction": "out",
            "context": "planted multihop chain",
        })
        ok = resp.get("ok") is True and (resp.get("result") or {}).get("type") == "relates_to"
        check(ok, f"link_items {src}->{dst} (relates_to, strength 0.9, out): "
                  f"{str(resp.get('result') or resp.get('error'))[:200]}")

    # DB sanity (parameterized): all planted rows present, exactly 3 chain edges.
    with psycopg.connect(url, row_factory=dict_row) as conn:
        n_items = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
        n_edges = conn.execute(
            "SELECT count(*) AS n FROM relationships WHERE type = %s", ("relates_to",),
        ).fetchone()["n"]
    check(n_items == len(ALL_DOCS), f"planted rows in DB: {n_items} == {len(ALL_DOCS)}")
    check(n_edges == 3, f"planted relates_to edges in DB: {n_edges} == 3")

    def do_search(args: dict) -> dict:
        resp = serve.request("search", args)
        if resp.get("ok") is not True:
            raise RuntimeError(f"search {args} failed: {str(resp)[:300]}")
        return resp["result"]

    def rank_of(results: list[dict], uri: str) -> int | None:
        uris = [r.get("source_uri") for r in results]
        return uris.index(uri) + 1 if uri in uris else None

    # --- 1) vector-only misses the far node --------------------------------------
    r_vec5 = do_search({"query": QUERY, "methods": ["vector"], "top_k": 5})
    vec5_uris = [r.get("source_uri") for r in r_vec5["results"]]
    check(A_URI in vec5_uris,
          f"vector-only top-5 contains the NEAR node A (uris={vec5_uris})")
    check(D_URI not in vec5_uris,
          f"vector-only top-5 does NOT contain the far node D (uris={vec5_uris})")

    # rank evidence over the full candidate pool (top_k=20 >= all planted docs)
    r_vec20 = do_search({"query": QUERY, "methods": ["vector"], "top_k": 20})
    vec20_uris = [r.get("source_uri") for r in r_vec20["results"]]
    vec_rank = rank_of(r_vec20["results"], D_URI) or (len(r_vec20["results"]) + 1)
    print(f"  vector-only rank of D at top_k=20: {vec_rank} (uris={vec20_uris})")

    # --- 2) graph with hops=3 reaches D through the chain, with provenance -------
    r_graph3 = do_search({"query": QUERY, "methods": ["graph"], "hops": 3, "top_k": 20})
    graph3 = r_graph3["results"]
    graph3_uris = [r.get("source_uri") for r in graph3]
    d_res = next((r for r in graph3 if r.get("source_uri") == D_URI), None)
    check(d_res is not None,
          f"graph hops=3 CONTAINS D (uris={graph3_uris})")
    if d_res is not None:
        check(bool(d_res.get("source_uri")) and bool(d_res.get("title")),
              f"D result carries provenance: source_uri={d_res.get('source_uri')!r} "
              f"title={d_res.get('title')!r}")
        via = d_res.get("via_relationship") or {}
        check(via.get("hop") == 3 and via.get("type") == "relates_to",
              f"D reached via the planted chain at hop 3 "
              f"(via_relationship={via})")
    graph_rank = rank_of(graph3, D_URI) or (len(graph3) + 1)
    print(f"  graph hops=3 rank of D: {graph_rank} (graph_ranking="
          f"{r_graph3['params'].get('graph_ranking')})")

    # --- 3) hybrid vector+graph also returns D (graph contributes it) ------------
    r_hybrid = do_search({"query": QUERY, "methods": ["vector", "graph"],
                          "hops": 3, "top_k": 20})
    hybrid_uris = [r.get("source_uri") for r in r_hybrid["results"]]
    check(D_URI in hybrid_uris,
          f"hybrid vector+graph hops=3 contains D (uris={hybrid_uris})")
    hybrid_graph_uris = [r.get("source_uri") for r in r_hybrid["by_method"].get("graph", [])]
    check(D_URI in hybrid_graph_uris,
          f"hybrid by_method['graph'] contains D — the graph leg contributed it "
          f"(graph uris={hybrid_graph_uris})")

    # --- 4) control: hops=1 does NOT reach D --------------------------------------
    r_graph1 = do_search({"query": QUERY, "methods": ["graph"], "hops": 1, "top_k": 20})
    graph1_uris = [r.get("source_uri") for r in r_graph1["results"]]
    check(bool(graph1_uris), f"control hops=1 graph returns something (uris={graph1_uris})")
    check(D_URI not in graph1_uris,
          f"control: graph hops=1 does NOT reach D (uris={graph1_uris})")

    # --- rank comparison: vector-only ranks D strictly worse than graph ----------
    check(vec_rank > graph_rank,
          f"rank comparison: vector-only rank of D ({vec_rank}) is strictly worse "
          f"than graph hops=3 rank ({graph_rank})")


# --- cleanup ----------------------------------------------------------------------

def cleanup(url: str, failures: list[str]) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        # relationships cascade via FK ON DELETE CASCADE
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s",
                     (URI_PREFIX + "%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            (URI_PREFIX + "%",),
        ).fetchone()["n"]
        orphans = conn.execute(
            """
            SELECT count(*) AS n FROM relationships r
            LEFT JOIN knowledge_items k ON k.id = r.from_id
            WHERE k.id IS NULL
            """
        ).fetchone()["n"]
    ok = remaining == 0 and orphans == 0
    print(f"{'PASS' if ok else 'FAIL'} cleanup (planted rows remaining: {remaining}, "
          f"orphan edges: {orphans})")
    if not ok:
        failures.append(f"cleanup left remaining={remaining} orphans={orphans}")


# --- main ---------------------------------------------------------------------------

def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection / destructive op: dbname must match
    # (^|_)test(_|$) case-insensitively (conninfo_to_dict is a pure parse).
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    if not ensure_database(url) or not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ read DATABASE_URL — point the serve child at the
    # test DB via its env (this process itself never imports them).
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    model_ok, model_err = check_models()
    if not model_ok:
        print(f"SKIP: embedding/reranker models unavailable ({model_err}) — "
              "this process stays torch-free; models are only checked via HF cache")
        sys.exit(0)
    print("PASS models (bge-m3 + bge-reranker-v2-m3 present in HF cache)")

    # One persistent bridge serve child for ingest/link/search. OMP_NUM_THREADS=2
    # per the 16GB commit-limit invariant (one torch process at a time).
    child_env = {**os.environ, "DATABASE_URL": url, "OMP_NUM_THREADS": "2"}
    print("NOTE serve child runs with OMP_NUM_THREADS=2 (commit-limit invariant; "
          "latency budgets are measured separately with default threads)")

    failures: list[str] = []
    serve: ServeClient | None = None
    try:
        serve = ServeClient(child_env)
        run_body(serve, url, failures)
    except Exception as e:
        traceback.print_exc()
        failures.append(f"unexpected error: {type(e).__name__}: {str(e)[:300]}")
    finally:
        if serve is not None:
            serve.close()  # serve-child kill in finally
        try:
            cleanup(url, failures)  # assertion 5: 0 rows remain
        except Exception as e:
            failures.append(f"cleanup failed: {type(e).__name__}: {str(e)[:200]}")

    if failures:
        print(f"\nMULTIHOP FAILED ({len(failures)} failure(s)):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nMULTIHOP ALL PASS (vector misses the far node, graph hops=3 reaches it)")
    sys.exit(0)


if __name__ == "__main__":
    main()
