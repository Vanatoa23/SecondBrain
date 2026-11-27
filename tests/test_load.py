"""200-document load + retrieval latency test against a REAL PostgreSQL + pgvector DB.

Setup — use a DEDICATED THROWAWAY database, never a production one:

    # against the local server (see task/PM_LEDGER.md batch 3):
    createdb load_test                      # on 127.0.0.1:5433 (TRUST auth)
    export TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/load_test
    python tests/test_load.py

TEST_DATABASE_URL is the ONLY accepted variable — there is deliberately no
DATABASE_URL fallback, because apply_schema() drops tables and must never be
pointed at a real database. As a second guard, the script refuses (exit 1) to
run against any database whose name does not match (^|_)test(_|$) (case-insensitive). If TEST_DATABASE_URL
is unset or unreachable, it prints
"SKIP: no test database available (set TEST_DATABASE_URL)" and exits 0, so it
never breaks environments without a DB.

Flow: apply_schema() drops and recreates the two tables (knowledge_items,
relationships) in the TEST database only, then applies db/schema.sql
(psql -f when psql is available, otherwise via psycopg). DATABASE_URL is then
pointed at the test DB for pipeline/ and retrieval/. 200 synthetic documents
are generated — each with a unique marker (loadtest-<i>-<6 hex chars>) and a
rotating topic — and ingested one by one via pipeline.ingest.ingest("text", ...),
timing the whole run (docs/sec). Then 10 evenly spaced documents are queried
by marker + topic through retrieval.search.search(query, top_k=5); every
wall-clock latency must be < 5.0s and recall@5 (target source_uri in top-5)
must be >= 8/10. Per-query latency, PASS/FAIL and a min/p50/max summary are
printed.

If the embedding/reranker models (bge-m3, bge-reranker-v2-m3) cannot be
loaded, documents are inserted via direct parameterized SQL and queries run
FTS-only (methods=["fts"], falling back to retrieval.fts.fts_search directly,
since search() embeds the query unconditionally); marker terms are
FTS-friendly, and recall is reported separately without failing in that mode.

Cleanup (always, in a finally block): DELETE FROM knowledge_items WHERE
source_uri LIKE 'load://%' — parameterized. Exit 0 on pass.

All SQL is parameterized.
"""

import hashlib
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REPO_ROOT = Path(__file__).resolve().parent.parent

import psycopg  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402

N_DOCS = 200
N_QUERIES = 10
LATENCY_BUDGET_S = 5.0
MIN_RECALL = 8  # of N_QUERIES

TOPICS = [
    "quantum computing",
    "home gardening",
    "index fund investing",
    "sourdough baking",
    "marathon training",
    "lisp programming",
    "venice canal history",
    "coffee roasting",
    "chess openings",
    "urban beekeeping",
]

# Per-document filler words keep the 200 synthetic docs lexically distinct.
# Docs are kept very short on purpose: the reranker (bge-reranker-v2-m3 on CPU)
# is the dominant per-query cost and scales with candidate count x token length.
_FILLER_WORDS = (
    "quokka zephyr nimbus lantern harbor meadow cactus tundra falcon willow "
    "cobalt saffron marble onyx pewter jasper topaz beryl quartz flint granite "
    "prairie fjord delta estuary lagoon savanna steppe badlands glacier dune "
    "orchard vineyard grove thicket heath moor marsh fen bog swamp creek brook "
    "rivulet canal levee dam sluice weir pier jetty quay wharf dock berth "
    "anchorage buoy beacon spire steeple belfry turret bastion citadel fortress "
    "palace villa chateau manor lodge cabin hut shack hovel bothy tent yurt "
    "igloo wigwam teepee caravan trailer wagon cart dray sled sleigh toboggan "
    "kayak canoe dinghy skiff yacht barge ferry liner trawler freighter tanker "
    "brig sloop schooner ketch cutter yawl gondola punt raft catamaran trimaran"
).split()

SKIP_MSG = "SKIP: no test database available (set TEST_DATABASE_URL)"


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
        from psycopg import conninfo
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
    """Try to load bge-m3 (embed) and bge-reranker-v2-m3 (rerank); trigger downloads."""
    try:
        from pipeline.embed import embed_text
        embed_text("warmup")
        from retrieval.rerank import rerank
        rerank("warmup", [{"content": "warmup"}])
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:200]}"


def make_docs() -> list[dict]:
    """200 synthetic docs: unique marker + rotating topic + distinct filler."""
    docs = []
    for i in range(N_DOCS):
        marker = f"loadtest-{i}-" + hashlib.sha256(f"load-{i}".encode()).hexdigest()[:6]
        topic = TOPICS[i % len(TOPICS)]
        rng = random.Random(20261125 + i)
        filler = " ".join(rng.sample(_FILLER_WORDS, 6))
        text = f"{marker} {topic} {filler}"
        docs.append({"index": i, "marker": marker, "topic": topic, "text": text})
    return docs


def run_ingest(url: str, docs: list[dict], model_ok: bool) -> float:
    """Ingest all docs via pipeline.ingest.ingest (or direct SQL without models)."""
    t0 = time.perf_counter()
    statuses: dict[str, int] = {}
    if model_ok:
        from pipeline.ingest import ingest
        for n, d in enumerate(docs, 1):
            res = ingest(
                "text", d["text"],
                source_uri=f"load://{d['index']}",
                title=f"Load doc {d['index']} {d['marker']}",
            )
            assert res["status"] == "inserted", (d["index"], res)
            statuses[res["status"]] = statuses.get(res["status"], 0) + 1
            if n % 25 == 0:
                print(f"  ingest progress {n}/{N_DOCS} ({time.perf_counter() - t0:.1f}s)")
    else:
        # No bge models: direct parameterized inserts (mirrors test_e2e.py),
        # embeddings stay NULL and queries run FTS-only.
        with psycopg.connect(url, row_factory=dict_row) as conn:
            for d in docs:
                conn.execute(
                    "INSERT INTO knowledge_items (type, title, content, mime_type, source_uri)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    ("note", f"Load doc {d['index']} {d['marker']}", d["text"],
                     "text/plain", f"load://{d['index']}"),
                )
        statuses["inserted"] = N_DOCS
    elapsed = time.perf_counter() - t0
    rate = N_DOCS / elapsed if elapsed > 0 else float("inf")
    mode = "pipeline.ingest.ingest" if model_ok else "direct SQL (models unavailable)"
    print(f"INGEST {N_DOCS} docs via {mode} in {elapsed:.2f}s -> {rate:.2f} docs/sec "
          f"(statuses={statuses})")
    return elapsed


def run_queries(docs: list[dict], model_ok: bool) -> tuple[list[float], int]:
    """10 evenly spaced marker queries; assert each < 5s and recall@5 >= 8/10."""
    from retrieval.search import search
    from retrieval.db import get_connection
    from retrieval.fts import fts_search

    indices = sorted({i * (N_DOCS - 1) // (N_QUERIES - 1) for i in range(N_QUERIES)})
    assert len(indices) == N_QUERIES, indices

    latencies: list[float] = []
    hits = 0
    for idx in indices:
        d = docs[idx]
        query = f"{d['marker']} {d['topic']}"
        target_uri = f"load://{idx}"
        t0 = time.perf_counter()
        if model_ok:
            out = search(query, top_k=5)
            results = out["results"]
        else:
            try:
                out = search(query, methods=["fts"], top_k=5)
                results = out["results"]
            except Exception:
                # search() embeds the query unconditionally — without bge-m3,
                # fall back to the raw FTS path (marker terms are FTS-friendly).
                with get_connection() as conn:
                    results = fts_search(conn, query, top_k=5)
        dt = time.perf_counter() - t0
        latencies.append(dt)
        top5 = [r.get("source_uri") for r in results[:5]]
        hit = target_uri in top5
        hits += 1 if hit else 0
        verdict = "PASS" if dt < LATENCY_BUDGET_S else "FAIL"
        print(f"query doc {idx:3d} marker={d['marker']} topic={d['topic']!r} "
              f"latency={dt:.3f}s {verdict} recall_hit={hit}")
        print(f"    top5 source_uris={top5}")
        assert dt < LATENCY_BUDGET_S, (
            f"search for doc {idx} took {dt:.3f}s (budget {LATENCY_BUDGET_S}s)"
        )

    lat = sorted(latencies)
    print(f"SUMMARY latency min={lat[0]:.3f}s p50={statistics.median(lat):.3f}s "
          f"max={lat[-1]:.3f}s budget={LATENCY_BUDGET_S}s")
    print(f"SUMMARY recall@5 = {hits}/{N_QUERIES} (required >= {MIN_RECALL})")
    if model_ok:
        assert hits >= MIN_RECALL, f"recall@5 {hits}/{N_QUERIES} < {MIN_RECALL}"
        print(f"PASS recall@5 {hits}/{N_QUERIES}")
    else:
        print(f"NOTE FTS-only mode (bge models unavailable): recall@5 {hits}/{N_QUERIES} "
              "reported separately, not asserted")
    return latencies, hits


def cleanup(url: str) -> None:
    with psycopg.connect(url, row_factory=dict_row) as conn:
        conn.execute("DELETE FROM knowledge_items WHERE source_uri LIKE %s", ("load://%",))
        remaining = conn.execute(
            "SELECT count(*) AS n FROM knowledge_items WHERE source_uri LIKE %s",
            ("load://%",),
        ).fetchone()["n"]
    assert remaining == 0
    print(f"PASS cleanup (load:// rows remaining: {remaining})")


def main() -> None:
    url = get_test_url()
    if not url:
        print(SKIP_MSG)
        sys.exit(0)

    # Refusal guard BEFORE any connection or destructive op: the dbname must
    # match (^|_)test(_|$) case-insensitively (stricter than a substring check).
    name = db_name(url)
    if not is_test_dbname(name):
        print(f"REFUSING to run against database {name!r}: TEST_DATABASE_URL must "
              "point at a test database (dbname matching (^|_)test(_|$)) — "
              "apply_schema() drops the knowledge tables.")
        sys.exit(1)

    if not db_reachable(url):
        print(SKIP_MSG)
        print(f"(connect to {_safe_db_label(url)} failed)")
        sys.exit(0)

    # pipeline/ and retrieval/ read DATABASE_URL — point them at the test DB
    os.environ["DATABASE_URL"] = url

    apply_schema(url)
    print("PASS setup_schema (db/schema.sql applied to test DB)")

    t0 = time.perf_counter()
    model_ok, model_err = check_models()
    print(f"models {'OK' if model_ok else 'UNAVAILABLE'} "
          f"(warmup {time.perf_counter() - t0:.1f}s)"
          + ("" if model_ok else f" — {model_err}"))
    if not model_ok:
        print("SKIP: embedding/reranker models unavailable — ingesting via direct "
              "SQL and querying FTS-only")

    docs = make_docs()
    try:
        run_ingest(url, docs, model_ok)
        run_queries(docs, model_ok)
    finally:
        cleanup(url)
    print("All load tests passed.")


if __name__ == "__main__":
    main()
